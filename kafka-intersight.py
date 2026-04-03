import datetime
import time
import json
import os
import random
import logging
from intersight.api import cond_api, aaa_api
from intersight.authentication import get_api_client
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable
from dotenv import load_dotenv

# --- CONFIGURATION ---
PAGE_SIZE = 1000
BOOTSTRAP_SERVERS = ['localhost:9092']
POLL_INTERVAL = 60  # seconds
INTERSIGHT_ENDPOINT = 'https://intersight.com'

COLD_START_MINUTES = 5
DELIVERY_TIMEOUT_SECONDS = 10.0
RETRY_BASE_SECONDS = 2.0
RETRY_MAX_SECONDS = 60.0
RETRY_JITTER_SECONDS = 1.0

load_dotenv()


def get_env_float(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    return float(value)


def get_env_int(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
DELIVERY_TIMEOUT_SECONDS = get_env_float('DELIVERY_TIMEOUT_SECONDS', DELIVERY_TIMEOUT_SECONDS)
RETRY_BASE_SECONDS = get_env_float('RETRY_BASE_SECONDS', RETRY_BASE_SECONDS)
RETRY_MAX_SECONDS = get_env_float('RETRY_MAX_SECONDS', RETRY_MAX_SECONDS)
RETRY_JITTER_SECONDS = get_env_float('RETRY_JITTER_SECONDS', RETRY_JITTER_SECONDS)
COLD_START_MINUTES = get_env_int('COLD_START_MINUTES', COLD_START_MINUTES)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s %(levelname)s %(name)s %(message)s'
)
logger = logging.getLogger('kafka-intersight')


def json_default(value):
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, default=json_default).encode('utf-8'),
    acks='all'
)


def compute_backoff(attempt):
    base = min(RETRY_MAX_SECONDS, RETRY_BASE_SECONDS * (2 ** max(attempt - 1, 0)))
    jitter = random.uniform(0, RETRY_JITTER_SECONDS)
    return min(RETRY_MAX_SECONDS, base + jitter)


def get_required_env(name):
    value = os.getenv(name)
    if value:
        return value
    raise ValueError(f"Missing required environment variable: {name}")


def load_secret_key():
    secret_key_file = os.getenv('INTERSIGHT_SECRET_KEY_FILE')
    if secret_key_file:
        if not os.path.exists(secret_key_file):
            raise ValueError(f"Secret key file not found: {secret_key_file}")
        with open(secret_key_file, 'r', encoding='utf-8') as handle:
            return handle.read()

    raise ValueError(
        'Missing required environment variable: INTERSIGHT_SECRET_KEY_FILE'
    )


def load_intersight_client():
    key_id = get_required_env('INTERSIGHT_KEY_ID')
    secret_key = load_secret_key()
    endpoint = os.getenv('INTERSIGHT_ENDPOINT', INTERSIGHT_ENDPOINT)

    return get_api_client(
        api_key_id=key_id,
        private_key_string=secret_key,
        endpoint=endpoint,
    )


def is_auth_error(error):
    err_name = error.__class__.__name__.lower()
    err_text = str(error).lower()
    return (
        'unauthorized' in err_name
        or 'forbidden' in err_name
        or 'authenticationfailure' in err_text
        or 'invalid api key' in err_text
        or 'status code: 401' in err_text
        or 'status code: 403' in err_text
    )


def is_transient_error(error):
    if isinstance(error, (KafkaTimeoutError, NoBrokersAvailable, TimeoutError, ConnectionError)):
        return True
    err_text = str(error).lower()
    transient_tokens = [
        'timeout',
        'temporarily',
        'connection reset',
        'connection refused',
        'service unavailable',
        'too many requests',
        'leader not available',
    ]
    return any(token in err_text for token in transient_tokens)


def poll_intersight(api_instance, api_method_name, last_run_dt, topic):
    """
    Polls a specific Intersight endpoint and produces to Kafka.
    """
    # Format timestamp for Intersight OData filter
    # Example: 2026-04-03T10:00:00.000Z
    filter_time = last_run_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"
    filter_str = f"ModTime gt {filter_time}"
    
    skip = 0
    has_more = True
    newest_timestamp = last_run_dt
    pending_deliveries = []
    fetched_count = 0
    api_method = getattr(api_instance, api_method_name)

    logger.info("poll-start topic=%s since=%s", topic, filter_time)

    while has_more:
        response = api_method(filter=filter_str, top=PAGE_SIZE, skip=skip)
        
        if not response or not response.results:
            break

        fetched_count += len(response.results)
            
        for record in response.results:
            # Use MOID as the unique Kafka Key
            message_key = record.moid.encode('utf-8')
            future = producer.send(
                topic=topic,
                key=message_key,
                value=record.to_dict()
            )
            pending_deliveries.append((record.moid, future))
            
            # Update our tracker if this record is newer
            if record.mod_time > newest_timestamp:
                newest_timestamp = record.mod_time
        
        if len(response.results) < PAGE_SIZE:
            has_more = False
        else:
            skip += PAGE_SIZE

    producer.flush(timeout=DELIVERY_TIMEOUT_SECONDS)

    published_count = 0
    delivery_failed_count = 0
    for record_moid, future in pending_deliveries:
        try:
            future.get(timeout=DELIVERY_TIMEOUT_SECONDS)
            published_count += 1
        except KafkaError as error:
            delivery_failed_count += 1
            logger.error(
                "kafka-delivery-failed topic=%s key=%s error=%s",
                topic,
                record_moid,
                error,
            )
            raise

    return newest_timestamp, {
        'fetched': fetched_count,
        'published': published_count,
        'delivery_failed': delivery_failed_count,
    }

def main():
    # Cold start: Start looking from 5 minutes ago
    cold_start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=COLD_START_MINUTES)

    api_client = load_intersight_client()
    alarm_api = cond_api.CondApi(api_client)
    audit_api = aaa_api.AaaApi(api_client)

    topic_watermarks = {
        'intersight-alarms': cold_start,
        'intersight-audits': cold_start,
    }
    cumulative_metrics = {
        'fetched': 0,
        'published': 0,
        'delivery_failed': 0,
        'poll_errors_by_type': {},
    }
    retry_attempt = 0

    logger.info(
        "poller-started endpoint=%s kafka_bootstrap=%s delivery_timeout_s=%s retry_base_s=%s retry_max_s=%s",
        os.getenv('INTERSIGHT_ENDPOINT', INTERSIGHT_ENDPOINT),
        BOOTSTRAP_SERVERS,
        DELIVERY_TIMEOUT_SECONDS,
        RETRY_BASE_SECONDS,
        RETRY_MAX_SECONDS,
    )

    while True:
        try:
            # 1. Poll Alarms
            alarms_timestamp, alarms_stats = poll_intersight(
                alarm_api,
                'get_cond_alarm_list',
                topic_watermarks['intersight-alarms'],
                'intersight-alarms'
            )
            topic_watermarks['intersight-alarms'] = alarms_timestamp
            
            # 2. Poll Audit Logs
            audits_timestamp, audits_stats = poll_intersight(
                audit_api,
                'get_aaa_audit_record_list',
                topic_watermarks['intersight-audits'],
                'intersight-audits'
            )
            topic_watermarks['intersight-audits'] = audits_timestamp

            loop_fetched = alarms_stats['fetched'] + audits_stats['fetched']
            loop_published = alarms_stats['published'] + audits_stats['published']
            loop_delivery_failed = alarms_stats['delivery_failed'] + audits_stats['delivery_failed']

            cumulative_metrics['fetched'] += loop_fetched
            cumulative_metrics['published'] += loop_published
            cumulative_metrics['delivery_failed'] += loop_delivery_failed

            logger.info(
                "run-summary alarms_published=%s audits_published=%s alarms_fetched=%s audits_fetched=%s",
                alarms_stats['published'],
                audits_stats['published'],
                alarms_stats['fetched'],
                audits_stats['fetched'],
            )
            logger.info(
                "metrics-total fetched=%s published=%s delivery_failed=%s",
                cumulative_metrics['fetched'],
                cumulative_metrics['published'],
                cumulative_metrics['delivery_failed'],
            )

            retry_attempt = 0
            logger.info("poll-success next_poll_seconds=%s", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)
            
        except Exception as error:
            error_type = error.__class__.__name__
            cumulative_metrics['poll_errors_by_type'][error_type] = (
                cumulative_metrics['poll_errors_by_type'].get(error_type, 0) + 1
            )

            if is_auth_error(error) or isinstance(error, ValueError):
                logger.error("fatal-error type=%s message=%s", error_type, error)
                raise

            retry_attempt += 1
            delay = compute_backoff(retry_attempt) if is_transient_error(error) else min(RETRY_MAX_SECONDS, RETRY_BASE_SECONDS)
            logger.warning(
                "poll-error type=%s transient=%s retry_attempt=%s sleep_seconds=%.2f message=%s",
                error_type,
                is_transient_error(error),
                retry_attempt,
                delay,
                error,
            )
            time.sleep(delay)

if __name__ == "__main__":
    main()
