import sys
sys.path.append('/home/jovyan/scripts')
from common_config import get_spark_session
spark = get_spark_session('Verify')

print('\n==================================================')
print('>>> [VALID SINK - MUST ONLY CONTAIN e1] <<<')
print('==================================================')
try:
    spark.read.format('delta').load('s3a://phase76-test/silver/tracking_events_v2/').select('event_id', 'event_time', 'event_type', 'user_id', 'session_id').show(truncate=False)
except Exception as e:
    print('Error:', e)

print('\n==================================================')
print('>>> [QUARANTINE SINK - MUST ONLY CONTAIN BAD EVENT] <<<')
print('==================================================')
try:
    spark.read.format('delta').load('s3a://phase76-test/silver/quarantine/tracking_events_v2/').select('validation_errors', 'raw_json').show(truncate=False)
except Exception as e:
    print('Error:', e)
