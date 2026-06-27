import sys
sys.path.append('/home/jovyan/scripts')
from common_config import get_spark_session
spark = get_spark_session('Verify_Live_Gold')

print('\n==================================================')
print('>>> [LIVE GOLD TABLE - MUST CONTAIN e1] <<<')
print('==================================================')
try:
    spark.read.format('delta').load('s3a://phase76-test/gold/fact_live_events_v2/').show(truncate=False)
except Exception as e:
    print('Error:', e)
