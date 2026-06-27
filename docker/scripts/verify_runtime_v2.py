import sys
sys.path.append('/home/jovyan/scripts')
from common_config import get_spark_session

spark = get_spark_session('Runtime_Verify_V2')

print('\n==================================================')
print('>>> SILVER V2 (M?ng l?c Streaming) <<<')
try:
    spark.read.format('delta').load('s3a://silver/tracking_events_v2/').show(5, truncate=False)
except Exception as e:
    print('L?i khi ??c Silver V2:', e)

print('\n>>> GOLD V2 (B?ng hi?n th? Metabase) <<<')
try:
    spark.read.format('delta').load('s3a://gold/fact_live_events_v2/').show(5, truncate=False)
except Exception as e:
    print('L?i khi ??c Gold V2:', e)
print('==================================================\n')
