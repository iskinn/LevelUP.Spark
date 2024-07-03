from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import pandas as pd
import time
from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from Connect_DB import connect_CH, connect_PG, encryptString, openKeyFile, spark_session, kafka_producer, \
    send_statistics, send_statistics_pg, tg_notification

# Нужно указать, чтобы spark подгрузил lib для kafka.
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars org.apache.spark:spark-sql-kafka-0-10_2.12-3.5.0 --packages org.apache.spark:spark-sql-kafka-0-10_2.12-3.5.0 pyspark-shell'

stream = 'shkCreate_sync'
app_params = openKeyFile(f'/opt/kafka-spark/Streams/{stream}/params.json')
connects = openKeyFile('/opt/kafka-spark/Streams/credentials.json')

# Разные переменные, задаются в ./params.json и ../credentials.json
spark_app_name = app_params['spark_app_name']
dt_load_start = app_params['dt_load_start']
kafka_topic = app_params['kafka_topic']
processing_time = app_params['kafka_processing_time']
kafka_etl_topic = connects['kafka_etl_topic']

spark, df = spark_session(stream)
producer = kafka_producer()

client = connect_CH()
ch_host = connects['ch_host']
ch_db_name = app_params['ch_db_name']
ch_dst_table = app_params['ch_dst_table']

pg_client = connect_PG()
pg_connect = pg_client.raw_connection()

checkpoint_path = f'/opt/kafka_checkpoint_dir/{spark_app_name}/{kafka_topic}/v1'


# Колонки, которые писать в ClickHouse. В kafka много колонок, не все нужны. Этот tuple нужен перед записью в ClickHouse.
columns_to_ch = ("shk_id", "wbsticker_id", "barcode", "chrt_id", "nm_id", "dt", "employee_id",
                 "place_id", "state_id", "tare", "tare_type",
                 "gi_id", "supplier_id", "invdet_id", "expire_dt", "has_excise",
                 "supplier_box_id", "is_surplus",  "ext_ids")


# Схема сообщений в топике kafka. Используется при формировании batch.
schema = StructType([
    StructField("shk_id", LongType(), False),
    StructField("wbsticker_id", LongType(), True),
    StructField("barcode", StringType(), True),
    StructField("chrt_id", LongType(), True),
    StructField("nm_id", LongType(), True),
    StructField("dt", StringType(), False),
    StructField("employee_id", LongType(), True),
    StructField("place_id", LongType(), True),
    StructField("state_id", StringType(), True),
    StructField("office_id", LongType(), True),
    StructField("wh_id", LongType(), True),
    StructField("is_stock", BooleanType(), True),
    StructField("is_podsort", BooleanType(), True),
    StructField("box_id", LongType(), True),
    StructField("container_id", LongType(), True),
    StructField("transfer_box_id", LongType(), True),
    StructField("pallet_id", LongType(), True),
    StructField("place_name", StringType(), True),
    StructField("stage", LongType(), True),
    StructField("street", LongType(), True),
    StructField("section", LongType(), True),
    StructField("rack", LongType(), True),
    StructField("field", LongType(), True),
    StructField("place_type_id", LongType(), True),
    StructField("ext_barcode", LongType(), True),
    StructField("no_wb_sticker", BooleanType(), True),
    StructField("goods_sticker", LongType(), True),
    StructField("goods_sticker_type", LongType(), True),
    StructField("tare", LongType(), True),
    StructField("tare_type", StringType(), True),
    StructField("gi_id", LongType(), True),
    StructField("supplier_id", LongType(), True),
    StructField("imei", LongType(), True),
    StructField("serial_number", LongType(), True),
    StructField("fur_mark", LongType(), True),
    StructField("invdet_id", LongType(), True),
    StructField("expire_dt", StringType(), True),
    StructField("has_excise", BooleanType(), True),
    StructField("supplier_box_id", LongType(), True),
    StructField("is_surplus", BooleanType(), True),
    StructField("supplier_boxcode", StringType(), True),
    StructField("ext_ids", StringType(), True),
    StructField("correction_dt", StringType(), True)
])


def column_filter(df):
    # select только нужные колонки.
    col_tuple = []
    for col in columns_to_ch:
        col_tuple.append(f"value.{col}")
    return df.selectExpr(col_tuple)


def dt_filter(df):
    # Отсекаем по дате не нужные строки.
    min_dt = df.select(min('dt').alias('min_dt'))
    return df.where(f"dt > '{dt_load_start}'"), min_dt.first()[0]


def get_payload_item(df_pd):
    # берем из DataFrame min и max dt и shk_id
    return df_pd[['shk_id', 'dt']].agg(['min', 'max']).to_string()


def load_to_ch(df):
    # Преобразуем в dataframe pandas и записываем в ClickHouse.
    df_pd = df.toPandas()
    df_pd.dt = pd.to_datetime(df_pd.dt, format='ISO8601', utc=True, errors='ignore')+pd.Timedelta(hours=3)
    df_pd.expire_dt = df_pd.expire_dt.str.slice(0, 10)
    df_pd.expire_dt = df_pd.expire_dt.fillna('1970-01-01')
    df_pd.expire_dt = df_pd.expire_dt.str.replace('2270','2260')
    df_pd.expire_dt = pd.to_datetime(df_pd.expire_dt, format='%Y-%m-%d', errors='ignore')
    df_pd.has_excise = df_pd.has_excise.astype(bool)
    df_pd.ext_ids = df_pd.ext_ids.fillna('').astype(str)
    df_pd = df_pd.assign(entry='shkCreate_kafka')


    while True:
        try:
            client.insert_dataframe(f'INSERT INTO {ch_db_name}.{ch_dst_table} VALUES', df_pd)
            # send_statistics(producer, kafka_etl_topic, spark_app_name,'sql_success', 0, f'INSERT INTO {ch_table} VALUES')
            return df_pd
            break
        except Exception as e:
            error_message = f'Error insert data to CH {ch_host}. Error: {e}'
            tg_notification(['282601909', '375107700'],
                            f"Kafka-spark {spark_app_name} Error insert data to CH!\n"
                            f"{error_message[:250]}")
            send_statistics(producer, kafka_etl_topic, spark_app_name,'error', 0, error_message)
            send_statistics_pg(pg_connect, spark_app_name, 'error', 0, error_message)
            time.sleep(60)

# Функция обработки batch. На вход получает dataframe-spark.
def foreach_batch_function(df2, epoch_id):
    df_rows = df2.count()
    # Если dataframe не пустой, тогда продолжаем.
    if df_rows > 0:
        # df2.printSchema()
        # Убираем не нужные колонки.
        df2 = column_filter(df2)
        # Отсекаем не нужные строки по дате.
        df2, min_dt = dt_filter(df2)
        df_rows_after_filter = df2.count()
        # Если dataframe не пустой, тогда продолжаем.
        if df_rows_after_filter > 0:
            # df2.show(5)
            # Записываем весь dataframe в ch.
            df_pd = load_to_ch(df2)
            stat_payload_item = get_payload_item(df_pd)
            send_statistics(producer, kafka_etl_topic, spark_app_name,'success', df_rows_after_filter, f'{stat_payload_item}')
            send_statistics_pg(pg_connect, spark_app_name, 'success', df_rows_after_filter, f'{stat_payload_item}')
        else:
            send_statistics(producer, kafka_etl_topic, spark_app_name,'success', df_rows, f'No data to load. dt < dt_load_start:{dt_load_start}')
            send_statistics_pg(pg_connect, spark_app_name, 'success', df_rows, f'No data to load. dt < dt_load_start:{dt_load_start}. min_dt: {min_dt}.')
    else:
        send_statistics(producer, kafka_etl_topic, spark_app_name,'success', df_rows, f'No data in Kafka.')
        send_statistics_pg(pg_connect, spark_app_name, 'success', df_rows, 'No data in Kafka')

send_statistics(producer, kafka_etl_topic, spark_app_name,'start', 0, f'{datetime.now():%Y-%m-%d %H:%M:%S%z}')
send_statistics_pg(pg_connect, spark_app_name, 'start', 0)

# Описание как создаются микробатчи. processing_time - задается вначале скрипта
query = df.select(from_json(col("value").cast("string"), schema).alias("value")) \
    .writeStream \
    .trigger(processingTime=processing_time) \
    .option("checkpointLocation", checkpoint_path) \
    .foreachBatch(foreach_batch_function) \
    .start()

query.awaitTermination()
