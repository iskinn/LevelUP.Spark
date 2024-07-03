
```
spark-submit --master spark://localhost:1560  \
    --py-files /opt/kafka-spark/Streams/Utils/Connect_DB.py \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --executor-cores 1 \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" 
    /opt/kafka-spark/Streams/shkCreate/shkCreate.py
```

# Структура файлов и папок

## docker-compose
Корневая папка для файлов контейнеров с необходимыми настройками.
Для каждой вложенной папки есть файлы настроек .env_main и .env_reserv с переменными окружения для основного и резервного развертывания. Перед развертыванием контейнеров, нужный файл необходимо переименовать в .env.
В файлах docker-compose могут использоваться внешний volume. Их нужно создавать заранее и давать права на запись службе docker.

## Streams
Корневая папка для заданий spark. Для каждого задания создается отдельная папка.
В каждой папке есть файл с параметрами и python файл самого задания.


pip install clickhouse_driver clickhouse_cityhash lz4 pandas

spark-submit --master spark://spark-master:7077  \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --executor-cores 1 \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
    /opt/spark/Streams/shkCreate_edu_100/shkCreate_sync_edu.py