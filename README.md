submit streaming gob
```
docker exec -it wiki-spark-master /opt/spark/bin/spark-submit --master spark://wiki-spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0  /opt/spark-data/streaming-job.py
```