# Big Data Final Project

## Authors: Oleksandra Shergina, Viktoriia Lushpak
***(Звіт до роботи з детальним описом в .pdf файлі)***
### Інструкції з розгортання

1. Spark Streaming job:

```
docker exec -it wiki-spark-master /opt/spark/bin/spark-submit --master spark://wiki-spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0  /opt/spark-data/streaming-job.py
```

2. Spark Batch job:

```
docker exec -it wiki-spark-master /opt/spark/bin/spark-submit --master spark://wiki-spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages spark:spark-cassandra-connector_2.12:3.3.0  /opt/spark-data/batch-job.py
```

### Тестування
Приклади запитів, які підтримує система:

GET http://localhost:8083/api/domains

GET http://localhost:8083/api/users/StarTrekker/pages?limit=100

GET http://localhost:8083/api/pages/d00aefd8-0c61-4bbe-a6f5-0935e40d2051

GET http://localhost:8083/api/domains/uk.wikipedia.org/pages?from=2026-05-13T12:30:00&to=2026-05-13T13:30:00&limit=100

GET http://localhost:8083/api/reports/hourly?domain=uk.wikipedia.org&hours=2

GET http://localhost:8083/api/analytics/editor-patterns?min_pages=8
