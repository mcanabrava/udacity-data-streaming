docker exec -it evaluate-human-balance-with-spark-streaming-spark-1 /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /home/workspace/project/starter/sparkpykafkajoin.py | tee ../../spark/logs/kafkajoin.log 