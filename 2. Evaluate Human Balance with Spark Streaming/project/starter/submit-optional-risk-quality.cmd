docker exec -it 2evaluatehumanbalancewithsparkstreaming-spark-1 /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /home/workspace/project/starter/sparkpyoptionalriskquality.py | tee ../../spark/logs/optional-quality.log 