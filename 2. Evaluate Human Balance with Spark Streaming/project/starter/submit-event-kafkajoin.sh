#!/bin/bash
echo "Starting script execution."

# Custom log message
echo "Starting spark-submit..." >> ../../spark/logs/kafkajoin.log

winpty docker exec -i 2evaluatehumanbalancewithsparkstreaming-spark-1 script -q -c "/opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/project/starter/sparkpykafkajoin.py" | tee -a ../../spark/logs/kafkajoin.log

# Custom log message
echo "spark-submit completed." >> ../../spark/logs/kafkajoin.log

echo "Script execution completed."
