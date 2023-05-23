This folder is for logging the output from the spark submit scripts.

- Save the Spark startup logs for submission with your solution using the commands below:

```
docker logs 2evaluatehumanbalancewithsparkstreaming-spark-1 > ../../spark/logs/spark-master.log

docker logs 2evaluatehumanbalancewithsparkstreaming-spark-worker-1-1 > ../../spark/logs/spark-worker.log
```