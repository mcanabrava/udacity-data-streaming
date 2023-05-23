### Challenge

The product team wants to add a new feature to an existing application. This application is called STEDI and it collects data from seniors during a [small exercise](https://youtu.be/XosjuXTCGeg). After the exercise, the data transmitted enables the application to monitor seniors’ balance risk. 

The new feature consists of a graph that shows fail risk (will they fall and become injured?) for recent assessments by birth year.

To make this insightful data available in real-time, it will be necessary to get data about the users from a Redis database, which will be used as a Kafka source. In addition to this, whenever new data is prodocued by a user, a payload should be published to the Kafka topic redis-server as shown below:

{
    "customer":"Jason.Mitra@test.com",
    "score":7.0,
    "riskDate":"2020-09-14T07:54:06.417Z"
}

Therefore, the technical challenge consists of generating a new payload in a Kafka topic using Spark Streaming to make the aggregated information for all the scores data available in the application. The data should be grouped by the birth year, which can be captured from Redis. The following image summarizes the architecture:

![alt_text](./images/arch.png "Architecture")

### Running the Application

#  Using Docker for your Exercises

You will need to use Docker to run the exercises on your own computer. You can find Docker for your operating system here: https://docs.docker.com/get-docker/

It is recommended that you configure Docker to allow it to use up to 2 cores and 6 GB of your host memory for use by the course workspace. If you are running other processes using Docker simultaneously with the workspace, you should take that into account also.



The docker-compose file at the root of the repository creates 9 separate containers:

- Redis
- Zookeeper (for Kafka)
- Kafka
- Banking Simulation
- Trucking Simulation
- STEDI (Application used in Final Project)
- Kafka Connect with Redis Source Connector
- Spark Master
- Spark Worker

It also mounts your repository folder to the Spark Master and Spark Worker containers as a volume  `/home/workspace`, making your code changes instantly available within to the containers running Spark.

Let's get these containers started!

```
cd [repositoryfolder]
docker-compose up
```

You should see 9 containers when you run this command:
```
docker ps
```
