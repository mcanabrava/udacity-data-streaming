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