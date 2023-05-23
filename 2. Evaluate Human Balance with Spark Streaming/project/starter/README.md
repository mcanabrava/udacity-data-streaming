

- Create a new Kafka topic to transmit the complete risk score with birth date, so the data can be viewed in the STEDI application graph



- You are going to to write 3 Spark Python scripts. Each will connect to a kafka broker running at `kafka:19092` :
    - `redis-server` topic: Write one spark script `sparkpyrediskafkastreamtoconsole.py` to subscribe to the `redis-server` topic, base64 decode the payload, and deserialize the JSON to individual fields, then print the fields to the console. The data should include the birth date and email address. You will need these.
    - `stedi-events` topic: Write a second spark script `sparkpyeventskafkastreamtoconsole.py` to subscribe to the `stedi-events` topic and deserialize the JSON (it is not base64 encoded) to individual fields. You will need the email address and the risk score.
    - New Topic: Write a spark script `sparkpykafkajoin.py` to join the customer dataframe and the customer risk dataframes, joining on the email address. Create a JSON output to the newly created kafka topic you configured for STEDI to subscribe to that contains at least the fields below:

```json
{"customer":"Santosh.Fibonnaci@test.com",
 "score":"28.5",
 "email":"Santosh.Fibonnaci@test.com",
 "birthYear":"1963"
} 
```

- From a new terminal type: `submit-event-kafkajoin.sh` or `submit-event-kafkajoin.cmd` to submit to the cluster

- Once the data is populated in the configured kafka topic, the graph should have real-time data points

![Populated Graph](images/populated_graph.png)

- Upload at least two screenshots of the working graph to the screenshots workspace folder 

