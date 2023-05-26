"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# input Kafka Topic. 
topic = app.topic("org.chicago.cta.stations", value_type=Station)
# output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.table", value_type=TransformedStation, partitions=1)

table = app.Table(
    "org.chicago.cta.stations.table",
    default=int,
    partitions=1,
    changelog_topic=out_topic
)


#  Using Faust, we are transforming input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`

def get_line_color(red, blue, green):
    if red:
        return "red"
    if blue:
        return "red"
    if green:
        return "red"
    return ""

@app.agent(topic)
async def transform_station_stream(stationEvents):
    async for stationEvent in stationEvents:
        sanitized = TransformedStation(
            station_id=stationEvent.station_id,
            station_name=stationEvent.station_name,
            order=stationEvent.order,
            line=get_line_color(stationEvent.red, stationEvent.blue, stationEvent.green),
        )      
        
        table[sanitized.station_id] = sanitized


if __name__ == "__main__":
    app.main()
