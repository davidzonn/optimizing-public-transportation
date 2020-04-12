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


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("connect_stations", value_type=Station)
out_topic = app.topic("com.udacity.station.descriptions", partitions=1, value_type=TransformedStation)

table = app.Table(
   "com.udacity.station.descriptions-table",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def transform_station(station_description_events):
    async for station_description in station_description_events:
        line = "red" if station_description.red else "green" if station_description.green \
            else "blue" if station_description.blue else "unknown"
        transformed_station = TransformedStation(
            station_description.station_id,
            station_description.station_name,
            station_description.order,
            line
        )
        await out_topic.send(key=str(station_description.station_id), value=transformed_station)


#Transform into status table for having a single data point per station.
@app.agent(out_topic)
async def create_station_summary(transformed_station_events):
    async for transformed_station in transformed_station_events:
        #Already copartitioned
        table[str(transformed_station.station_id)] = transformed_station.value


if __name__ == "__main__":
    app.main()
