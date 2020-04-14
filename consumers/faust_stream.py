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
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1, value_type=TransformedStation)

table = app.Table(
   "stations-table",
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def transform_station(station_description_events):
    async for station_description in station_description_events.group_by(TransformedStation.station_name):
        line = "red" if station_description.red else "green" if station_description.green \
            else "blue" if station_description.blue else "unknown"
        transformed_station = TransformedStation(
            station_description.station_id,
            station_description.station_name,
            station_description.order,
            line
        )
        table[transformed_station.station_name] = transformed_station


if __name__ == "__main__":
    app.main()
