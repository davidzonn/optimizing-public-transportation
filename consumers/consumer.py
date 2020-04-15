"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    SCHEMA_REGISTRY_URL = "http://localhost:8081"
    BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers": KafkaConsumer.BOOTSTRAP_SERVERS,
            "group.id": 1
        }

        if self.offset_earliest:
            self.broker_properties["auto.offset.reset"] = "earliest"
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = KafkaConsumer.SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)
            pass

        self.consumer.subscribe([self.topic_name_pattern,], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            if self.offset_earliest:
                partition.offset = OFFSET_BEGINNING
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        message = self.consumer.poll(timeout=1)
        if message is None:
            return 0
        if message.error() is not None:
            logging.error(f"Error consuming records: {message.error()} . Skipping")
            return 0
        logging.debug(f"consumed message {message.key()}: {message.value()}")
        self.message_handler(message)
        return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
