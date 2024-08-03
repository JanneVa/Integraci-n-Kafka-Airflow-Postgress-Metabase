from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import numpy as np
from datetime import datetime
import os
import time

class FarmDataGenerator:
    def __init__(self, kafka_broker, schema_registry_url, topic):
        # Initialize the Schema Registry Client
        self.schema_registry_client = SchemaRegistryClient({
            'url': schema_registry_url
        })

        # Define Avro schemas
        self.key_schema_str = """
        {
            "namespace": "farm.data",
            "type": "record",
            "name": "Key",
            "fields": [
                {"name": "fecha_nacimiento", "type": "string"}
            ]
        }
        """

        self.value_schema_str = """
        {
            "namespace": "farm.data",
            "type": "record",
            "name": "FarmRecord",
            "fields": [
                {"name": "fecha_registro", "type": "string"},
                {"name": "fecha_nacimiento", "type": "string"},
                {"name": "tipo", "type": "string"},
                {"name": "volumen", "type": "int"}
            ]
        }
        """

        # Create Avro serializers
        self.key_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=self.key_schema_str,
            to_dict=self.key_to_dict
        )

        self.value_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=self.value_schema_str,
            to_dict=self.value_to_dict
        )

        # Initialize the Producer with required configurations
        self.producer = Producer({
            'bootstrap.servers': kafka_broker
        })

        self.topic = topic
        self.start_date = datetime.strptime('2023-05-16', '%Y-%m-%d')

    def delivery_report(self, err, msg):
        """Callback function to report message delivery results."""
        if err is not None:
            print(f"Delivery failed for record {msg.key()}: {err}")
        else:
            print(f"Record {msg.key()} successfully produced to {msg.topic()} partition {msg.partition()} at offset {msg.offset()}")

    def generate_random_data(self):
        # Generate random month and day within the specified range
        random_month = np.random.randint(5, 11)  # May (5) to October (10)
        random_day = np.random.randint(1, 29)  # To ensure no invalid dates
        record_date = self.start_date.replace(month=random_month, day=random_day)

        # Calculate the register date
        if random_month == 5:
            register_date = record_date.replace(month=4, day=15)  # April 15
        else:
            register_date = record_date.replace(month=random_month - 1, day=15)  # Previous month 15th

        row = {
            'fecha_registro': register_date.strftime('%Y-%m-%d'),
            'fecha_nacimiento': record_date.strftime('%Y-%m-%d'),
            'tipo': np.random.choice(['R', 'S']),
            'volumen': np.random.randint(40, 101)
        }

        return row

    def key_to_dict(self, key, ctx):
        """Converts the key data to a dictionary for Avro serialization."""
        return {"fecha_nacimiento": key}

    def value_to_dict(self, value, ctx):
        """Converts the value data to a dictionary for Avro serialization."""
        return value

    def run(self, wait_time=5, num_jsons=10):
        while True:
            json_data_list = [self.generate_random_data() for _ in range(num_jsons)]
            for data in json_data_list:
                # Serialize the key and value
                try:
                    key = self.key_serializer(
                        data['fecha_nacimiento'],
                        SerializationContext(self.topic, MessageField.KEY)
                    )
                    value = self.value_serializer(
                        data,
                        SerializationContext(self.topic, MessageField.VALUE)
                    )

                    # Produce the message
                    self.producer.produce(
                        topic=self.topic,
                        key=key,
                        value=value,
                        on_delivery=self.delivery_report
                    )
                except Exception as e:
                    print(f"Serialization or production failed: {e}")

            self.producer.flush()
            time.sleep(wait_time)

if __name__ == "__main__":
    kafka_broker = os.getenv('KAFKA_BROKER', 'broker-1:9092')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'farm-data')
    schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://schema_registry:8081')

    generator = FarmDataGenerator(kafka_broker, schema_registry_url, kafka_topic)
    generator.run(wait_time=2, num_jsons=1)
