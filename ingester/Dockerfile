# Base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy files
COPY producer.py /app
COPY requirements.txt /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Environment variables
ENV KAFKA_BROKER=broker-1:9092
ENV KAFKA_TOPIC=farm-data

# Command to run the application
CMD ["python", "producer.py"]
