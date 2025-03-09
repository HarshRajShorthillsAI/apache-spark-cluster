FROM bitnami/spark:latest

# Copy Spark environment variables
COPY spark-env.sh /opt/bitnami/spark/conf/

# Copy application code
COPY app /app

WORKDIR /app
