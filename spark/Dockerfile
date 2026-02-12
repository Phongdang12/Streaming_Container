FROM apache/spark:3.5.0

USER root

# Install Python dependencies
RUN pip install --no-cache-dir pyspark==3.5.0

# Create directories with proper permissions
RUN mkdir -p /opt/spark-jobs /opt/spark-jars /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
    chown -R 185:185 /home/spark/.ivy2

USER 185

WORKDIR /opt/spark-jobs
