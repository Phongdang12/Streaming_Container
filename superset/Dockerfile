FROM apache/superset:3.0.0

USER root

# Install Trino driver
RUN pip install trino==0.328.0

USER superset

WORKDIR /app
