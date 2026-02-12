#!/bin/sh

export HADOOP_HOME=/opt/hadoop-3.2.0
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar
export JAVA_HOME=/usr/local/openjdk-8

# Variables with defaults
DB_HOST=${METASTORE_DB_HOSTNAME:-postgres}
DB_PORT=${METASTORE_DB_PORT:-5432}
DB_TYPE=${METASTORE_DB_TYPE:-postgres}

echo "Waiting for database on ${DB_HOST} to launch on ${DB_PORT} ..."

while ! nc -z ${DB_HOST} ${DB_PORT}; do
  sleep 1
done

echo "Database on ${DB_HOST}:${DB_PORT} started"

echo "Waiting 10s for DNS stabilization..."
sleep 10

echo "Initializing schema for $DB_TYPE..."
# Attempt to init schema. If it fails (already exists), we ignore and proceed.
/opt/apache-hive-metastore-3.0.0-bin/bin/schematool -initSchema -dbType $DB_TYPE -verbose || echo "Schema initialization failed or already exists. Proceeding..."

echo "Starting Hive Metastore..."
/opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore