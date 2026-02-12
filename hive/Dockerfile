FROM bitsondatadev/hive-metastore:latest

# Copy configuration
COPY config/hive-site.xml /opt/apache-hive-metastore-3.0.0-bin/conf/hive-site.xml

# Copy custom entrypoint
COPY entrypoint.sh /entrypoint.sh
USER root

# Download Postgres JDBC Driver
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -o /opt/apache-hive-metastore-3.0.0-bin/lib/postgresql-42.6.0.jar || \
    wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/apache-hive-metastore-3.0.0-bin/lib/postgresql-42.6.0.jar

# Fix Postgres 12+ incompatibility (remove "SET default_with_oids = false;")
RUN find /opt/apache-hive-metastore-3.0.0-bin -name "hive-schema-*.postgres.sql" -exec sed -i "s/SET default_with_oids = false;//g" {} +

RUN sed -i 's/\r$//' /entrypoint.sh
RUN chmod +x /entrypoint.sh
RUN chown -R hive:hive /opt/apache-hive-metastore-3.0.0-bin

USER hive
ENTRYPOINT ["sh", "/entrypoint.sh"]