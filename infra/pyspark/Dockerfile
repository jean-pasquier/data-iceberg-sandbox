ARG SPARK_VERSION=4.0.1

FROM apache/spark:${SPARK_VERSION}-python3

ARG SPARK_MAJOR_VERSION=4.0
ARG SPARK_SCALA_VERSION=2.13
ENV ICEBERG_VERSION=1.10.0

ENV SPARK_HOME=/opt/spark

WORKDIR /opt/spark

# Download Iceberg extensions
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SPARK_SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SPARK_SCALA_VERSION}-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SPARK_SCALA_VERSION}-${ICEBERG_VERSION}.jar
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

ENV IJAVA_CLASSPATH=/opt/spark/jars/*

