ARG SPARK_VERSION=3.5.7

FROM apache/spark:${SPARK_VERSION}-python3

ARG SPARK_MAJOR_VERSION=3.5
ARG SPARK_SCALA_VERSION=2.12
ENV ICEBERG_VERSION=1.10.0

ENV SPARK_HOME=/opt/spark

WORKDIR /opt/spark

# install spark ?
# RUN mkdir -p ${SPARK_HOME}  && curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz  && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1  && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz # buildkit

# Download Iceberg extensions
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SPARK_SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SPARK_SCALA_VERSION}-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SPARK_SCALA_VERSION}-${ICEBERG_VERSION}.jar
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

ENV IJAVA_CLASSPATH=/opt/spark/jars/*

