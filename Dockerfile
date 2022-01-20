FROM ubuntu:18.04
RUN apt-get update \
  && apt-get -y install wget \
  && apt-get -y install vim \
  && apt-get -y install git \
  && apt-get -y install scala 

RUN apt-get install -y --no-install-recommends software-properties-common
RUN add-apt-repository -y ppa:openjdk-r/ppa
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk
RUN apt-get install -y openjdk-8-jre
RUN update-alternatives --config java
RUN update-alternatives --config javac
RUN update-java-alternatives --set /usr/lib/jvm/java-1.8.0-openjdk-amd64


RUN wget https://archive.apache.org/dist/kafka/2.4.1/kafka_2.11-2.4.1.tgz \
  && tar -xvzf kafka_2.11-2.4.1.tgz 

RUN wget https://pkg.osquery.io/deb/osquery_5.1.0-1.linux_amd64.deb

#RUN wget https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
RUN wget https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
#RUN wget http://mirrors.myaegean.gr/apache/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz

RUN git clone https://github.com/gaurav1620/Spark_Hudi \
  && mkdir /etc/osquery \
  && cp Spark_Hudi/osq* /etc/osquery/. \
  && rm Spark_Hudi/code.scala \
  && mv Spark_Hudi StreamHandler 

RUN dpkg -i ./osquery_5.1.0-1.linux_amd64.deb
RUN rm -rf /opt/spark && mkdir /opt/spark \
  && tar -xvzf spark-2.4.5-bin-hadoop2.7.tgz \
  && cp -r spark-2.4.5-bin-hadoop2.7/. /opt/spark/. \
  && echo "export SPARK_HOME=/opt/spark" >> ~/.profile \
  && echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.profile \
  && echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile 

# install sbt
#RUN apt-get update \
#  && apt-get install apt-transport-https curl gnupg -yqq \
#  && "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list \
#  && "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list \
#  && curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import \
#  && chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg \
#  && apt-get update \
#  && apt-get install sbt


RUN kafka_2.11-2.4.1/bin/zookeeper-server-start.sh kafka_2.11-2.4.1/config/zookeeper.properties \ 
  & kafka_2.11-2.4.1/bin/kafka-server-start.sh kafka_2.11-2.4.1/config/server.properties \
  & kafka_2.11-2.4.1/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Orders1 \
  & kafka_2.11-2.4.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Orders1


RUN osqueryd

# Test kafka
#  echo "hello world" | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic Orders1

# Clear kafka data
# kafka_2.11-2.4.1/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic Orders1

# Run the scala project
# sbt package && /opt/spark/bin/spark-submit --class StreamHandler --master local[*] --packages  "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.apache.hudi:hudi-spark-bundle_2.11:0.9.0,org.apache.spark:spark-avro_2.11:2.4.4"  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' target/scala-2.11/stream-handler_2.11-1.0.jar


