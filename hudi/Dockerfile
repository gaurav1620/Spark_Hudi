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


RUN wget https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz

RUN git clone https://github.com/gaurav1620/Spark_Hudi.git \
  && rm Spark_Hudi/code.scala \
  && mv Spark_Hudi StreamHandler 

RUN rm -rf /opt/spark && mkdir /opt/spark \
  && tar -xvzf spark-2.4.5-bin-hadoop2.7.tgz \
  && cp -r spark-2.4.5-bin-hadoop2.7/. /opt/spark/. \
  && echo "export SPARK_HOME=/opt/spark" >> ~/.profile \
  && echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.profile \
  && echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile 
#&& source ~/.profile

# install sbt

RUN  wget https://github.com/sbt/sbt/releases/download/v1.3.9/sbt-1.3.9.zip \
  && apt-get install zip unzip -y\
  && unzip sbt-1.3.9.zip \
  && rm -rf /opt/sbt \
  && mkdir /opt/sbt \
  && cp -r sbt/* /opt/sbt


# the order of commands below is imp
RUN sed -i 's/localhost/kafka/g' /StreamHandler/src/main/scala/StreamHandler.scala
RUN cd StreamHandler && /sbt/bin/sbt package

ENTRYPOINT ["tail", "-f", "/dev/null"]

#RUN apt-get update \
#  && apt-get install apt-transport-https curl gnupg -yqq \
#  && "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list \
#  && "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list \
#  && curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import \
#  && chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg \
#  && apt-get update \
#  && apt-get install sbt

#RUN apt-get install apt-transport-https curl gnupg -yqq \
#  && echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list \
#  && echo "deb https://repo.scala-sbt.org/scalasbt/debian/" | tee /etc/apt/sources.list.d/sbt_old.list \
#  && curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import \
#  && chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg \
#  && apt-get update && apt-get install sbt





#  echo "hello world" | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic Orders1

# kafka_2.11-2.4.1/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic Orders1

#sbt package && /opt/spark/bin/spark-submit --class StreamHandler --master local[*] --packages  "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.apache.hudi:hudi-spark-bundle_2.11:0.9.0,org.apache.spark:spark-avro_2.11:2.4.4"  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' target/scala-2.11/stream-handler_2.11-1.0.jar


