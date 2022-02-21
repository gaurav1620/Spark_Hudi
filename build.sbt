name := "Stream Handler"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
	"org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
	"org.apache.hudi" %% "hudi-spark-bundle" % "0.9.0" % "provided",
	//"org.apache.hadoop" %% "hadoop-aws" % "2.6.0" % "provided",
	"io.minio" % "minio" % "6.0.13" % "provided",
	//"io.minio" % "minio" % "8.3.6" % "provided",
	"commons-io" % "commons-io" % "2.5" % "provided",
	"org.slf4j" % "slf4j-api"% "1.7.5",
	"ch.qos.logback"% "logback-classic" % "1.0.9",
	//"org.apache.hadoop" % "hadoop-common" % "2.3.0"% "provided",
	//"org.apache.hadoop" % "hadoop-hdfs-client" % "3.2.0" % "provided",
//	"org.apache.spark" %% "spark-hive" % "1.0.0" % "provided",


	//  "org.apache.hudi" % "hudi-spark" % "0.5.0-incubating",
//	"com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",
//	"com.datastax.cassandra" % "cassandra-driver-core" % "4.0.0"
//	"com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",
//	"com.datastax.cassandra" % "cassandra-driver-core" % "4.0.0"
)
