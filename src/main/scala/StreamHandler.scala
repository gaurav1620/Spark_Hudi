import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import java.time.LocalDateTime


case class DeviceData(ts: String  ,board_version: String,computer_name: String,cpu_brand: String,cpu_logical_cores:String, cpu_microcode:String, partitionpath: String)

object StreamHandler {
	def main(args: Array[String]) {

		// initialize Spark
		val spark = SparkSession
			.builder
			.appName("Stream Handler")
			.getOrCreate()


		spark.streams.addListener(new StreamingQueryListener() {
			override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
				println("Query started: " + queryStarted.id)
			}
			override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
				println("Query terminated: " + queryTerminated.id)
			}
			override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
//				println("Query made progress: " + queryProgress.progress)
			}
		})


		val tableName = "osquerydb6"
		val basePath = "file:///tmp/osquerydb6"
		val dataGen = new DataGenerator
		val inserts = convertToStringList(dataGen.generateInserts(10))
		val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
//				df.write
//					 .format("hudi").
//				 	options(getQuickstartWriteConfigs).
//				 	option(PRECOMBINE_FIELD.key(), "ts").
//				 	option(RECORDKEY_FIELD.key(), "uuid").
//				 	option(PARTITIONPATH_FIELD.key(), "partitionpath").
//				 	option(TBL_NAME.key(), tableName).
//				 	mode(Overwrite).
//				 	save(basePath)


		import spark.implicits._

		// read from Kafka
		val inputDF = spark
			.readStream
			.format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
			.option("kafka.bootstrap.servers", "localhost:9092")
			.option("subscribe", "Orders1")
			.load()

		val rawDF = inputDF.selectExpr("CAST(value AS STRING )").as[String]

		val expandedDF_Mod = rawDF.map(row => row.split(","))
			.map(row => row.map(x => x.split("\"")(3)))
		val expandedDF = rawDF.map(row => row.split(","))
			.map(row => DeviceData(
				LocalDateTime.now().toString(),
					row(3).split("\"")(3),
					row(4).split("\"")(3),
					row(5).split("\"")(3),
					row(6).split("\"")(3),
					row(7).split("\"")(3),
				"americas/brazil/sao_paul"
		))
		val query = expandedDF
			.writeStream
//			.trigger(Trigger.ProcessingTime("2 seconds"))
			.foreachBatch{(batchDF: Dataset[DeviceData], batchID: Long)=>
//				batchDF.createOrReplaceTempView("temp_table")
//				batchDF.sparkSession.sql("select * from temp_table").show()
				println("Started execution *********************")
//				batchDF.select($"arg1").write
//				batchDF.withColumn("ts", current_timestamp())
				batchDF.selectExpr("ts as key","board_version","computer_name","cpu_brand", "cpu_logical_cores", "cpu_microcode").write
					.format("hudi").
					options(getQuickstartWriteConfigs).
					option(PRECOMBINE_FIELD.key(), "key").
					option(RECORDKEY_FIELD.key(), "key").
					option(PARTITIONPATH_FIELD.key(), "_hoodie_partition_path").
					option(TBL_NAME.key(), tableName).
					mode(Append).
					save(basePath)
			}
			.outputMode("update")
//			.format("console")
			.start()


		query.awaitTermination();
//		query.awaitTermination(10000);
//		spark.read
//			.format("org.apache.hudi")
//			.option("basePath", "file:///tmp/osquery1")
//			.load().show()

//
//		val dataStreamReader = spark
//			.readStream
//			.format("kafka")
//			.option("kafka.bootstrap.servers", "localhost:9092")
//			.option("subscribe", "Orders1")
//			.option("startingOffsets", "latest")
//			.option("maxOffsetsPerTrigger", 100000)
//			.option("failOnDataLoss", false)
//
//		val df3 = dataStreamReader.load()
//			.selectExpr(
//				"topic as kafka_topic",
//		"CAST(partition AS STRING) kafka_partition",
//		"cast(timestamp as String) kafka_timestamp",
//		"CAST(offset AS STRING) kafka_offset",
//		"CAST(key AS STRING) kafka_key",
//		"CAST(value AS STRING) kafka_value",
//		"current_timestamp() current_time"
//		)
//		.selectExpr(
//			"kafka_topic",
//		"concat(kafka_partition,'-',kafka_offset) kafka_partition_offset",
//		"kafka_offset",
//		"kafka_timestamp",
//		"kafka_key",
//		"kafka_value",
//		"substr(current_time,1,10) partition_date")
//
//		val query3 = df3
//			.writeStream
//			.queryName("demo")
//		.foreachBatch { (batchDF: DataFrame, _: Long) => {
//			batchDF.persist()
//
//			println(LocalDateTime.now() + "start writing cow table")
//			batchDF.write
//				.format("hudi").
//				options(getQuickstartWriteConfigs).
//				option(PRECOMBINE_FIELD.key(), "ts").
//				option(RECORDKEY_FIELD.key(), "uuid").
//				option(PARTITIONPATH_FIELD.key(), "partitionpath").
//				option(TBL_NAME.key(), tableName).
//				mode(Overwrite).
//				save(basePath)
//
//			println(LocalDateTime.now() + "finish")
//			batchDF.unpersist()
//		}
//		}
//			.option("checkpointLocation", "/tmp/sparkHudi/checkpoint/")
//			.start()
//
//		query3.awaitTermination()


	}
}