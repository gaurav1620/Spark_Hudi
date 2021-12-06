import org.apache.spark.sql._
import scala.util.parsing.json._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
//import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import java.time.LocalDateTime


case class DeviceData(ts: String  ,board_version: String,computer_name: String,cpu_brand: String,cpu_logical_cores:String, cpu_microcode:String, partitionpath: String)

object StreamHandler {
	def main(args: Array[String]) {

    val spark = SparkSession.builder.
      master("local")
      .appName("spark")
      .getOrCreate()

    val tablename = "qa"
    val basepath = "file:///tmp/qa"
    val basepath1 = "/tmp/qa"
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "Orders1")
      .option("failOnDataLoss", "false")
      .option("startingOffsets", "earliest")
//      .load().writeStream.format("console").start().awaitTermination()
      .load()

//    df.withColumn()
//    spark.read.format("hudi").load(basepath).show()

    val rawDF = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)", "CAST(topic AS STRING)",  "CAST(partition AS INT)", "CAST(offset AS INT)", "CAST(timestamp AS STRING)" );
//    rawDF.select("value").writeStream.format("console").start().awaitTermination()
//      rawDF.writeStream.format("console").start().awaitTermination()
//
//
//
//                options(getQuickstartWriteConfigs).
//                option(PRECOMBINE_FIELD.key(), "key").
//                option(RECORDKEY_FIELD.key(), "key").
//                option(PARTITIONPATH_FIELD.key(), "_hoodie_partition_path").
//                option(TBL_NAME.key(), tableName).
//                mode(Append).
//                save(basePath)
    rawDF.writeStream.format("hudi").
      option(PRECOMBINE_FIELD.key(), "timestamp").
      option("checkpointLocation", basepath).
      option(RECORDKEY_FIELD.key(), "timestamp").
//      option(PARTITIONPATH_FIELD.key(),"timestamp").
      option(PARTITIONPATH_FIELD.key(), "_hoodie_partition_path").
      option(OPERATION_OPT_KEY,"insert").
      option("hoodie.insert.shuffle.parallelism","1").
      option("hoodie.datasource.write.table.type","COPY_ON_WRITE").
      option("hoodie.table.name", tablename).
      start(basepath)
      .awaitTermination()

//    val x = spark.read.format("hudi").load("/tmp/qa")
//    x.show()
//    val parsed = JSON.parseFull(x.limit(10).select($"value").head()(0).toString())





//    val hudi_options = {
//      "hoodie.table.name": "hudi_acct",
//      "hoodie.table.type": "MERGE_ON_READ",
//      "hoodie.datasource.write.operation": "upsert",
//      "hoodie.datasource.write.recordkey.field": "acctid",
//      "hoodie.datasource.write.precombine.field": "ts",
//      "hoodie.datasource.write.partitionpath.field": "date",
//      "hoodie.datasource.write.hive_style_partitioning": "true",
//      "hoodie.upsert.shuffle.parallelism": 8,
//      "hoodie.insert.shuffle.parallelism": 8,
//    }
//    df.writeStream
//      .format("hudi")
//      .outputMode("Append")
//      .option("mergeSchema", "true")
//      .option("checkpointLocation", basepath1)
//      .option("hudi.table.name", tablename)
//      .start(basepath1)
//      .awaitTermination()
//    df.writeStream
//      .format("hudi")
//      .outputMode("Append")
//      .option("mergeSchema", "true")
//      .option("checkpointLocation", "/tmp/osquery7")
//      .option(TABLE_NAME, tablename)
//      .option(RECORDKEY_FIELD_OPT_KEY, "kafka_partition_offset")
//      .option(PARTITIONPATH_FIELD_OPT_KEY, "partition_date")
//      .start("/tmp/osquery7")
//      .awaitTermination()
//    df.write.format("hudi").  options(getQuickstartWriteConfigs).  option(PRECOMBINE_FIELD_OPT_KEY, "ts").  option(RECORDKEY_FIELD_OPT_KEY, "uuid") .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").option("TABLE_NAME", tablename).mode(Overwrite).  save(basepath)

    // read the data
    //Currently we are reading only key and value from the table.
//    val df1=spark.read.format("delta").option("versionAsof",1).load("/home/kushal/Desktop/tmp1/people-10m")
//    df1.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").show(50,false)
//    println(df1.head())
    /*

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


      val tablename = "osquerydb7"
      val basepath = "file:///tmp/osquerydb7"
      val datagen = new datagenerator
      val inserts = converttostringlist(datagen.generateinserts(10))
      val df = spark.read.json(spark.sparkcontext.parallelize(inserts, 2))
  //				df.write
  //					 .format("hudi").
  //				 	options(getquickstartwriteconfigs).
  //				 	option(precombine_field.key(), "ts").
  //				 	option(recordkey_field.key(), "uuid").
  //				 	option(partitionpath_field.key(), "partitionpath").
  //				 	option(tbl_name.key(), tablename).
  //				 	mode(overwrite).
  //				 	save(basepath)


      import spark.implicits._

      // read from Kafka
      val inputDF = spark
        .readStream
        .format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "Orders1")
        .option("failOnDataLoss", "false")
        .option("startingOffsets", "earliest")
        .load()


//      val rawDF = inputDF.selectExpr("CAST(value AS STRING )").as[String]
		val rawDF = inputDF.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)");

//		val expandedDF_Mod = rawDF.map(row => row.split(","))
//        .map(row => row.map(x => x.split("\"")(3)))
//      val expandedDF = rawDF.map(row => row.split(","))
//        .map(row => DeviceData(
//          LocalDateTime.now().toString(),
//            row(3).split("\"")(3),
//            row(4).split("\"")(3),
//            row(5).split("\"")(3),
//            row(6).split("\"")(3),
//            row(7).split("\"")(3),
//          "americas/brazil/sao_paul"
//      )

		rawDF.write
			.format("hudi").
			options(getQuickstartWriteConfigs).
			option(PRECOMBINE_FIELD.key(), "ts").
			option(RECORDKEY_FIELD.key(), "uuid").
			option(PARTITIONPATH_FIELD.key(), "partitionpath").
			option(TBL_NAME.key(), tableName)
			.mode(Append)
			.save(basePath)

//		var count = 0
//      val query = expandedDF
//        .writeStream
//  //			.trigger(Trigger.ProcessingTime("2 seconds"))
//        .foreachBatch{(batchDF: Dataset[DeviceData], batchID: Long)=>
//  //				batchDF.createOrReplaceTempView("temp_table")
//  //				batchDF.sparkSession.sql("select * from temp_table").show()
//          count+=1
//          println("Writing batch : " + count.toString())
//  //				batchDF.select($"arg1").write
//  //				batchDF.withColumn("ts", current_timestamp())
//          batchDF.selectExpr("ts as key","board_version","computer_name","cpu_brand", "cpu_logical_cores", "cpu_microcode").write
//            .format("hudi").
//            options(getQuickstartWriteConfigs).
//            option(PRECOMBINE_FIELD.key(), "key").
//            option(RECORDKEY_FIELD.key(), "key").
//            option(PARTITIONPATH_FIELD.key(), "_hoodie_partition_path").
//            option(TBL_NAME.key(), tableName).
//            mode(Append).
//            save(basePath)
//          println("Wrote batch " + count.toString())
//        }
//        .outputMode("update")
//  //			.format("console")
//        .start()
//			query.awaitTermination();
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
     */
	}
}