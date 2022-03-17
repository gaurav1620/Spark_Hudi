import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, StructType}
import scala.util.control._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.from_json
    import org.apache.log4j.Logger

    import org.apache.log4j.Level


import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

import scala.util.parsing.json._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

import java.util.logging.StreamHandler
import scala.util.Try
//import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import java.time.LocalDateTime

import java.io.ByteArrayInputStream

import org.apache.commons.io.IOUtils
import sun.misc.BASE64Decoder

case class DeviceData(ts: String  ,board_version: String,computer_name: String,cpu_brand: String,cpu_logical_cores:String, cpu_microcode:String, partitionpath: String)

object StreamHandler {
	def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    def jsonToMap(js: String) : Map[String,String] = {
      return JSON.parseFull(js).get.asInstanceOf[Map[String,String]]
    }


    val spark = SparkSession.builder.
      master("local")
      .appName("spark")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "EZ3R6AEBYFXBNQX4QW5Z")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "+Kn+eo478srn2OaFK89kqSiugU+c1ztcy3+xbeZ8")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "minio:9000")
    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.signing-algorithm","S3SignerType")
      val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
      import sqlContext.implicits._

    //spark.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class", classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter], classOf[org.apache.hadoop.fs.PathFilter]);

    val cow = 1;
    var table_name = "osq_cow";
    var table_type = "COPY_ON_WRITE"
    if(cow == 0){
      table_name = "osq_mor";
      table_type = "MERGE_ON_READ"
    }

    val tablename = table_name
    val basepath = "file:///tmp/"+ table_name
    val basepath1 = "/tmp/"+table_name
    val osquery_names = List("pack_system-snapshot_some_query1", "pack_system-snapshot_some_query2", "pack_system-snapshot_some_query3","pack_system-snapshot_some_query4")
    //val osquery_names = List("pack_system-snapshot_some_query2")

     var count = 0
        val df = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", "Orders1")
          .option("failOnDataLoss", "false")
          .option("startingOffsets", "earliest")
          .load()
    val df2 =  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(partition AS INT)", "CAST(offset AS INT)", "CAST(timestamp AS STRING)")
 
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println("Writing batch : "+ batchId.toString + "having " + batchDF.count().toString + " records.")
        batchDF.persist()

        //Filter queries
          osquery_names.foreach(query_name =>{
            val outLoop = new Breaks
           outLoop.breakable{
            val filteredDF = batchDF.filter("value like '%" + query_name+ "%'")
            if(filteredDF.count() == 0)outLoop.break()

             val schema = new StructType()
              .add("name",StringType)
              .add("hostIdentifier",StringType)
              .add("calendarTime",StringType)
              .add("unixTime",StringType)
              .add("epoch",StringType)
              .add("counter",StringType)
              .add("columns",StringType)
              .add("action",StringType)

            val explodedDF = filteredDF.
              select(from_json(col("value"), schema).alias("value")).select("value.*")

            println("*********************************************")
            println(schema.getClass)
            //filteredDF.show(2,false)
            //explodedDF.select("columns").show(2,false)
            //explodedDF.show(2,false)

            //val schema2 = explodedDF.select(schema_of_json(explodedDF.select(col("columns")).first.getString(0))).as[String].first
            val replacedString = explodedDF.select(col("columns")).first.getString(0).replaceAll("\"\"","\"temp\"")

            //val json_schema = spark.read.json(explodedDF.rdd.map(lambda row: row.columns)).schema
            //println(json_schema)
            println("##################")



            //val schema2 = DataType.fromJson(replacedString.toString)
            val schema2 = explodedDF.select(schema_of_json(explodedDF.select(col("columns")).first.getString(0))).as[String].first
            val expr_string = "from_json(columns, '"+schema2.toString + "') as parsed_json"
            val finalDF = explodedDF.selectExpr(expr_string).select("parsed_json.*")
            finalDF.
              write.
              format("org.apache.hudi").
              options(getQuickstartWriteConfigs).
              option("checkpointLocation", basepath).
              option(PRECOMBINE_FIELD.key(), "timestamp").
              option(RECORDKEY_FIELD.key(), "timestamp").
              option(PARTITIONPATH_FIELD.key(), "_hoodie_partition_path").
//              option(OPERATION_OPT_KEY,"upsert").
              option("hoodie.datasource.write.table.type",table_type.toString()).
//              option("hoodie.parquet.max.file.size",String.valueOf(2  * 1024 * 1024)).
              option("hoodie.parquet.max.file.size",String.valueOf(2  * 1024 * 256)).
//              option("hoodie.parquet.small.file.limit": "204857600").
//              option("hoodie.parquet.max.file.size": "484402653184").
//              option("hoodie.upsert.shuffle.parallelism","1").
              option("hoodie.table.name", tablename.toString()).
              mode(Append).
              //save("http://minio:9000/"+query_name)
              save("s3a://"+"osquery/")
              //save("s3a://"+"osquery/" + query_name+"/")
              //save(basepath+"/"+query_name)

            //val anotherOne = explodedDF.withColumn('new_json_column', from_json(col('columns'), json_schema))

           // val totallynew = explodedDF.select(from_json(col("columns"), schema2).alias("columns")).select("columns.*")
           //val totallynew = explodedDF.select("columns.*")

            //totallynew.show(2,false)

            //println(explodedDF.schema)
              //show(2,false)

              /*
              write.
              format("org.apache.hudi").
              options(getQuickstartWriteConfigs).
              option("checkpointLocation", basepath).
              option(PRECOMBINE_FIELD.key(), "timestamp").
              option(RECORDKEY_FIELD.key(), "timestamp").
              option(PARTITIONPATH_FIELD.key(), "_hoodie_partition_path").
//              option(OPERATION_OPT_KEY,"upsert").
              option("hoodie.datasource.write.table.type",table_type.toString()).
//              option("hoodie.parquet.max.file.size",String.valueOf(2  * 1024 * 1024)).
              option("hoodie.parquet.max.file.size",String.valueOf(2  * 1024 * 256)).
//              option("hoodie.parquet.small.file.limit": "204857600").
//              option("hoodie.parquet.max.file.size": "484402653184").
//              option("hoodie.upsert.shuffle.parallelism","1").
              option("hoodie.table.name", tablename.toString()).
              mode(Append).
              //save("http://minio:9000/"+query_name)
              //save("s3://"+"osquery/")
              save("s3a://"+"osquery/" + query_name+"/")



            println("    " + query_name + " has " + filteredDF.count().toString + " records.")
            if(filteredDF.count() == 0)outLoop.break()
            val parsed = filteredDF.select("value").rdd.map(r => r(0)).collect().map(x =>x.toString)
            //val gp = parsed.map(x => x.toString.slice(x.toString.indexOf("[") + 1, x.toString.indexOf("]") ))
            print(parsed.getClass)

            val gp = parsed.map(x => x.toString.slice(x.toString.indexOf("{",x.toString.indexOf("{") + 1) , x.toString.indexOf("}") +1 ))

            var str = "["
            gp.foreach(x => {str = str + x.toString + ","})
            str = str.dropRight(1)
            str = str + "]"

            //if(str == "]" || (str.count(_ == 'l') >= str.length()-2))outLoop.break()

            str = str.replaceAll("\"\"","\"temp\"")
//            println(gp.getClass)
//            println("Incoming " + str)
//
            val tempStr = Seq(str)
            if(tempStr.isEmpty)outLoop.break()
            println(str)
            val df_final = tempStr.toDF("json")
            val schema = df_final.select(schema_of_json(df_final.select(col("json")).first.getString(0))).as[String].first
//            println(schema.toString)
            val expr_string = "from_json(json, '"+schema.toString + "') as parsed_json"
//            println( "EXPR : " + expr_string)
//            df_final.select(to_json(schema))
            df_final.show()
            val parsedJson1 = df_final.selectExpr(expr_string)
            val data = parsedJson1.selectExpr("explode(parsed_json) as json").select("json.*").withColumn("id",monotonicallyIncreasingId())

            var df_1 = filteredDF.withColumn("id1",monotonically_increasing_id())
            var df_2 =data.withColumn("id2", monotonically_increasing_id())

            def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
            val hasTSasColumn = hasColumn(df_2, "timestamp")

            if(hasTSasColumn == true){
              df_2 = df_2.drop("timestamp")
            }


            val data_new = df_1.join(df_2,col("id1")===col("id2"),"inner")
              .drop("id1","id2")

//             data_new.show()
            data_new.
           //filteredDF.show()
            //filteredDF.
              write.
              format("org.apache.hudi").
              options(getQuickstartWriteConfigs).
              option("checkpointLocation", basepath).
              option(PRECOMBINE_FIELD.key(), "timestamp").
              option(RECORDKEY_FIELD.key(), "timestamp").
              option(PARTITIONPATH_FIELD.key(), "_hoodie_partition_path").
//              option(OPERATION_OPT_KEY,"upsert").
              option("hoodie.datasource.write.table.type",table_type.toString()).
//              option("hoodie.parquet.max.file.size",String.valueOf(2  * 1024 * 1024)).
              option("hoodie.parquet.max.file.size",String.valueOf(2  * 1024 * 256)).
//              option("hoodie.parquet.small.file.limit": "204857600").
//              option("hoodie.parquet.max.file.size": "484402653184").
//              option("hoodie.upsert.shuffle.parallelism","1").
              option("hoodie.table.name", tablename.toString()).
              mode(Append).
              //save("http://minio:9000/"+query_name)
              //save("s3://"+"osquery/")
              save("s3a://"+"osquery/" + query_name+"/")
              //save(basepath+"/"+query_name)
      */
        }
      })
        batchDF.unpersist()
        println("Done writing the batch !")
      }
      df2.start().awaitTermination()

	}
}
