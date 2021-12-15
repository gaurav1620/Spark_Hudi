// spark-shell
import spray.json._
import org.apache.hudi.QuickstartUtils._
import scala.util.parsing.json._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._



def jsonToMap(js:String) : Map[String,Any] = {
  return JSON.parseFull(js).get.asInstanceOf[Map[String,Any]]
}
def getValFromRecord(df: Dataset) : Any = {
}
val record_keys = Set(calendarTime, name, epoch, numerics, snapshot, hostIdentifier, counter, unixTime, action)


x.limit(10).select($"value").foreach(zp => println(jsonToMap(zp(0).toString()).get("name").get))


val tableName = "osquerydb6"
val basePath = "file:///tmp/osquerydb6"
// gg
JSON.parseFull(x.limit(10).select($"value").head()(0).toString()).get.asInstanceOf[Map[String, Any]].get("name")
spark.read.format("hudi").load(basePath).limit(2).select($"value").foreach(x => println(x(0).getClass))
spark.read.format("hudi").load(basePath).limit(1).select($"value").foreach(x => println(JSON.parseFull(x(0).toString()).get.getClass))

spark.read.format("hudi").load(basePath).select($"cpu_brand").show()
spark.read.format("hudi").option("as.of.instant", "20211124091108").load(basePath).show()

val dataGen = new DataGenerator
val tripsSnapshotDF = spark.read.format("hudi").load(basePath)
tripsSnapshotDF.show()
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

//Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi table as below.
val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD.key(), "ts").
  option(RECORDKEY_FIELD.key(), "uuid").
  option(PARTITIONPATH_FIELD.key(), "partitionpath").
  option(TBL_NAME.key(), tableName).
  mode(Overwrite).
  save(basePath)

val tripsSnapshotDF = spark.read.format("hudi").load(basePath)
//load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

val x = spark.read.format("hudi").load("/tmp/qa")
val parsed = JSON.parseFull(x.limit(10).select($"value").head()(0).toString())
//read data

df.select($"fare", $"begin_lon").show() 
df.filter($"fare" > 21).show()           // returns all the columns
//Doesnt work : ERROR
spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()


// UPDATE DATA
val updates = convertToStringList(dataGen.generateUpdates(10))
val df = spark.read.json(spark.sparkContext.parallelize(updates, 2))

df.write.format("hudi")
  .options(getQuickstartWriteConfigs)
  .option(PRECOMBINE_FIELD.key(), "ts")
  .option(RECORDKEY_FIELD.key(), "uuid")
  .option(PARTITIONPATH_FIELD.key(), "partitionpath")
  .option(TBL_NAME.key(), tableName)
  .mode(Append)
  .save(basePath)

//Time travel query                          2021/11/24 09:11:08
spark.read.format("hudi").option("as.of.instant", "20211124091108").load(basePath).show()

spark-shell
// reload data
spark.
  read.
    format("hudi").
      load(basePath).
        createOrReplaceTempView("hudi_trips_snapshot")

        val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime").map(k => k.getString(0)).take(50)
        val beginTime = commits(commits.length - 2) // commit time we are interested in

        // incrementally query data
        val tripsIncrementalDF = spark.read.format("hudi").
          option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL).
            option(BEGIN_INSTANTTIME.key(), beginTime).
              load(basePath)
              tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

              spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()
