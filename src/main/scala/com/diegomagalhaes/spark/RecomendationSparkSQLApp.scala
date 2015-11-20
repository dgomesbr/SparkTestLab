package com.diegomagalhaes.spark

import java.time.format.DateTimeFormatter
import java.util.Locale

import com.diegomagalhaes.nginxlogparser.{NginxLineParser, NginxLogRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by diego.magalhaes on 11/20/2015.
  */
object RecomendationSparkSQLApp {

  val DateFormatterInput = DateTimeFormatter.ofPattern("dd/MMM/YYYY:HH:mm:ss Z",Locale.US)
  val DateFormatterOutput = DateTimeFormatter.ofPattern("YYYY-MM-dd hh:mm:ss",Locale.US)

  /**
    * Function to be used as UDF for Spark SQL
    * @param date date to be formatted
    * @return Date in the "YYYY-MM-dd hh:mm:ss" pattern
    */
  def formatDate(date:String) = DateFormatterOutput.format(DateFormatterInput.parse(date))


  def main(args: Array[String]) {

    //https://issues.apache.org/jira/browse/SPARK-2356
    System.setProperty("hadoop.home.dir", "c:\\temp\\")


    def getConfigurationSpec: SparkConf = {
      new SparkConf()
        .setMaster("local[4]")
        .setAppName("Spark Recomendation App")
        .set("spark.executor.memory", "2G")
        .set("spark.rdd.compress", "true")
        .set("spark.localExecution.enabled", "true")
        .set("spark.storage.memoryFraction", "1")
        .set("spark.driver.memory", "2G")
        .set("spark.broadcast.blockSize", "128")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(Array(classOf[NginxLogRecord], classOf[LiteCsvWriter]))
    }

    val sc = new SparkContext(getConfigurationSpec)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.udf.register("customDateFormatter", formatDate(_:String))

    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.implicits._

    val parser = new NginxLineParser
    val data = sc
      .textFile("C:\\temp\\hive\\access.log-2015-11-15-1447552921.gz")
      //.textFile("s3n://bemobilogs/vpc-8656a8e3/Tim-ADS-ec2/2015/11/15/access/ip-192-168-0-12/*")
      .mapPartitions(_.flatMap(parser.parse))
      .persist(org.apache.spark.storage.StorageLevel.DISK_ONLY) // cache!
    data.name = "logs"

    data.toDF().registerTempTable("logs")

    val click_sql_with_date =
      """
        SELECT
          customDateFormatter(dateTime) AS DATE,
          IF (MSISDN='-',XCALL,MSISDN) AS MSISDN,
          UserAgent AS UA,
          URL,
          'C' as URL_TYPE
        FROM
          logs
        WHERE
          (MSISDN != '-' OR XCALL != '-') AND
          verb is not null AND
          instr(URL,'ck.php') > 0
      """
    val validLogs = sqlContext.sql(click_sql_with_date)
    validLogs.take(1).foreach(println)

    //val file = "C:\\temp\\hive\\recomendacao"
    //val destination = "C:\\temp\\hive\\recomendacao.csv"

    //FileUtil.fullyDelete(new java.io.File(file))
    //sc.union(clicks, visits).saveAsTextFile(file)
    //merge(file, destination)

    sc.stop()
  }
}
