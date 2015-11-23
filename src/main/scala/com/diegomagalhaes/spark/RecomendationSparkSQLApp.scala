package com.diegomagalhaes.spark

import java.text.SimpleDateFormat
import java.util.Locale

import com.diegomagalhaes.nginxlogparser.{NginxLineParser, NginxLogRecord}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by diego.magalhaes on 11/20/2015.
  */
object RecomendationSparkSQLApp {

  val DateFormatterInput = new SimpleDateFormat("dd/MMM/YYYY:HH:mm:ss Z",Locale.US)
  val DateFormatterOutput = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss",Locale.US)

  /**
    * Function to be used as UDF for Spark SQL
    * @param date date to be formatted
    * @return Date in the "YYYY-MM-dd hh:mm:ss" pattern
    */
  def formatDate(date:String) = DateFormatterOutput.format(DateFormatterInput.parse(date))


  def main(args: Array[String]) {

    //https://issues.apache.org/jira/browse/SPARK-2356
    System.setProperty("hadoop.home.dir", "c:\\temp\\")


      // Run the word count
      RecomendationSparkSQLApp.execute(
        master    = Some("local[*]"),
        args      = args.toList,
        jars      = Seq.empty//List(SparkContext.jarOfObject(this).get)
      )

      // Exit with success
      System.exit(0)
/*    def getConfigurationSpec: SparkConf = {
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
    }*/
  }

  def execute(master: Option[String], args: List[String], jars: Seq[String] = Nil) {
    val sc = {
      val conf = new SparkConf()
        .setAppName(RecomendationSparkSQLApp.getClass.getName)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.ui.showConsoleProgress", "false")
        .registerKryoClasses(Array(classOf[NginxLogRecord], classOf[LiteCsvWriter]))
        .setJars(jars)
      for (m <- master) {
        conf.setMaster(m)
      }
      new SparkContext(conf)
    }

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.udf.register("customDateFormatter", formatDate(_:String))

    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.implicits._

    val parser = new NginxLineParser
    val data = sc.textFile("C:\\temp\\hive\\access.log-2015-11-15-1447552921.gz")
      //.textFile("s3n://bemobilogs/vpc-8656a8e3/Tim-ADS-ec2/2015/11/15/access/ip-192-168-0-12/*")
      .mapPartitions(_.flatMap(parser.parse))
      .persist(org.apache.spark.storage.StorageLevel.DISK_ONLY) // cache!

    data.toDF().registerTempTable("logs")
    val click_sql_with_date =
    //s"SELECT verb, ResponseCode, count(ResponseCode) FROM logs WHERE verb is not null GROUP BY verb, ResponseCode ORDER BY verb"
    """
      SELECT

      FROM
        logs
      WHERE
        (MSISDN != '-' OR XCALL != '-') AND
        verb is not null AND
        instr(URL,'ck.php') > 0
    """
    val validLogs = sqlContext.sql(click_sql_with_date)
    validLogs.foreach(println)//.take(1).foreach(println)
  }
}
