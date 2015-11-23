package com.diegomagalhaes.spark

import java.text.SimpleDateFormat
import java.util.Locale

import com.diegomagalhaes.nginxlogparser.{NginxLineParser, NginxLogRecord}
import com.github.tototoshi.csv.{DefaultCSVFormat, QUOTE_ALL, Quoting}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.postfixOps

object RecomendationApp {

  val DateFormatterInput = new SimpleDateFormat("dd/MMM/YYYY:HH:mm:ss Z",Locale.US)
  val DateFormatterOutput = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss",Locale.US)

  implicit val csvFormat = new DefaultCSVFormat with Serializable{
    override val delimiter: Char = ','
    override val quoting: Quoting = QUOTE_ALL
  }

  def collectRecords(clickData: RDD[NginxLogRecord]): RDD[(String, String, String, String, String)] = {
    clickData
      .filter(r => r.MSISDN != "-" || r.XCALL != "-" && r.verb != null)
      .map(x =>  ( formatDate(DateFormatterInput, DateFormatterOutput, x.dateTime), extractMobileNumber(x.MSISDN, x.XCALL), x.UserAgent, x.URL, "C" ) )
  }

  def extractMobileNumber(msisdn: String, xcall: String) = if (msisdn == "-") xcall else msisdn
  def formatDate(dfIn: SimpleDateFormat, dfOut: SimpleDateFormat, date: String) = dfOut.format(dfIn.parse(date))

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  def main(args: Array[String]) {

    //https://issues.apache.org/jira/browse/SPARK-2356
    System.setProperty("hadoop.home.dir", "c:\\temp\\")


    val conf = new SparkConf()
                      .setMaster("local[4]")
                      .setAppName("Spark Recomendation App")
                      .set("spark.executor.memory","2G")
                      .set("spark.rdd.compress","true")
                      .set("spark.storage.memoryFraction","1")
                      .set("spark.driver.memory","2G")
                      .set("spark.broadcast.blockSize","128")
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .set("spark.localExecution.enabled","true")
                      .registerKryoClasses(Array(classOf[NginxLogRecord],classOf[LiteCsvWriter]))
    val sc = new SparkContext(conf)

    val parser = new NginxLineParser
    val csvWriter = new LiteCsvWriter(csvFormat)

    val adsPath = "C:\\temp\\hive\\ads\\access.log-*"
    val clickData = sc textFile(adsPath,2) mapPartitions( _.flatMap(parser parse)  )
    val clicks = collectRecords(clickData) filter(_._4.contains("ck.php")) map( x => csvWriter toCsvString Seq(List(x._1, x._2, x._3, x._4, x._5)) )

    val visitPath = "C:\\temp\\hive\\ads\\access.log-*"
    val visitData = sc textFile(visitPath,2) mapPartitions( _.flatMap(parser parse)  )
    val visits = collectRecords(visitData) map( x => csvWriter toCsvString Seq(List(x._1, x._2, x._3, x._4, x._5)) )


    val file = "C:\\temp\\hive\\recomendacao"
    val destination = "C:\\temp\\hive\\recomendacao.csv"

    FileUtil.fullyDelete(new java.io.File(file))

    sc.union(clicks, visits).saveAsTextFile(file)
    merge(file, destination)

    sc.stop()
  }
}
