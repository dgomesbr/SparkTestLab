package com.diegomagalhaes.spark

import java.time.format.DateTimeFormatter
import java.util.Locale

import com.diegomagalhaes.nginxlogparser.{NginxLineParser, NginxLogRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.postfixOps

object RecomendationApp {

  private def extractMobileNumber(msisdn: String, xcall: String) = {
    msisdn match {
      case "-" => xcall
      case _ => msisdn
    }
  }

  private def formatDate(date: String) = {
    val dt_in = DateTimeFormatter.ofPattern("dd/MMM/YYYY:HH:mm:ss Z",Locale.US).parse(date)
    DateTimeFormatter.ofPattern("YYYY-MM-dd hh:mm:ss",Locale.US).format(dt_in)
  }
  def main(args: Array[String]) {
    //https://issues.apache.org/jira/browse/SPARK-2356
    System.setProperty("hadoop.home.dir", "c:\\temp\\")

    val parser = new NginxLineParser
    val conf = new SparkConf()
                      .setMaster("local")
                      .setAppName("Spark Recomendation App")
                      .set("spark.executor.memory","1G")
                      .set("spark.rdd.compress","true")
                      .set("spark.storage.memoryFraction","1")
                      .set("spark.driver.memory","1G")
                      .set("spark.broadcast.blockSize","128")
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .set("spark.localExecution.enabled","true")
                      .registerKryoClasses(Array(classOf[NginxLogRecord]))


    val sc = new SparkContext(conf)

    //val adsPath = "C:\\temp\\hive\\ads\\access.log-2015-05-*.gz"
    val adsPath = "C:\\temp\\hive\\ads\\access.log-2015-05-06-1430881321.gz"
    val clickData = sc.textFile(adsPath,2).flatMap(parser parse).filter(_.verb != null).cache()
    val clicks = collectRecords(clickData).filter(_._4.contains("ck.php"))

    val visitPath = "C:\\temp\\hive\\ads\\access.log-2015-05-*.gz"
    val visitData = sc.textFile(visitPath ,2).flatMap(parser parse).filter(_.verb != null).cache()
    val visits = collectRecords(visitData)

    val data = sc.union(clicks, visits).sortBy(_._1)
    data.saveAsTextFile("C:\\temp\\hive\\recomendacao.csv")
    sc stop()
  }

  def collectRecords(clickData: RDD[NginxLogRecord]): RDD[(String, String, String, String, String)] = {
    clickData
      .filter(r => (r.MSISDN != "-" || r.XCALL != "-"))
      .map(x =>
      (
        formatDate(x.dateTime),
        extractMobileNumber(x.MSISDN, x.XCALL),
        x.UserAgent,
        x.URL,
        "C"
        )
      )
  }
}
