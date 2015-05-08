package com.diegomagalhaes.spark

import java.time.format.DateTimeFormatter
import java.util.Locale

import com.diegomagalhaes.nginxlogparser.{NginxLineParser, NginxLogRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.language.postfixOps

object RecomendationApp {
  val logger = LoggerFactory.getLogger("name")

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

    lazy val parser = new NginxLineParser
    val conf = new SparkConf()
                      .setMaster("local")
                      .setAppName("Spark Recomendation App")
                      .set("spark.executor.memory","1G")
                      .set("spark.rdd.compress","true")
                      .set("spark.storage.memoryFraction","1")
                      .set("spark.driver.memory","1G")
                      .set("spark.reducer.maxMbInFlight","64")
                      .set("spark.broadcast.blockSize","1024")
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .set("spark.localExecution.enabled","true")
                      .registerKryoClasses(Array(classOf[NginxLogRecord]))


    val sc = new SparkContext(conf)
    val path = "C:\\temp\\hive\\ads\\access.log-2015-05-*.gz"
    val data = sc.textFile(path,2).flatMap(parser parse).cache()

    val clickData = data
                      .filter(r => r.verb != null && r.URL.contains("ck.php") && (r.MSISDN != "-" || r.XCALL != "-"))
                      .map(x =>
                        (
                          formatDate(x.dateTime),
                          extractMobileNumber(x.MSISDN, x.XCALL),
                          x.UserAgent,
                          x.URL,
                          "C"
                        )
                      )

//    val visits = data
//      .filter(_.URL.contains("ck.php"))
//      .filter(r => r.MSISDN != "-" || r.XCALL != "-")
//      .map(x => (x.dateTime, if (x.MSISDN == "-") x.XCALL else x.MSISDN, x.UserAgent, x.URL, "V"))

    val sample = clickData take 5
    sample foreach println

    sc stop
  }
}
