import com.diegomagalhaes.nginxlogparser.NginxLineParser
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.postfixOps

/**
 * Created by diego.magalhaes on 5/6/2015.
 */
object RecomendationApp {
  def main(args: Array[String]) {

    //https://issues.apache.org/jira/browse/SPARK-2356
    System.setProperty("hadoop.home.dir", "c:\\temp\\")

    val parser = new NginxLineParser
    val conf = new SparkConf()
                      .setMaster("local")
                      .setAppName("Spark Recomendation App")
                      .set("spark.executor.memory","2G")
                      .set("spark.rdd.compress","true")
                      .set("spark.storage.memoryFraction","1")
                      .set("spark.driver.memory","1G")
                      .set("spark.reducer.maxMbInFlight","256")
                      .set("spark.broadcast.blockSize","10240")


    val sc = new SparkContext(conf)
    val path = "C:\\temp\\hive\\access.log-2015-05-05-1430794906"
    val data = sc.textFile(path,2)
                      .map(parser parse _ )
                      .filter(_.isDefined)
                      .map(_.get)
                      .filter(_.verb != null)
                      .cache()

    val clicks = data
                    .filter(_.URL.contains("ck.php"))
                    .filter(r => r.MSISDN != "-" || r.XCALL != "-")
                    .map(x => (x.dateTime, if (x.MSISDN == "-") x.XCALL else x.MSISDN, x.UserAgent, x.URL, "C"))

    val visits = data
      .filter(_.URL.contains("ck.php"))
      .filter(r => r.MSISDN != "-" || r.XCALL != "-")
      .map(x => (x.dateTime, if (x.MSISDN == "-") x.XCALL else x.MSISDN, x.UserAgent, x.URL, "V"))
  }
}
