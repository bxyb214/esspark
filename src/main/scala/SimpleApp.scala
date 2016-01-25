/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.elasticsearch.spark._ 
import org.elasticsearch.hadoop.mr.EsOutputFormat
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}


object SimpleApp {
  def main(args: Array[String]) {
    //val logFile = "/user/hadoop/test/README.md" // Should be some file on your system
//    val conf = new SparkConf().setAppName("Simple Application")
    val conf = new SparkConf().setAppName("Test elasticsearch.")//.setMaster(master)
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)
    /*val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))*/
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    val esNodes = "9.115.69.86"
    val esResource = "test/hadoopDataset"
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set(ConfigurationOptions.ES_RESOURCE, esResource)
    jobConf.set(ConfigurationOptions.ES_NODES, esNodes)
    sc.makeRDD(Seq(numbers, airports)).saveToEs(esResource)
    
/*    esNodes match {
      case Some(node) => jobConf.set(ConfigurationOptions.ES_NODES, node)
      case _ => // Skip it
    }*/
  }
}
