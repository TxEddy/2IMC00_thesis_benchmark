import com.github.tototoshi.csv.CSVWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import java.io.File

object SparkDemo {
  def main(args : Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.INFO)
    val sc = new SparkContext("local[*]" , "SparkDemo")
    val lines = sc.textFile("/Library/Java/JavaVirtualMachines/jdk1.8.0_192.jdk/Contents/Home/COPYRIGHT")
    val words = lines.flatMap(line => line.split(' '))
    val wordsKVRdd = words.map(x => (x,1))
    val count = wordsKVRdd.reduceByKey((x,y) => x + y).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2, x._1)).take(10)
    count.foreach(println)
  }
}

