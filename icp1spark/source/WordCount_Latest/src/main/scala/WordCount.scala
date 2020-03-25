/**
 * Illustrates flatMap + countByValue for wordcount.
 */

import org.apache.spark._

object WordCount {
    def main(args: Array[String]) {

      System.setProperty("hadoop.home.dir", "C:\\winutils")

      //val inputFile = args(0)
      //val outputFile = args(1)
      val conf = new SparkConf().setMaster("local").setAppName("test").set("spark.local.dir", "/tmp/spark-temp");

      //val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)

      // Load our input data.
      //val input =  sc.textFile(inputFile)
      val input = sc.textFile("input/input.txt")

      // Split up into words.
      val words = input.flatMap(line => line.split("\\s+"))
      words.foreach(println)

      // Transform into word and count.
      val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
      counts.foreach(println)
      // Save the word count back out to a text file, causing evaluation.
      counts.saveAsTextFile("countoutput");

    }



}
