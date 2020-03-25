

import org.apache.spark._

object secondary {
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
      //reading input from file
      val countryinputRDD = sc.textFile("input/secondary.txt")

      //file content
      countryinputRDD.foreach(println)
      //mapping
      val mappedrdd = countryinputRDD.map(_.split(",")).map { n => ((n(0), n(1)),n(2))}

      mappedrdd.foreach(println)
      val reducers = 4;

      val reducedrdd = mappedrdd.groupByKey(reducers).mapValues(iter => iter.toList.sortBy(k => k))
      println("output - reduced")
      reducedrdd.foreach { println }
      //saving output to file (actions)
      reducedrdd.saveAsTextFile("secondaryoutput");

    }



}
