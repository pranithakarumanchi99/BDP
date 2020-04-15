import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source

object BonusDataFrame {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .appName("Bonus")
      .master("local[*]")
      .getOrCreate()
    val sqlContext = sparkSession.sqlContext



    val sc = sparkSession.sparkContext
    val fileRdd =
      sc.textFile("survey.csv").filter(x => !x.contains("Timestamp")).map(_.split(",")).map { x
      =>
        org.apache.spark.sql.Row(x: _*)
      }

    val schema = StructType(
      StructField("Timestamp", StringType, false) ::
        StructField("Age", StringType, false) ::
        StructField("Gender", StringType, false) ::
        StructField("Country", StringType, false) ::
        StructField("state", StringType, false) ::
        StructField("self_employed", StringType, false) ::
        StructField("family_history", StringType, false) ::
        StructField("treatment", StringType, false) ::
        StructField("work_interfere", StringType, false) ::
        StructField("no_employees", StringType, false) ::
        StructField("remote_work", StringType, false) ::
        StructField("self_employed", StringType, false) ::
        StructField("benefits", StringType, false) ::
        StructField("care_options", StringType, false) ::
        StructField("wellness_program", StringType, false) ::
        StructField("seek_help", StringType, false) ::
        StructField("anonymity", StringType, false) ::
        StructField("mental_health_consequence", StringType, false) ::
        StructField("phys_health_consequence", StringType, false) ::
        StructField("coworkers", StringType, false) ::
        StructField("supervisor", StringType, false) ::
        StructField("mental_health_interview", StringType, false) ::
        StructField("phys_health_interview", StringType, false) ::
        StructField("mental_vs_physical", StringType, false) ::
        StructField("obs_consequence", StringType, false) ::
        StructField("comments", StringType, false) ::
        Nil)

    val sqlDf = sqlContext.createDataFrame(fileRdd, schema)
    sqlDf.show(5)

  }



}
