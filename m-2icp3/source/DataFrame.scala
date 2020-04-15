import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DataFrame {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .appName("spark-icp-3-sql")
      .master("local[*]")
      .getOrCreate()
    val sqlContext = sparkSession.sqlContext
    // Part-1_question_1 : importing dataset into data frame
    val dataframe = sqlContext.read.option("header", "true").option("inferSchema", "true").csv("survey.csv")
    dataframe.show(10)
    println("First 10 Rows of DataFrame")
    dataframe.createOrReplaceTempView("survey")
    // Part-1_question_2 : saving the data frame
    dataframe.write.mode("overwrite").format("csv").save("datasavedfile")

    // Part-1_question_3 : droppping duplicate

    println(dataframe.count())
    println("count before removing duplicates ")

    dataframe.dropDuplicates()

    println(dataframe.count())
    println("count after removing duplicates ")
    //  part-1 question-4 : creating two dataframes for union operations
    val df1 = dataframe.limit(10)
    val df2 = dataframe.limit(10)
     // performing inner join and storing data in df1
    //df1.join(df2, df1.col("Country").equalTo(df2.col("Country")), "inner").show()
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
     //union operation
    val unionDf = df1.union(df2)
    //ordering union dataframe by country
    unionDf.orderBy("Country").show()


     //Part-1 question-4 Grouping by treatment
    dataframe.groupBy(col("treatment"), col("Country")).avg("Age").show()
    println("groupBy treatment country ")

       //Part -2

    //1-Apply the basic queries related to Joins and aggregate functions(at least 2) part-2

     //joining data frames inner join based on country
    val joinedDataFrame = df1.join(df2, df1.col("Country").equalTo(df2.col("Country")), "inner")

    joinedDataFrame.show()

    sparkSession.sql("select df1.Age,df1.Country from df1 inner join df2 on df1.Country == df2.Country and df1.Gender == df2.Gender").show()

    dataframe.groupBy("Country").mean("Age").show()
    println("Max,Min adn average aggregate function")
    sparkSession.sql("SELECT max(`Age`) FROM survey").show()
    println("Max aggregate function")
    sparkSession.sql("SELECT min(`Age`) FROM survey").show()
    println("Min aggregate function")
    sparkSession.sql("SELECT avg(`Age`) FROM survey").show()
    println("average aggregate function")
    sparkSession.sql("SELECT avg(`Age`),`Country` FROM survey group by `Age`,`Country`").show()

    //2-Write a query to fetch 13th Row in the dataset.


    println(dataframe.take(13).last)
    println("13th row  13 rows of dataframe")
  }
}