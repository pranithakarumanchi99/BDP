



import org.apache.spark._
object MergeSort {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MergeSort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    def mergeSort(xs: List[Int]): List[Int] = {
      println("Merge sort iteration")
      xs.foreach(print)

      val n = xs.length / 2
      if (n == 0) xs
      else {
        def merge(xs: List[Int], ys: List[Int]): List[Int] =
          (xs, ys) match {
            case (Nil, ys) => ys
            case (xs, Nil) => xs
            case (x :: xs1, y :: ys1) =>
              if (x < y) x :: merge(xs1, ys)
              else y :: merge(xs, ys1)
          }

        val (left, right) = xs splitAt (n)
        merge(mergeSort(left), mergeSort(right))

      }
    }

    val list = List(9, 8, 5, 2, 11, 3, 4, 12)
    val result = sc.parallelize(Seq(list)).map(mergeSort)
    result.foreach(println)
    println(result)

  }
}