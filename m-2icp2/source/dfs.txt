import org.apache.spark.{SparkConf, SparkContext}
object DFS {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    val conf = new SparkConf().setAppName("Depthfirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    type V = Int
    type Graph = Map[V, List[V]]
    val g: Graph = Map(1 -> List(2,3,5), 2 -> List(1,3,6), 3 -> List(3,4), 4 -> List(1,3),5 -> List(1,6),6 -> List(1,2))
    //I want this to return results in the different layers that it finds them (hence the list of list of vertex)

    def DFS(start: V, g:Graph): List[V] = {

      def DFS0(v: V, visited: List[V]): List[V] = {
        if (visited.contains(v))
          visited
        else {
          val neighbours:List[V] = g(v) filterNot visited.contains
          neighbours.foldLeft(v :: visited)((b,a) => DFS0(a,b))
        }
      }
      DFS0(start,List()).reverse
    }

    val dfsresult=DFS( start = 2,g)
    println("Resulted path is --->>>", dfsresult.mkString(","))

  }
}