import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkContext,SparkConf}

object PA22 {

  def main(args: Array[String]): Unit = {

    // uncomment below line and change the placeholders accordingly
    //val sc = SparkSession.builder().master("spark://hartford:31215").getOrCreate().sparkContext

    // to run locally in IDE,
    // But comment out when creating the jar to run on cluster
    val sc = SparkSession.builder().master("local").getOrCreate().sparkContext

    // to run with yarn, but this will be quite slow, if you like try it too
    // when running on the cluster make sure to use "--master yarn" option
    //    val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext

    //    Empirical Observation - 1
    //First Part
    val nodes_1 = sc.textFile(args(1)).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(1).split("-")(0)
      , 1)).reduceByKey((x, y) => x + y)
    val final_node = nodes_1.sortByKey()
    final_node.saveAsTextFile(args(2))

    ///Second Part
    val nodes = sc.textFile(args(1)).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(0)
      , line.split("\t")(1).split("-")(0)))
    val edges_1_direct = sc.textFile(args(0)).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(0)
      , line.split("\t")(1)))

    val year = List("1992", "1993", "1994", "1995", "1996", "1997","1998", "1999","2000", "2001","2002") //List of years for calculating g(d)
    var till_year = new ListBuffer[String]() // to keep track of year
    var count_list = new ListBuffer[Long]()
    for (a <- 0 to 10) {
      //      val till_year = List("1992", "1993", "1994", "1995", "1996", "1997")
      till_year += year(a)
      var nodes_each_year = nodes.filter { case (key, value) => (till_year contains value) } //Filter nodes for a particulat year
      var new_edge = edges_1_direct.join(nodes_each_year)
      var edges_1_prev = new_edge.map {
        case (key, (value1, value2)) => (key, value1.toString)
      }
      var edges_1 = edges_1_prev.filter { case (key, value) => key != value }
      var edges_1_pair_prev = edges_1.flatMap {
        case (node1, node2) =>
          var edges = new ListBuffer[String]()
          var start_node = ""
          var end_node = ""
          if (node1.toInt > node2.toInt) {
            start_node = node2
            end_node = node1
          }
          if (node1.toInt < node2.toInt) {
            start_node = node1
            end_node = node2
          }
          edges += (start_node + "~" + end_node + ":" + start_node + "\t" + end_node)
          edges += (end_node + "~" + start_node + ":" + end_node + "\t" + start_node)
          edges.toList
      }.map(x => (x.split(":")(0), x.split(":")(1)))

      var edges_1_pair = edges_1_pair_prev.reduceByKey((x, y) => x,30)
      count_list += edges_1_pair.count()/2
    }
    println(count_list)
  }
}


