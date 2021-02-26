import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}
import shapeless.syntax.std.tuple.productTupleOps
import scala.util.control._

object PA21 {

  def main(args: Array[String]): Unit = {
    val loop = new Breaks;
    //val sc = SparkSession.builder().master("local").getOrCreate().sparkContext
    val sc = SparkSession.builder().master("spark://hartford:31215").getOrCreate().sparkContext
    //val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext
    println("Started Successfullyy")
    //Load Citations and concatenate with the flipped data
    val edges_1_direct = sc.textFile(args(0)).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(0)
      , line.split("\t")(1)))
    //  val edges_1_flip = sc.textFile(args(0)).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(1)
    //    , line.split("\t")(0)))
    //    val edges_1_merged = edges_1_direct.union(edges_1_flip)

    //Load nodes - with value as year published
    val nodes = sc.textFile(args(1)).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(0)
      , line.split("\t")(1).split("-")(0)))

    val year = List("1992", "1993", "1994", "1995", "1996", "1997") //List of years for calculating g(d)
    val g20 = List(727, 869493, 6685253, 22234693, 50406009, 76823404) //g(20) values from year 1992 - 1997
    var till_year = new ListBuffer[String]() // to keep track of year
    var final_lis = new ListBuffer[ListBuffer[String]]()
    till_year += "1992"
//    till_year += "1993"
//    till_year += "1994"
//    till_year += "1995"
//    till_year += "1996"
    //    till_year += "1995"
    ///For year from 1992 - 1997
    for (a <- 0 to 0) {
      //      val till_year = List("1992", "1993", "1994", "1995", "1996", "1997")
      //till_year +=year(a)
      var nodes_each_year = nodes.filter { case (key, value) => (till_year contains value) } //Filter nodes for a particulat year
      var new_edge = edges_1_direct.join(nodes_each_year, 30)
      var edges_1_prev = new_edge.map {
        case (key, (value1, value2)) => (key, value1.toString)
      }
      var edges_1 = edges_1_prev.filter { case (key, value) => key != value }
      var new_edge_flip_prev = new_edge.map {
        case (key, (value1, value2)) => (value1.toString, key)
      }
      var new_edge_flip = new_edge_flip_prev.filter { case (key, value) => key != value }

      var edges_merged = edges_1.union(new_edge_flip)




      //For each edge a-b => (a~b, a b) and (b~a, b a)
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

      //      //For each edge a-b => (a, a b) and (b, b a)
      var edges_1_list = edges_1.flatMap {
        case (node1, node2) =>
          var edges = new ListBuffer[String]()
          edges += (node1 + ":" + node1 + "\t" + node2)
          edges += (node2 + ":" + node2 + "\t" + node1)
          edges.toList
      }.map(x => (x.split(":")(0), x.split(":")(1)))


      //Get list of neighbors for each node
      var neighbors_edge_1 = edges_merged.groupByKey(30)

      var neighbors = neighbors_edge_1.map(x => (x._1, x._2.toList))


      //      // Created 2 Edge Path Eg: (1~2,1	4	  2) Sorted
      val edges_2_initial = neighbors.flatMap {
        case (node, nodeList) =>
          var edges = new ListBuffer[String]()
          if (nodeList.size > 1) {
            for (i <- 0 to (nodeList.size - 2)) {
              for (j <- i + 1 to (nodeList.size - 1)) {
                val start_node = nodeList(i)
                val end_node = nodeList(j)
                var start_end = ""
                if (start_node.toInt > end_node.toInt) {
                  start_end = end_node + "~" + start_node
                  edges += (start_end + ":" + end_node + "\t" + node + "\t" + start_node)
                }
                else {
                  start_end = start_node + "~" + end_node
                  edges += (start_end + ":" + start_node + "\t" + node + "\t" + end_node)
                }
              }
            }
          }
          edges.toList
      }.map(x => (x.split(":")(0), x.split(":")(1)))


      //Subtract the path in 2nd - 1st (with 2 edge - with 1 edge) TO remove not shortest path
      val shortest_edges_2 = edges_2_initial.subtractByKey(edges_1_pair, 30)


      //Drop Duplicate index with similar (start ~ end) and outputs (2 ~ 3, 2,1,3) sorted(sorting done before)
      //      val duplicate_removed_edge_2 = shortest_edges_2.zipWithIndex.map {
      //        case ((key, value), index) => (key, (value, index))
      //      }.reduceByKey((v1, v2) => if (v1._2 < v2._2) v1 else v2).mapValues(_._1)
      val duplicate_removed_edge_2 = shortest_edges_2.reduceByKey((x, y) => x,30)


      //      //For each edge (a~b,a,c,b) => (b, a c b) and (a, b c a)
      val edge_2_list = duplicate_removed_edge_2.flatMap {
        case (node, nodeList) =>
          var edges = new ListBuffer[String]()
          val node_1 = node.split("~")(1)
          edges += (node_1 + ":" + nodeList)
          val node_2 = node.split("~")(0)
          var temp_reverse = ((nodeList.split("\t")).toList).reverse
          var reversed = temp_reverse.mkString("\t")
          edges += (node_2 + ":" + reversed)
          edges.toList
      }.map(x => (x.split(":")(0), x.split(":")(1)))


      //merge the shortest paths from 1 and 2
      var shortest_path_full = edges_1_pair.union(duplicate_removed_edge_2)

      // List to keep tract of counts
      var count_list = new ListBuffer[Long]()
      count_list += edges_1_pair.count()/2
      count_list += duplicate_removed_edge_2.count()

      println(count_list)
      //Iterate from 3 to d number of path

      var prev_edge = edge_2_list
      //     var prev_edge_1 = edge_2_list.persist(StorageLevel.MEMORY_AND_DISK)
      var edge_n_path = prev_edge.join(edges_1_list, 30)
      var shortest_path_full_persist = shortest_path_full.persist(StorageLevel.DISK_ONLY)
      loop.breakable {
        for (w <- 3 to 21) {
          if(w>3){
            edge_n_path = prev_edge.join(edges_1_list, 30)
          }

          var edge_n_path1 = edge_n_path.flatMap {
            case (node, (nodeList1, nodeList2)) =>
              var edges = new ListBuffer[String]()
              var temp_duplicate = (((nodeList1 + "\t").++(nodeList2)).split("\t").toList.distinct)
              if (temp_duplicate(0).toInt > temp_duplicate.last.toInt) {
                var reversed = temp_duplicate.reverse
                edges += (temp_duplicate.last + '~' + temp_duplicate(0) + ":" + reversed.mkString("\t"))}
              if (temp_duplicate(0).toInt < temp_duplicate.last.toInt) {
                edges += (temp_duplicate(0) + '~' + temp_duplicate.last + ":" + temp_duplicate.mkString("\t"))}
              edges.toList
          }.map(x => (x.split(":")(0), x.split(":")(1)))


          //Filter out only those with length greater than w ( path length value)
          var edge_n_filtered = edge_n_path1.filter { case (node, nodeList) => ((nodeList.split("\t")).toList).size > w }

          //Drop Duplicate index with similar (start ~ end)
          //          val duplicate_removed_edge_n = edge_n_filtered.zipWithIndex.map {
          //            case ((key, value), index) => (key, (value, index))
          //          }.reduceByKey((v1, v2) => if (v1._2 < v2._2) v1 else v2).mapValues(_._1)
          val duplicate_removed_edge_n = edge_n_filtered.reduceByKey((x, y) => x,30)
          //Subtract the path in 2nd - 1st (with 2 edge - with 1 edge) TO remove not shortest path
          //Output is (start~end, path)
          var shortest_edges_n = duplicate_removed_edge_n.subtractByKey(shortest_path_full, 30)

          shortest_path_full = shortest_path_full_persist.unpersist()
          shortest_path_full = shortest_path_full.union(shortest_edges_n)
          shortest_path_full_persist = shortest_path_full.persist(StorageLevel.DISK_ONLY)
          count_list += shortest_edges_n.count()

          prev_edge = shortest_edges_n.flatMap {
            case (node, nodeList) =>
              var edges = new ListBuffer[String]()
              val node_1 = node.split("~")(1)
              edges += (node_1 + ":" + nodeList)
              val node_2 = node.split("~")(0)
              var temp_reverse = ((nodeList.split("\t")).toList).reverse
              var reversed = temp_reverse.mkString("\t")
              edges += (node_2 + ":" + reversed)
              edges.toList
          }.map(x => (x.split(":")(0), x.split(":")(1)))


          // Print out different values for debugging
          println(a)
          println(w)
          println(count_list)
          println(count_list.sum)
          println(g20(a))
          if ((count_list.sum.toDouble / g20(a)) >= 0.90) {

            println("Breakkkkkkkkkk")
            loop.break;
            sc.stop()
          }
        }
      }
    }
  }
}