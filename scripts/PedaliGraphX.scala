
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object PedaliGraphX {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Pedali-GraphX")
      .setMaster("spark://spark-master:7077")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("\n===== GRAPHE PEDALI.MA =====")

    // 1. Les stations sont les sommets
    val vertices: RDD[(VertexId, String)] =
      sc.parallelize(Seq(
        (1L, "Agdal"),
        (2L, "Hassan"),
        (3L, "Hay Riad"),
        (4L, "Souissi"),
        (5L, "Ocean")
      ))

    // 2. Les connexions entre stations
    val edges: RDD[Edge[Int]] =
      sc.parallelize(Seq(
        Edge(1L, 2L, 1),
        Edge(1L, 4L, 1),
        Edge(2L, 3L, 1),
        Edge(2L, 5L, 1),
        Edge(3L, 4L, 1),
        Edge(3L, 5L, 1),
        Edge(4L, 5L, 1)
      ))

    // 3. Construire le graphe
    val graph = Graph(vertices, edges)

    println("\n===== NOMBRE DE STATIONS =====")
    println("Stations : " + graph.vertices.count())

    println("\n===== NOMBRE DE CONNEXIONS =====")
    println("Connexions : " + graph.edges.count())

    // 4. Calculer les degres
    println("\n===== DEGRES DES STATIONS =====")

    val degrees = graph.degrees.collect()

    degrees.foreach {
      case (id, degree) =>
        println("Station " + id + " : " + degree)
    }

    // 5. PageRank
    println("\n===== PAGERANK =====")

    val ranks = graph.pageRank(0.0001).vertices.collect()

    ranks.foreach {
      case (id, rank) =>
        println("Station " + id + " : " + rank)
    }

    println("\n===== GRAPHX TERMINE =====")

    sc.stop()
  }
}
