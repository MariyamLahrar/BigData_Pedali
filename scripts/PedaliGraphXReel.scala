
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PedaliGraphXReel {

  def run(spark: SparkSession): Unit = {

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    println("\n===== PARTIE 6 : GRAPHX PEDALI =====")

    // 1. Charger les donnees Parquet depuis MinIO
    val stations = spark.read.parquet(
      "s3a://lake/silver/graph/vertices/"
    )

    val trajets = spark.read.parquet(
      "s3a://lake/silver/graph/edges/"
    )

    println("Stations : " + stations.count())
    println("Aretes : " + trajets.count())

    // 2. Generer des identifiants numeriques uniques
    val stationIds = stations
      .select("id", "city", "capacity")
      .distinct()
      .withColumn(
        "vertex_id",
        monotonically_increasing_id()
      )
      .cache()

    // 3. Construire les sommets GraphX
    val vertices: RDD[(VertexId, String)] =
      stationIds.rdd.map { row =>
        (
          row.getAs[Long]("vertex_id"),
          row.getAs[String]("id")
        )
      }

    // 4. Associer les aretes aux identifiants GraphX
    val sources = stationIds.select(
      col("id").alias("src"),
      col("vertex_id").alias("src_id")
    )

    val destinations = stationIds.select(
      col("id").alias("dst"),
      col("vertex_id").alias("dst_id")
    )

    val edgesDF = trajets
      .join(sources, Seq("src"))
      .join(destinations, Seq("dst"))

    val edges: RDD[Edge[Int]] =
      edgesDF.rdd.map { row =>
        Edge(
          row.getAs[Long]("src_id"),
          row.getAs[Long]("dst_id"),
          row.getAs[Long]("trip_count").toInt
        )
      }

    // 5. Construire le graphe
    val graph = Graph(vertices, edges).cache()

    println("\n===== GRAPHE GRAPHX =====")
    println("Sommets : " + graph.vertices.count())
    println("Aretes : " + graph.edges.count())

    // 6. Calculer les degres
    println("\n===== TOP 10 DEGRES =====")

    graph.degrees
      .takeOrdered(10)(
        Ordering.by[(VertexId, Int), Int](-_._2)
      )
      .foreach(println)

    // 7. Calculer PageRank
    println("\n===== TOP 10 PAGERANK =====")

    graph.pageRank(0.001)
      .vertices
      .takeOrdered(10)(
        Ordering.by[(VertexId, Double), Double](-_._2)
      )
      .foreach(println)

    // 8. Composantes connexes
    println("\n===== COMPOSANTES CONNEXES =====")

    val components = graph.connectedComponents()

    println(
      "Nombre de composantes : " +
      components.vertices.map(_._2).distinct().count()
    )


    // ==========================================
    // SHORTEST PATHS - GRAPHX
    // ==========================================

    println("\n===== SHORTEST PATHS =====")

    // Selectionner deux stations de reference
    // parmi les sommets existants
    val references = graph.vertices
      .takeOrdered(2)(
        Ordering.by[(VertexId, String), VertexId](_._1)
      )
      .map(_._1)
      .toSeq

    println("Stations de reference : " +
      references.mkString(", "))

    // Calculer les distances vers ces stations
    val shortestPaths = org.apache.spark.graphx.lib.ShortestPaths
      .run(graph, references)

    println("\n===== DISTANCES VERS LES REFERENCES =====")

    shortestPaths.vertices
      .take(20)
      .foreach {
        case (stationId, distances) =>
          println(
            "Station " + stationId +
            " : " + distances
          )
      }


    println("\nGRAPHX TERMINE AVEC SUCCES")

    stationIds.unpersist()
  }
}
