import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.{ExecutionEnvironment, tuple}
import org.apache.flink.graph.{Edge, Graph, Vertex}
import org.apache.flink.api.java.tuple.Tuple2

import collection.JavaConversions._
import scala.io.Source

object GellyExample extends App {
  // 1) Tworzymy środowisko przetwarzania danych
  val env = ExecutionEnvironment.getExecutionEnvironment

  // 2) Stacje key - id stacji, value - nazwa stacji
  val vertexFile = Source.fromFile("./station.csv")
  val vertices = vertexFile
    .getLines()
    .filter(!_.startsWith("\"station_id\""))
    .map(_.split(","))
    .map(array => new Vertex(
      array(0).slice(1, array(0).length-1),
      array(1).slice(1, array(1).length-1))
    )
    .toArray
    .toSeq
  vertexFile.close()

  // Krawedzie zawierają id roweru
  val edgeFile = Source.fromFile("./trip.csv")
  val edges = edgeFile
    .getLines()
    .filter(!_.startsWith("\"trip_id\""))
    .map(_.split(","))
    .filter(array => array(7).contains("-"))
    .map(array => new Edge(
      array(7).slice(1, array(7).length-1),
      array(8).slice(1, array(8).length-1),
      array(3).slice(1, array(3).length-1))
    )
    .toArray
    .toSeq
  edgeFile.close()

  // Tworzymy graf z węzłów i krawędzi
  val graph = Graph.fromCollection(vertices, edges, env)

  // zad.7 Znajdź i wyświetl osobę, która dokonała największej liczby przejazdów.
  // Sprawdzono w Excelu, że powinna to być osoba z rowerem SEA00281 (667 przejazdów)
  class CountTuple3ToTuple2Mapper extends MapFunction[org.apache.flink.api.java.tuple.Tuple3[String, String, String], org.apache.flink.api.java.tuple.Tuple2[String, Long]] {
    override def map(t: tuple.Tuple3[String, String, String]) =
      new org.apache.flink.api.java.tuple.Tuple2[String, Long](t.f2, 1)
  }

  val mostUsedBikeTuple = graph.getEdgesAsTuple3
    .map(new CountTuple3ToTuple2Mapper)
    .groupBy(0).reduce((w1, w2) => new Tuple2[String, Long](w1.f0, w1.f1 + w2.f1))
    .reduce((w1, w2) => if(w1.f1 < w2.f1) new Tuple2[String, Long](w2.f0, w2.f1) else new Tuple2[String, Long](w1.f0, w1.f1) )
    .collect().get(0)
  println("Zad.7 Największa liczba przejazdów: " + mostUsedBikeTuple.f0 + " (" + mostUsedBikeTuple.f1.toString + ")")
  val mostUsedBike = mostUsedBikeTuple.f0

  // zad.8
  // Graf z dobrymi krawędziami i zbędnymi wierzchołkami
  val usedGraph = graph.filterOnEdges(edge => edge.getValue == mostUsedBike)

  val unconnectedVerticesId = usedGraph.getDegrees
    .filter(_.f1.getValue == 0)
    .map(vertex => vertex.f0)
    .collect()

  // Lista zbędnych wierzchołków
  val unconnectedVertices = vertices.filter(x => unconnectedVerticesId.contains(x.getId))

  // Graf bez zbędnych wierzchołków
  val mostUsedGraph = usedGraph.removeVertices(unconnectedVertices)
  //         6) Uzyskajmy z grafu kogoś kto zna co najmniej dwie osoby
  //                val someone = mostUsedGraph.outDegrees.filter(_.f1.getValue >= 2).collect().get(0)
  //                println(someone.f0)

  val vertexMaxOutDeg = mostUsedGraph.outDegrees().maxBy(1).collect() // Najczęściej używana stacja jako startowa
  val vertexMaxInDeg = mostUsedGraph.inDegrees().maxBy(1).collect() // Najczęściej używana stacja jako końcowa

  println("Zad.9")
  println("Najczęściej używana stacje jako startowe (nazwa, licznik): " + vertexMaxOutDeg)

  println("Najczęściej używana stacje jako końcowe (nazwa, licznik): " + vertexMaxInDeg)
}
