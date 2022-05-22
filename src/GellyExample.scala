import org.apache.flink.api.java.{ExecutionEnvironment}
import org.apache.flink.graph.{Edge, Graph, Vertex}
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

        //TODO znaleźć rower z największą liczbą przejazdów
        val mostUsedBike = "SEA00151"

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

        val vertexMaxOutDeg = mostUsedGraph.outDegrees().maxBy(1) // Najczęściej używana stacja jako startowa
        val vertexMaxInDeg = mostUsedGraph.inDegrees().maxBy(1) // Najczęściej używana stacja jako końcowa

        println("Najczęściej używana stacja jako startowa:")
        vertexMaxInDeg.print()

        println("Najczęściej używana stacja jako końcowa:")
        vertexMaxOutDeg.print()

}
