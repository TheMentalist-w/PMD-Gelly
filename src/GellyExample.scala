import java.util

import org.apache.flink.api.java.{ExecutionEnvironment}
import org.apache.flink.graph.{Edge, Graph, Vertex}
import collection.JavaConversions._

import scala.io.Source

object GellyExample extends App {
        // 1) Tworzymy środowisko przetwarzania danych
        val env = ExecutionEnvironment.getExecutionEnvironment

        // 2) Stacje key - id stacji, value - nazwa stacji
        val vertexFile = Source.fromFile("./station.csv")
        val vertices= vertexFile
          .getLines()
          .filter(!_.startsWith("\"station_id\""))
          .map(_.split(","))
          .map(array => new Vertex(array(0), array(1)))
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
          .map(array => new Edge(array(7), array(8), array(3)))
          .toArray
          .toSeq
        edgeFile.close()

        // 3) Tworzymy graf z węzłów i krawędzi
        val graph = Graph.fromCollection(vertices, edges, env)

        // 6) Uzyskajmy z grafu kogoś kto zna co najmniej dwie osoby
        val someone = graph.outDegrees.filter(_.f1.getValue >= 2).collect().get(0)
        println(someone.f0)
}