
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import breeze.linalg.{Vector, DenseVector, squaredDistance}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object DBSCAN {
  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ')).map(_.toDouble)
    }
  
  def pointHash(position:String): VertexId ={
    position.replace(" ", "").hashCode.toLong
  }
  def main(args:Array[String])
  {
    //dataset path
    val datasetFile = "/home/ywy/TestSpark/kmeans_data.txt"
    //debugfile path
    val debugFile = "/home/ywy/TestSpark/debugFile"
    val debugFile2 = "/home/ywy/TestSpark/debugFile2"
    //
    val eps = 2.0
    //
    val minPnts = 5
    val conf = new SparkConf()
      .setAppName("DBSCAN")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)
    val sc = new SparkContext(conf)
  
    val lines = sc.textFile(datasetFile)
    //convert to double and vector
    val points = lines.map(parseVector).cache()
    //generate distance matrix
    val distanceBetweenPoints = points.cartesian(points)
    .filter{case (x,y) => (x!=y)}
    .map{case (x,y) => ((x,y),squaredDistance(x,y))}
    //with the eps
    val pointsWithinEps = distanceBetweenPoints
    .filter{case ((x,y),distance) => (distance <= eps)} 
    //generate edge matrix
    val testPoints = pointsWithinEps
    .map{case((x,y),distance)=>(pointHash(x.toString),pointHash(y.toString),1.0)}
    
    val vertices = points.map(point => (pointHash(point.toString), point.toString)).cache
    //vertices.saveAsTextFile(debugFile)
    //construct edges
    val edges: RDD[Edge[Double]] = testPoints
    .map{case(x,y,1.0) => Edge(srcId=x,dstId=y,1.0)}
   //debugFile
    edges.saveAsTextFile(debugFile2)
    //construct graph
    //val graph: Graph[Any, Double] = Graph.fromEdges(edges, Double)
    val graph = Graph(vertices, edges, "").cache
    println("num edges = " + graph.numEdges)
    println("num vertices = " + graph.numVertices)
    val cc = graph.connectedComponents().vertices
    cc.saveAsTextFile(debugFile)
    val ccByPointname = vertices.join(cc).map {
    case (id, (point, cc)) => (point, cc)
    }
    // Print the result
    println(ccByPointname.collect().mkString("\n"))
  }
}