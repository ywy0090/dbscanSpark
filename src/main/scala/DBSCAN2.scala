
import scala.collection.Map
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import breeze.linalg.{Vector, DenseVector, squaredDistance}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy._


object DBSCAN {
  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ')).map(_.toDouble)
    }
  
  def pointHash(position:String): VertexId ={
    position.replace(" ", "").hashCode.toLong
  }
  def checkDegree(x:Long, pointCount:Map[VertexId,Long], minPts:Integer ): Boolean ={
    if(pointCount.contains(x))
    {
      if(pointCount(x) >= minPts)
        return true
    }
    return false
  }
  def main(args:Array[String])
  {
    //dataset path
    val datasetFile = ""
    //debugfile
    //
    val eps = 5.0
    //
    val minPnts = 5
    val conf = new SparkConf().setAppName("DBSCAN")
    //  .setSparkHome(System.getenv("SPARK_HOME"))
    //  .setJars(SparkContext.jarOfClass(this.getClass).toList)
      
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
    //pointsWithinEps.saveAsTextFile(debugFile2)
    //generate edge matrix
    val testPoints = pointsWithinEps
    .map{case((x,y),distance)=>(pointHash(x.toString),pointHash(y.toString))}
    
    val pointsCount = testPoints.countByKey()
    //
    val filterPoints = testPoints.filter{case(x,y)=>checkDegree(x,pointsCount,minPnts)}
    //filterPoints.saveAsTextFile(debugFile2)
    val pointPairs = filterPoints.distinct()
    pointPairs.saveAsTextFile(debugFile)
    
    
    val vertices = points.map(point => (pointHash(point.toString), point.toString)).cache  
    
    val filterVertices = vertices.filter{case(x,y)=>checkDegree(x,pointsCount, minPnts)}
    filterVertices.saveAsTextFile(debugFile2)
    //construct edges
    val edges: RDD[Edge[Double]] = pointPairs
    .map{case(x,y) => Edge(srcId=x,dstId=y)}
   //debugFile
    //edges.saveAsTextFile(debugFile2)
    //construct graph
    //val graph: Graph[Any, Double] = Graph.fromEdges(edges, Double)
    val graph = Graph(filterVertices, edges, "").cache
    graph.partitionBy(PartitionStrategy.RandomVertexCut, 4)
    //println("num edges = " + graph.numEdges)
    //println("num vertices = " + graph.numVertices)
    val cc = graph.connectedComponents().vertices
    //cc.saveAsTextFile(debugFile)
    val ccByPointname = vertices.join(cc).map {
    case (id, (point, cc)) => (point, cc)
    }
    // Print the result
    println(ccByPointname.collect().mkString("\n"))
    //ccByPointname.saveAsTextFile(debugFile)
  }
}
