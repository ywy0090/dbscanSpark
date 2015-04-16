

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
  def generateIndex(line:String):Array[String]={
    return line.split(',') 
  }
  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ')).map(_.toDouble)
    }
  
  def main(args:Array[String])
  {
    //dataset path
    
    val datasetFile = ""
    //debugfile path
   
    //
    val eps = 5.0
    //
    val minPnts = 5
    val conf = new SparkConf().setAppName("DBSCAN")
   
    val sc = new SparkContext(conf)
    val lines = sc.textFile(datasetFile)
    val count = lines.count() // lines is the rdd
    val partitionLinesCount = count / lines.partitions.length
    val linesWithIndex = lines.map(generateIndex).map{x=>(x.head.toLong,x.last)}
    //linesWithIndex.saveAsTextFile(debugFile)
    
    //get index and denseVector
    
    //convert to double and vector
    
    val points = linesWithIndex.map{case(x,y)=>(x,parseVector(y))}.cache()
    points.saveAsTextFile(debugFile)
    //generate distance matrix
    val distanceBetweenPoints = points.cartesian(points)
    .filter{case(x,y) => (x!=y)}
    
    val distanceFilter=distanceBetweenPoints.map{case((idx,x),(idy,y)) => ((idx,idy),squaredDistance(x,y))}
    //with the eps
    val pointsWithinEps = distanceFilter
    .filter{case ((x,y),distance) => (distance <= eps)}.cache()
    //pointsWithinEps.saveAsTextFile(debugFile2)
    //generate edge matrix
    val testPoints = pointsWithinEps
    .map{case((x,y),distance)=>(x,y)}
    
    val pointsCount = testPoints.map{case(x,y)=>(x,1)}.reduceByKey((a,b)=> a+b)
    val filterPointsCount = pointsCount.filter{case(x,y)=> (y>=minPnts)}
    //filterPointsCount.saveAsTextFile(debugFile)
    val filterPoints = testPoints.join(filterPointsCount).map{case(k,(v,w)) => (k,v)}
    //val pointsCount = testPoints.countByKey()
    //
    //val filterPoints = testPoints.filter{case(x,y)=>checkDegree(x,pointsCount,minPnts)}
    filterPoints.saveAsTextFile(debugFile2)
    //val pointPairs = filterPoints.flatMap{case(K,v)=>(K)}
   
    //pointPairs.saveAsTextFile(debugFile2)
    
    
    val vertices = points.map{case(x,y)=>(x,y.toString())}.cache  
    
    //val filterVertices = vertices.filter{case(x,y)=>checkDegree(x,pointsCount, minPnts)}
    //filterVertices.saveAsTextFile(debugFile2)
    //construct edges
    val edges: RDD[Edge[Double]] = filterPoints
    .map{case(x,y) => Edge(srcId=x,dstId=y)}
   //debugFile
    //edges.saveAsTextFile(debugFile2)
    //construct graph
    //val graph: Graph[Any, Double] = Graph.fromEdges(edges, Double)
    val graph = Graph(vertices, edges, "").cache
    //graph.partitionBy(PartitionStrategy.RandomVertexCut, 4)

    val cc = graph.connectedComponents().vertices
    //cc.saveAsTextFile(debugFile)
    val ccByPointname = vertices.join(cc).map {
    case (id, (point, cc)) => (point, cc)
    }
    // Print the result
    println(ccByPointname.collect().mkString("\n"))
    
  
  }
}
