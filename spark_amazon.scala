// Download and unzip the Amazon product co-purchasing network, March 02 2003 dataset
import sys.process._
import java.net.URL
import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import java.io.FileInputStream

def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
}

class BufferedReaderIterator(reader: BufferedReader) extends Iterator[String] {
  override def hasNext() = reader.ready
  override def next() = reader.readLine()
}

object GzFileIterator {
  def apply(file: java.io.File, encoding: String) = {
    new BufferedReaderIterator(
      new BufferedReader(
        new InputStreamReader(
          new GZIPInputStream(
            new FileInputStream(file)), encoding)))
  }
}

def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
  val p = new java.io.PrintWriter(f)
  try { op(p) } finally { p.close() }
}

println("Downloading the zip file")
fileDownloader("https://snap.stanford.edu/data/amazon0302.txt.gz", "/resources/data/amazon0302.txt.gz")
println("Reading and unzipping")
val iterator = GzFileIterator(new java.io.File("/resources/data/amazon0302.txt.gz"), "UTF-8")
println("Writing to text file")
printToFile(new File("/resources/data/amazon0302.txt")) { p =>
  iterator.foreach(p.println)
}

// Import GraphX and all of its components
import org.apache.spark.graphx._

// Create the graph
val amazonGraph = GraphLoader.edgeListFile(sc=sc,
                                           path="/resources/data/amazon0302.txt",
                                           canonicalOrientation=true).partitionBy(PartitionStrategy.RandomVertexCut)

// Number of vertices and edges of our amazonGraph
println("Number of vertices: " + amazonGraph.numVertices)
println("Number of edges: " + amazonGraph.numEdges)

// Cache the graph for faster computations
amazonGraph.cache()

// To analyze more, we can find those products that are frequently co-purchased together and form a "cluster" in order to,
// for example, recommend "related" products when someone buys a product or even balance the supply-demand, etc. 
// There are number of ways we can define what we mean by a cluster in a graph. The simplest cluster is a triangle.
val triCounts = amazonGraph.triangleCount()

// The result of calling triCounts.vertices is an RDD of pairs of (VertexId, # triangles containing the Vertex). 
// Therefore, we can count the total number of triangles, by suming up all the each vertex triangle count with reduce. 
// Moreover, since each edge is counted three times (once for every vertex of a triangle), we need to divide by  3
// to find the exact total count. The result in fact, verifies the claim here about the number of triangles.
val totalTriCounts = triCounts.vertices.map(x => x._2).reduce(_ + _) / 3
println("Total number of triangles: " + totalTriCounts)

// We can find all the weakly connected components by calling connectedComponents() method and the result will be a 
// Graph object labeling each component by the lowest-numbered vertex.
val wcc = amazonGraph.connectedComponents()

val numVertWCC = wcc.vertices.map(x => (x._2, 1)).reduceByKey(_ + _)
val numVertLargestWCC = numVertWCC.map(_.swap).sortByKey(ascending=false).first()
println("Number of vertices in the largest WCC: " + numVertLargestWCC._1)
println("Total number of WCC: " + numVertWCC.count())

// Finally, we can use google's Page Rank algorithm to measure the importance of
// a product based on the number (and quality) of other products that were bought 
// with it. That is, when multiple products (with high quality) are frequently 
// co-purchased with a product  j,
//  then product  j
//  recieves high importance or weight.
val pr = amazonGraph.staticPageRank(numIter=5)
val productsRank = pr.vertices.map(_.swap).sortByKey(ascending=false).map(_.swap)
println("Five most important product ids and their weights:")
productsRank.take(5).foreach(println)
