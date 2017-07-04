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
