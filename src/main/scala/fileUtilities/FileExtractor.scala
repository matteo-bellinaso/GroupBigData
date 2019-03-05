package fileUtilities

import java.io._
import java.util.zip._

import scala.io.Source

class FileExtractor {

  def extract(input: String, output: String): String =  {
    var in = new GZIPInputStream(new FileInputStream(input))
    // write setup in different objects to close later properly (important for big files )
    var fos = new FileOutputStream(output)
    var w = new PrintWriter(new OutputStreamWriter(fos, "UTF-8"))
    for (line <- Source.fromInputStream(in).getLines()) {
     // println(line)
      w.write(line + "\n")
    }

    w.close()
    fos.close()
    output
  }

}
