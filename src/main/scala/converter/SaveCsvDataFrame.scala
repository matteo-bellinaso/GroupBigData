package converter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame}

class SaveCsvDataFrame(path: String, sc: SparkContext) {

  val fs = FileSystem.get(sc.hadoopConfiguration)

  def save(dataFrame: DataFrame, fileName: String) = {

    val outPath = path + "/" + fileName

    val pathFinal = new Path(outPath)

    if (!fs.exists(pathFinal)) {
      dataFrame.repartition(1).write.csv(path + "/" + fileName)
    } else {
      if (fs.delete(pathFinal, true)) {
        dataFrame.repartition(1).write.csv(path + "/" + fileName)
      } else {
        println("problema con l'eleminazione del file")
      }
    }
  }
}
