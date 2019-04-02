package converter

import java.io.File

import org.apache.spark.sql.DataFrame

class SaveCsvDataFrame(path: String) {

  def save(dataFrame: DataFrame, fileName: String) = {

    val file = new File(path + fileName)

    if (file == null) {
      dataFrame.repartition(1).write.csv(path + "/" + fileName)

    }

  }

}
