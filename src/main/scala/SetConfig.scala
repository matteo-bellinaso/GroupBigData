import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.SparkConf


object SetConfig {

  def setSparkConfiguration() = {

    val path: File = new File("../resources/application.properties")
    val prop: Properties = new Properties()
    prop.load(new FileInputStream(path))

    val propkey = prop.keys()

    val sparkConf = new SparkConf()

    while (propkey.hasMoreElements) {
      val key = String.valueOf(propkey.nextElement())
      sparkConf.set(key, prop.getProperty(key))
      println(key)
    }

    // sparkConf.getAll.foreach(println)
  }
}
