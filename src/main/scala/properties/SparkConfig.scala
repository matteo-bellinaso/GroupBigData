package properties

import java.io.{File, FileInputStream}
import java.util.Properties

import entity.ConnectionScala
import org.apache.spark.SparkConf

class SparkConfig private(private val prop:Properties, private val propKey: java.util.Enumeration[_]) {

  def setSparkConfiguration(): SparkConf = {
    val sparkConf = new SparkConf()

    while (propKey.hasMoreElements) {
      val key = String.valueOf(propKey.nextElement())
      sparkConf.set(key, prop.getProperty(key))
      //println(key)
    }
    sparkConf
  }
}


object SparkConfig {
  private var _instance: SparkConfig = _

  def init(path: String) = {
    // "/Users/matteobellinaso/Desktop/lynx accademy/Big Data/GroupBigData/src/main/resources/databaseconnection.properties"
    val file = new File(path)
    val prop: Properties = new Properties()
    prop.load(new FileInputStream(file))
    val propKey = prop.keys()
    _instance = new SparkConfig(prop,propKey)
  }

  def instance() = {
    _instance
  }

}
