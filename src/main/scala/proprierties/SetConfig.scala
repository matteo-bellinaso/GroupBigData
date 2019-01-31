package proprierties

import java.io.{File, FileInputStream}
import java.util.Properties

import entity.ConnectionScala
import org.apache.spark.SparkConf

object SetConfig {

  def setSparkConfiguration() = {
    val path: File = new File("/Users/matteobellinaso/Desktop/lynx accademy/Big Data/GroupBigData/src/main/resources/application.properties")
    val prop: Properties = new Properties()
    prop.load(new FileInputStream(path))
    val propkey = prop.keys()
    val sparkConf = new SparkConf()

    while (propkey.hasMoreElements) {
      val key = String.valueOf(propkey.nextElement())
      sparkConf.set(key, prop.getProperty(key))
      println(key);
    }
  }


  def setConnectionConfiguration(): ConnectionScala = {
    val path: File = new File("/Users/matteobellinaso/Desktop/lynx accademy/Big Data/GroupBigData/src/main/resources/databaseconnection.properties")
    val prop: Properties = new Properties()
    prop.load(new FileInputStream(path))
    val propkey = prop.keys()

    var user: String = null
    var password: String = null
    var url: String = null
    var driver: String = null

    while (propkey.hasMoreElements) {
      val key = String.valueOf(propkey.nextElement())
      key match {
        case "user" => user = prop.getProperty(key)
        case "password" => password = prop.getProperty(key)
        case "url" => url = prop.getProperty(key)
        case "driver" => driver = prop.getProperty(key)
      }
    }
    new ConnectionScala(user,password, url, driver)
  }

}
