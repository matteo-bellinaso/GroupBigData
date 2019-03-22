package properties

import java.io.{File, FileInputStream}
import java.util.Properties

import entity.ConnectionScala
import utility.PropertyEnum

class ApplicationConfig private(private val prop: Properties, private val propKey: java.util.Enumeration[_]) {


  def getProperty(key: String): String = {
    prop.getProperty(key)
  }

  def setConnectionConfiguration(): ConnectionScala = {
    /* var user: String = null
     var password: String = null
     var url: String = null
     var driver: String = null

     while (propKey.hasMoreElements) {
       val key = String.valueOf(propKey.nextElement())
       key match {
         case "user" => user = prop.getProperty(key)
         case "password" => password = prop.getProperty(key)
         case "url" => url = prop.getProperty(key)
         case "driver" => driver = prop.getProperty(key)
       }
     }
     ConnectionScala(user, password, url, driver)*/
    ConnectionScala(getProperty(PropertyEnum.user), getProperty(PropertyEnum.password), getProperty(PropertyEnum.dbUrl), getProperty(PropertyEnum.driver))
  }
}


object ApplicationConfig {
  private var _instance: ApplicationConfig = _

  def init(path: String) = {
    // "/Users/matteobellinaso/Desktop/lynx accademy/Big Data/GroupBigData/src/main/resources/databaseconnection.properties"
    val file = new File(path)
    val prop: Properties = new Properties()
    prop.load(new FileInputStream(file))
    val propKey = prop.keys()
    _instance = new ApplicationConfig(prop, propKey)
  }

  def instance() = {
    _instance
  }


}
