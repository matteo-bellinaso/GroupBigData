package fileUtilities

import java.io.File
import java.net.URL
import org.apache.commons.io.FileUtils

import properties.ApplicationConfig
import utility.PropertyEnum


class FileDownloader {

  /* def download(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }*/

  def downloadWithRedirect(urlString: String): String = {

    val url = new URL(urlString)
    val redirectedUrl = url.openConnection().getHeaderField("Location")
    val filename = getFilenameFromURL(redirectedUrl)
    val connection = new URL(redirectedUrl).openConnection()
    connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11")
    connection.connect()
    val stream = connection.getInputStream
    FileUtils.copyInputStreamToFile(stream, new File(ApplicationConfig.instance().getProperty(PropertyEnum.downloadLocation) + filename))
    filename
  }

  def getFilenameFromURL(urlString: String): String = {
    val splittedUrl = urlString.split("/")
    val size = splittedUrl.length - 1
    splittedUrl(size)
  }
}
