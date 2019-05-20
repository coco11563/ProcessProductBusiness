package pub.sha0w.ETL

import java.io.{BufferedReader, File, FileReader}
import java.util
import java.util.List

import org.apache.avro.Schema
import pub.sha0w.ETL.obj.Name
import pub.sha0w.ETL.utils.FTPUtils

import scala.collection.JavaConverters._

object ftpTest {
  def main(args: Array[String]): Unit = {
//     addr : String, user : String, passwd : String, port : Int
    FTPUtils.initClient("192.168.3.125", "ftp", "", 21)
    val file_name_set = FTPUtils.listFile("/springer").toMap
    val reader = new BufferedReader(
      new FileReader(new File("C:\\Users\\coco1\\IdeaProjects\\ProcessProductBusiness\\src\\main\\scala\\res\\head_100.testset")))
    var str : String = null
    val nameli: util.LinkedList[Name] = new util.LinkedList[Name]()
    while ({
      str = reader.readLine()
      str != null
    }) {
      val len = 17
      var j: Int = 0
      val date_index = 8
      val doi = 6
      val journalid: Int = 0
      var tf: Int = 0
      var t: Int = 0
      val li = new util.LinkedList[String]()
      while ( {
        j < len
      }) {
        tf = t
        t = str.indexOf("\t", t) + 1
        if (t == 0) t = str.length - 1
        var value: Any = null
        val subStr: String = str.substring(tf, t - 1)
        li.add(subStr)
        j += 1
      }
      nameli.add(new Name(li.get(date_index), li.get(doi), li.get(journalid).toInt))
    }
    val result = new util.LinkedList[String]()
    for (n <- nameli.asScala) {
      val l : scala.List[String] = n.getAvailableName
      var index = 0
      while(!file_name_set.contains(l(index)) && index < l.length - 1) {
        index += 1
      }
      if (!file_name_set.contains(l(index))) {
        result.add(l(index) + n.doi)
      } else {
        val path = file_name_set.get(l(index)) // path
        result.add(l(index) + path)
      }
    }
    println(result)
  }
}
