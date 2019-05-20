package pub.sha0w.ETL.obj

class Name(val date : String, val articleDoi : String, val journalID : Int) extends Serializable {
  val year : String = date.split("-").head
  val doi : String = articleDoi.split("-")(2)
  var availableDOIIndex : Int = doi.length

  def getAvailableName : List[String] = {
    var li: List[String] = List[String]()
    var index = 0
    while (index < availableDOIIndex) {
      val id: String = journalID  + "_" + year + "_" + doi.substring(index, availableDOIIndex) + "_ReferencePDF.pdf"
      li = li :+ id
      index += 1
    }
    li
  }

}
object Name {
  def main(args: Array[String]): Unit = {
    val n = new Name("2019-09-01", "dd-dd-dd000-dd" , 1303)
    for (name <- n.getAvailableName) println(name)
  }
}
