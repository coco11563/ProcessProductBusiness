package pub.sha0w.ETL.obj

class Name(val date : String, val articleDoi : String, val journalID : Int) extends Serializable {
  val year : String = date.split("-").head
  val doi : String = articleDoi.split("-")(2)
  var availableDOIIndex : Int = doi.length

  def getAvailableName : List[String] = {
    var li: List[String] = List[String]()
    var index = availableDOIIndex
    while (index > 0) {
      val id: String = journalID  + "_" + year + "_" + doi.substring(0, index)
      li = li :+ id
    }
    li
  }
}
