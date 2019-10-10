package pub.sha0w.ETL

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import pub.sha0w.ETL.obj.Name
import pub.sha0w.ETL.utils.FTPUtils

object MajorProcess {

  def updateRow(row: Row, key: String, value: String, schema : StructType): Row = {
    val seq = row.toSeq
    Row.fromSeq(seq.updated(schema.fieldIndex(key), value))
  }
  private val logger: Logger = LoggerFactory.getLogger(MajorProcess.getClass)
  def main(args: Array[String]): Unit = {
    FTPUtils.initClient(args(0), args(1), args(2), args(3).toInt)
    val file_name_set = FTPUtils.listFile(args(4))
    System.setProperty("hive.metastore.uris", args(5)) //hivemetastore = thrift://packone123:9083
    System.setProperty("org.apache.commons.net.ftp.systemType.default", args(6))
    val conf = new SparkConf()
      .setAppName("SpringerProcess")
      .set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val o_table = hiveContext.read.table("origin.o_delivery_report_article")
    val t_table = hiveContext.read.table("temp.t_product_business_fulltext")
    val o_group_name = "articletitle"
    val t_group_name = "businessext_en_title"
    // journalid_year_doi_ReferencePDF.pdf  5_2016_404_ReferencePDF.pdf
    val o_pdffound_need_field = "journalid;onlinedate;articledoi"
    //(addr : String, user : String, passwd : String, port : Int)

    val name_set_bro = sc.broadcast[Map[String,String]](file_name_set.toMap)

    val o_df: DataFrame = o_table.select("articletitle", o_pdffound_need_field.split(";") : _*)
    val combine_cell_data = o_df.map(r => {
      (r.getAs[String](o_group_name).toLowerCase, new Name(r.getAs[String]("onlinedate"), r.getAs[String]("articledoi"), r.getAs[Int]("journalid")))
    })
    val t_schema = t_table.schema

    val t_fulltext = t_table.map(r => {
      ({
        val op = Option(r.getAs[String](t_group_name))
          if (op.isDefined) op.get.toLowerCase
          else null
      }, r)
    })

    val joined_row = t_fulltext
      .leftOuterJoin(combine_cell_data)
      .persist()

    val processed_row = joined_row.map(pair => {
      val title = pair._1
      val name = pair._2._2
      var row = pair._2._1
      if (title == null) row
      else {
        if (name.isEmpty) {
          row
        } else {
          val has = row.getAs[String]("has_full_text")
          if (has == "1") {
            row = updateRow(row, "source", "overwrite", t_schema)
          } else {
            row = updateRow(row, "has_full_text", "1", t_schema)
            row = updateRow(row, "source", "springer", t_schema)
          }
          row
        }
      }
      })
    hiveContext.createDataFrame(processed_row, t_schema)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("middle.m_product_business_fulltext")

    val id_path_map = joined_row
      .filter(f => f._2._2.isDefined)
      .map(f => (f._2._1.getAs[String]("business_product_id"),
        {
          val li = f._2._2.get.getAvailableName
          var index = 0
          while(!name_set_bro.value.contains(li(index)) && index < li.length - 1) {
            index += 1
          }
          name_set_bro.value.get(li(index)) // path
        }))
      .filter(f => f._2.isDefined)
      .map(f => {
        Row.fromTuple((f._1, f._2.get))
      })

    val structType : StructType = new StructType(Array(StructField("id", StringType, nullable = false),
      StructField("path", StringType, nullable = false)))
    hiveContext.createDataFrame(id_path_map, structType)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("temp.t_pdf_id_path_map")
    val o_schema = o_df.schema
  }
}
