package sda.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
case class JsonReader(path: String,
                      delimiter:Option[String]=None,
                      multiline:Option[Boolean]=None

                    )
  extends Reader {
  val format = "json"

  def read()(implicit  spark: SparkSession): DataFrame = {

    spark.read.format(format)
      .option("delimiter",delimiter.getOrElse(","))
      .option("multiline", multiline.getOrElse(true))
      .load(path)

  }
}