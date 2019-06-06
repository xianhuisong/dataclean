package com.acc.datalake.dataclean

import com.acc.datalake.dataclean.udf.UDF
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object MainProcess {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("data-clean").enableHiveSupport()
      .getOrCreate()

    val strLen = spark.udf.register("strLen", UDF.strLen _)

    import spark.implicits._

    val userClickDF = spark.read.options(Map[String, String] {
      "delimiter" -> ","
      "header" -> "true"
    }).csv("/data/example/user/click/user_click.csv")

    userClickDF.createOrReplaceTempView("user_click")

    val userDF = spark.sql("select * from default.user_info")

    userDF.createOrReplaceTempView("user_info")

    val df = spark.sql("select a.user_id,b.name,strLen(b.name) as nameLen,a.source,a.eventTime" +
      " from user_click a join user_info b on a.user_id = b.id")


    df.show


    df.write.mode(SaveMode.Overwrite).csv("/data/example/user/click_detail")


    spark.sql("drop table if exists click_detail ")

    spark.sql("create external table click_detail(" +
      "\n    id      Long," +
      "\n    name    string," +
      "\n    nameLen  int," +
      "\n    source   string," +
      "\n    eventTime   Long)" +
      "\nrow format delimited\n" +
      "fields terminated by ','" +
      "\nlocation '/data/example/user/click_detail/'")

    spark.sql("select * from click_detail").show

    spark.stop()
  }

}
