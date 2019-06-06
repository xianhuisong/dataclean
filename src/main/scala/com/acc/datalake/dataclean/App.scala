package com.acc.datalake.dataclean

import com.acc.datalake.dataclean.entity.{User, UserClick}
import com.acc.datalake.dataclean.udf.UDF
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}


object App {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("data-clean").enableHiveSupport()
      .getOrCreate()

    val strLen = spark.udf.register("strLen", UDF.strLen _)

    import spark.implicits._

    //implicit val strToLong = (str: String) => str.toLong
    val userClickSchema = Encoders.product[UserClick].schema

    val userClickDS = spark.read.schema(userClickSchema).options(Map[String, String] {
      "delimiter" -> ","
      "header" -> "true"
    }).csv("/data/example/user/click/user_click.csv").as[UserClick]


    userClickDS.show()

    val userDS = spark.sql("select * from default.user_info").as[User]

    userDS.show()


    val unionDF = userClickDS.join(userDS,
      userClickDS.col("user_id") === userDS.col("id"))
      .select(userDS.col("id"),
        userDS.col("name"),
        strLen(userDS.col("name")).as("nameLen"),
        userClickDS.col("source"),
        userClickDS.col("eventTime"))

    unionDF.show


    unionDF.write.mode(SaveMode.Overwrite).csv("/data/example/user/click_detail")


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


    unionDF.groupBy(unionDF.col("id")).count().as("click_num")
      .write.mode(SaveMode.Overwrite).csv("/data/example/user/click_count")

    spark.stop()

  }

}
