package com.wocloud.config


import com.wocloud.config.PropertiesConfig._
import org.apache.spark.sql.DataFrame


object MongodbConfig {

  //mongodb参数
  val mongodb_output_uri=mongodb_output

  //保存至mongodb
  def saveToMongodb(df: DataFrame, table: String) = {
    val MongoDbOptions: Map[String, String] = Map[String, String](
      "spark.mongodb.output.collection" -> table,
      "spark.mongodb.output.uri" -> mongodb_output_uri,
      "spark.mongodb.output.replaceDocument" -> "false"
    )

    df.write
        .format("com.mongodb.spark.sql.DefaultSource")
        .mode("append")
        .options(MongoDbOptions)
        .save()

  }

}
