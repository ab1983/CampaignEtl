package com.campaignetl

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class responsible for ingesting the data. use the environment variables ORDERS_FILE_PATH and EVENTS_FILE_PATH
 * to change the file paths.
 *
 * @param spark
 */
class DataLoader(spark: SparkSession) {
  def loadOrdersFromFile():DataFrame={
    spark.read.json(sys.env.getOrElse("ORDERS_FILE_PATH","sitecore-bx-data-engineer-codetest/etl-data/10k-orders.json"))
  }

  def loadEventsFromFile():DataFrame = {
    spark.read.json(sys.env.getOrElse("EVENTS_FILE_PATH","sitecore-bx-data-engineer-codetest/etl-data/200k-events.json"))
  }
}
