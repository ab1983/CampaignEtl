package com.campaignetl

import org.apache.spark.sql._
import org.slf4j.LoggerFactory

/**
 * Data Pipeline to find the highest performing campaign.
 * @param spark
 */
class CampaignEtlJob(spark: SparkSession) {
  import spark.implicits._
  val log = LoggerFactory.getLogger(classOf[CampaignEtlJob])

  def run()= {
    try{
      val bestCampaignName = executeJob()

      //print best campaign name
      println(s"Highest performing campaign: $bestCampaignName")
      log.info("Job completed successfully!")
    }
    catch {
      case ex: Throwable =>
        log.error("Error processing CampaignEtlJob", ex)
    }

  }

  def executeJob():String={

    val dataLoader = getDataLoader()
    loadAndCreateEventsTempView(dataLoader)
    loadAndCreateOrdersTempView(dataLoader)

    val attributionDf = getAttribution()

    val sortedCampaignByAttributionValueDf = getSortedCampaignByAttributionValue(attributionDf)

    val bestCampaignName = getHighestPerformingCampaign(sortedCampaignByAttributionValueDf)

    bestCampaignName
  }

  def loadAndCreateOrdersTempView(dataLoader: DataLoader):Unit={
    val ordersDf = dataLoader.loadOrdersFromFile()
    ordersDf.createOrReplaceTempView("orders")
  }

  def loadAndCreateEventsTempView(dataLoader: DataLoader):Unit = {
    val eventsDf = dataLoader.loadEventsFromFile()
    eventsDf.createOrReplaceTempView("events")
  }

  def getAttribution():DataFrame = {
    val attributionQuerySql = scala.io.Source.fromFile(sys.env.getOrElse("ATTRIBUTION_QUERY_FILE_PATH","sitecore-bx-data-engineer-codetest/attribution-query.sql")).mkString
    spark.sql(attributionQuerySql)
  }

  def getSortedCampaignByAttributionValue(attributionDf: DataFrame):Dataset[Row]={
    val sortPartition = attributionDf.repartition($"campaign_id").select($"campaign_id",$"attribution_value")
      .groupBy($"campaign_id")
      .agg(functions.max($"attribution_value").as("attribution_value"))
    sortPartition.select($"campaign_id").orderBy($"attribution_value".desc)
  }

  def getHighestPerformingCampaign(sortedCampaignByAttributionValueDf: DataFrame):String={
    sortedCampaignByAttributionValueDf.take(1).map(_.mkString).headOption.getOrElse("")
  }

  def getDataLoader()={
    new DataLoader(spark)
  }
}
