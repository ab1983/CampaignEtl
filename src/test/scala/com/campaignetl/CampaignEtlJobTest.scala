package com.campaignetl

import org.apache.spark.sql.SparkSession
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpecLike

class CampaignEtlJobTest extends AnyFlatSpecLike with MockitoSugar{

  val spark = SparkSession.builder.master("local[*]")
    .config("spark.sql.shuffle.partitions", "100")
    .appName("Campaign Performance ETL Test").getOrCreate()

  behavior of "Campaign ETL Job"

  it should "execute Job to return the Highest Performing Campaign" in {
    val etlJob = spy(new CampaignEtlJob(spark))
    val dataLoader = spy(new DataLoader(spark))
    val orders = spark.read.json(getClass.getClassLoader.getResource("etl-data/10k-orders.json").getPath)
    val events = spark.read.json(getClass.getClassLoader.getResource("etl-data/200k-events.json").getPath)
    when(etlJob.getDataLoader()).thenReturn(dataLoader)
    when(dataLoader.loadOrdersFromFile()).thenReturn(orders)
    when(dataLoader.loadEventsFromFile()).thenReturn(events)
    val result = etlJob.executeJob()
    assert(result == "camp4")
  }

}
