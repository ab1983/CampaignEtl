package com.campaignetl

import org.apache.spark.sql.SparkSession

/**
 * Startup object is responsible for start Spark session and call the job. The environment variable SPARK_MASTER could be used
 * assign a host to the master node.
 */
object Startup extends App{
  val spark = SparkSession.builder.master(sys.env.getOrElse("SPARK_MASTER","local[*]"))//.config("spark.sql.shuffle.partitions", "16")
    .appName("Campaign Performance ETL").getOrCreate()
  new CampaignEtlJob(spark).run()
}
