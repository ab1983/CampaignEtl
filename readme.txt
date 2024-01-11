* Campaign Etl

- Data Pipeline to find the highest performing campaign.

Project using Spark 3 as one of the latest versions
Startup object is responsible for start Spark session and call the job. The environment variable SPARK_MASTER could be used
assign a host to the master node.

The job was broken down in 5 steps:
    loadAndCreateEventsTempView, loadAndCreateOrdersTempView, getAttribution,
    getSortedCampaignByAttributionValue, getHighestPerformingCampaign.

getSortedCampaignByAttributionValue - Will aggregate by the campaign and max attribution_value to reduce data shuffle.
DataLoader class responsible for ingesting the data. use the environment variables ORDERS_FILE_PATH and EVENTS_FILE_PATH
to change the file paths.
Test covering the whole feature.
Additional Tests should be added to measure performance.
Scala-test and scala-logging were also added as dependencies.
