name := "CampaignEtl"

version := "0.1"

scalaVersion := "2.12.3"
val sparkVersion = "3.0.0"
val scalaLoggingVersion = "3.9.2"
val mockitoScalaVersion = "1.16.42"
val scalaTestVersion = "3.2.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "compile"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "compile"
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
libraryDependencies += "org.mockito" %% "mockito-scala" % mockitoScalaVersion % Test
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion % Compile
