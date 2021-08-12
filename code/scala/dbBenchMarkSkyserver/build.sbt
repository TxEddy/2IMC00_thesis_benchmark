name := "dbBenchMarkSkyserver"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "com.github.tototoshi" %% "scala-csv" % "1.5.2"
)
