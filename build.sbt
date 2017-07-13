organization := "org.hammerlab.adam"

name := "core"

version := "0.23.2-SNAPSHOT"

addSparkDeps
publishTestJar
enableScalariform

testDeps += ("org.mockito" ^ "mockito-core") ^ "2.6.4"

testUtilsVersion := "1.2.4-SNAPSHOT"
sparkTestsVersion := "2.1.0-SNAPSHOT"

deps ++= Seq(
  bdg_formats,
  bdg_utils_cli ^ "0.2.15",
  bdg_utils_intervalrdd,
  bdg_utils_io,
  bdg_utils_metrics,
  bdg_utils_misc,
  commons_io,
  hadoop_bam ^ "7.8.1-SNAPSHOT",
  htsjdk,
  loci ^ "2.0.0-SNAPSHOT",
  log4j,
  parquet_avro,
  paths ^ "1.1.1-SNAPSHOT",
  spark_util ^ "1.2.0-SNAPSHOT",
  "it.unimi.dsi" ^ "fastutil" ^ "6.6.5",
  "org.apache.avro" ^ "avro" ^ "1.8.1",
  "org.apache.httpcomponents" ^ "httpclient" ^ "4.5.2",
  ("org.apache.parquet" ^ "parquet-scala_2.10" ^ "1.8.1") - scala_lang
)

compileAndTestDeps += (reference % "1.3.1-SNAPSHOT" - htsjdk)

testTestDeps += (bdg_utils_misc - spark)
