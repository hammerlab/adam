organization := "org.hammerlab.adam"

name := sparkName("core")

version := "0.23.2-SNAPSHOT"

addSparkDeps
publishTestJar
enableScalariform

testDeps += "org.mockito" % "mockito-core" % "2.6.4"

testUtilsVersion := "1.2.4-SNAPSHOT"

testJarTestDeps += (libs.value('bdg_utils_misc) exclude("org.apache.spark", "*"))

deps ++= Seq(
  libs.value('bdg_formats),
  libs.value('bdg_utils_cli),
  libs.value('bdg_utils_intervalrdd),
  libs.value('bdg_utils_io),
  libs.value('bdg_utils_metrics),
  libs.value('bdg_utils_misc),
  libs.value('commons_io),
  "org.seqdoop" % "hadoop-bam" % "7.8.0" exclude("org.apache.hadoop", "hadoop-client"),
  libs.value('htsjdk),
  libs.value('loci).copy(revision = "2.0.0-SNAPSHOT"),
  libs.value('log4j),
  libs.value('parquet_avro),
  libs.value('paths).copy(revision = "1.1.1-SNAPSHOT"),
  libs.value('spark_util).copy(revision = "1.2.0-SNAPSHOT"),
  "it.unimi.dsi" % "fastutil" % "6.6.5",
  "org.apache.avro" % "avro" % "1.8.1",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "org.apache.parquet" % "parquet-scala_2.10" % "1.8.1" exclude("org.scala-lang", "scala-library")
)

compileAndTestDeps += (libs.value('reference) exclude("com.github.samtools", "htsjdk"))
