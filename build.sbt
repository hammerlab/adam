organization := "org.hammerlab.adam"

name := sparkName("core")

version := "0.20.4-SNAPSHOT"

addSparkDeps
publishTestJar
enableScalariform

hadoopVersion := "2.7.3"
scalatestVersion := "2.2.1"

// Using ":=" here to clobber the usual default hammerlab-test-libs that are added by parent-plugin, which use
// Scalatest 3.0.0.
testDeps := Seq(
  "org.mockito" % "mockito-core" % "1.10.19",
  libs.value('scalatest)
)

testJarTestDeps += (libs.value('bdg_utils_misc) exclude("org.apache.spark", "*"))

deps ++= Seq(
  libs.value('bdg_formats),
  libs.value('bdg_utils_cli),
  libs.value('bdg_utils_intervalrdd),
  libs.value('bdg_utils_io),
  libs.value('bdg_utils_metrics),
  libs.value('bdg_utils_misc),
  libs.value('commons_io),
  libs.value('hadoop_bam) exclude("com.github.samtools", "htsjdk"),
  libs.value('loci) exclude("com.github.samtools", "htsjdk"),
  libs.value('log4j),
  "com.github.samtools" % "htsjdk" % "2.5.0",
  "com.netflix.servo" % "servo-core" % "0.10.0",
  "it.unimi.dsi" % "fastutil" % "6.6.5",
  "org.apache.avro" % "avro" % "1.8.0",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "org.apache.parquet" % "parquet-avro" % "1.8.1",
  "org.apache.parquet" % "parquet-scala_2.10" % "1.8.1" exclude("org.scala-lang", "scala-library")
)
