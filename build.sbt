organization := "org.hammerlab.adam"

name := sparkName("adam-core")

version := "0.20.4-SNAPSHOT"

hadoopVersion := "2.7.3"

scalatestVersion := "2.2.1"

addSparkDeps
publishTestJar
enableScalariform

// Using ":=" here to clobber the usual default hammerlab-test-libs that are added by parent-plugin, which use
// Scalatest 3.0.0.
testDeps := Seq(
  "org.mockito" % "mockito-core" % "1.10.19"
)

testJarTestDeps += (libraries.value('bdg_utils_misc) exclude("org.apache.spark", "*"))

libraryDependencies ++= Seq(
  libraries.value('bdg_formats),
  libraries.value('bdg_utils_cli),
  libraries.value('bdg_utils_intervalrdd),
  libraries.value('bdg_utils_io),
  libraries.value('bdg_utils_metrics),
  libraries.value('bdg_utils_misc),
  libraries.value('commons_io),
  libraries.value('hadoop_bam) exclude("com.github.samtools", "htsjdk"),
  libraries.value('log4j),
  "com.github.samtools" % "htsjdk" % "2.5.0",
  "com.netflix.servo" % "servo-core" % "0.10.0",
  "it.unimi.dsi" % "fastutil" % "6.6.5",
  "org.apache.avro" % "avro" % "1.8.0",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "org.apache.parquet" % "parquet-avro" % "1.8.1",
  "org.apache.parquet" % "parquet-scala_2.10" % "1.8.1" exclude("org.scala-lang", "scala-library"),
  "org.hammerlab" %% "genomic-loci" % "1.4.4" exclude("com.github.samtools", "htsjdk")
)
