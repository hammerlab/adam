organization := "org.hammerlab.adam"

name := sparkName("core")

version := "0.21.2"

addSparkDeps
publishTestJar
enableScalariform

hadoopVersion := "2.7.3"

testDeps += "org.mockito" % "mockito-core" % "1.10.19"

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

compileAndTestDeps += (libs.value('reference) exclude("com.github.samtools", "htsjdk"))
