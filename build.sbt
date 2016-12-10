organization := "org.hammerlab.adam"

name := ParentPlugin.sparkName("adam-core")

version := "0.20.4-SNAPSHOT"

sonatypeProfileName := "org.hammerlab"

val utilsVersion = "0.2.9"

hadoopVersion := "2.7.3"

scalatestVersion := "2.2.1"

testDeps ++= Seq(
  "org.bdgenomics.utils" %% "utils-misc" % utilsVersion classifier("tests") exclude("org.apache.spark", "*"),
  "org.mockito" % "mockito-core" % "1.10.19"
)

providedDeps ++= Seq(
  libraries.value('hadoop),
  libraries.value('spark)
)

libraryDependencies ++= Seq(
  libraries.value('bdg_formats),
  libraries.value('kryo),
  libraries.value('hadoop_bam),
  "org.bdgenomics.utils" %% "utils-metrics" % utilsVersion,
  "org.bdgenomics.utils" %% "utils-misc" % utilsVersion,
  "org.bdgenomics.utils" %% "utils-io" % utilsVersion exclude("com.fasterxml.jackson.core", "*"),
  "org.bdgenomics.utils" %% "utils-cli" % utilsVersion,
  "org.bdgenomics.utils" %% "utils-intervalrdd" % utilsVersion,
  "commons-io" % "commons-io" % "2.4",
  "it.unimi.dsi" % "fastutil" % "6.6.5",
  "org.apache.avro" % "avro" % "1.8.0",
  "org.slf4j" % "slf4j-log4j12" % "1.7.21",
  "org.apache.parquet" % "parquet-avro" % "1.8.1",
  "org.apache.parquet" % "parquet-scala_2.10" % "1.8.1" exclude("org.scala-lang", "scala-library"),
  "com.github.samtools" % "htsjdk" % "2.5.0",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "com.netflix.servo" % "servo-core" % "0.10.0",
  "org.hammerlab" %% "genomic-loci" % "1.4.2" exclude("com.github.samtools", "htsjdk")
)

import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.defaultScalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
                               .setPreference(AlignParameters, true)
                               .setPreference(CompactStringConcatenation, false)
                               .setPreference(AlignSingleLineCaseStatements, true)
                               .setPreference(DoubleIndentClassDeclaration, true)
                               .setPreference(PreserveDanglingCloseParenthesis, true)
