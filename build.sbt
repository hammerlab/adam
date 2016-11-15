organization := "org.hammerlab.adam"

name := ParentPlugin.sparkName("adam-core")

version := "0.21.0-SNAPSHOT"

val utilsVersion = "0.2.9"

scalatestVersion := "2.2.1"

libraryDependencies <++= libraries { v => Seq(
  "org.bdgenomics.utils" %% "utils-metrics" % utilsVersion,
  "org.bdgenomics.utils" %% "utils-misc" % utilsVersion,
  "org.bdgenomics.utils" %% "utils-io" % utilsVersion exclude("com.fasterxml.jackson.core", "*"),
  "org.bdgenomics.utils" %% "utils-cli" % utilsVersion,
  "org.bdgenomics.utils" %% "utils-intervalrdd" % utilsVersion,
  "com.esotericsoftware.kryo" % "kryo" % "2.24.0",
  "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.10.0",
  "commons-io" % "commons-io" % "2.4",
  "org.scala-lang" % "scala-library" % "2.10.6",
  "org.apache.hadoop" % "hadoop-client" % "2.7.3" exclude("asm", "asm") exclude("org.jboss.netty", "netty") exclude("org.codehaus.jackson", "*") exclude("org.sonatype.sisu.inject", "*") exclude("javax.servlet", "servlet-api") exclude("com.google.guava", "guava"),
  v('spark) exclude("org.apache.hadoop", "hadoop-client") exclude("org.apache.hadoop", "hadoop-mapreduce"),
  "it.unimi.dsi" % "fastutil" % "6.6.5",
  "org.apache.avro" % "avro" % "1.8.0",
  "org.slf4j" % "slf4j-log4j12" % "1.7.21",
  "org.apache.parquet" % "parquet-avro" % "1.8.1",
  "org.apache.parquet" % "parquet-scala_2.10" % "1.8.1" exclude("org.scala-lang", "scala-library"),
  "org.seqdoop" % "hadoop-bam" % "7.7.0",
  "com.github.samtools" % "htsjdk" % "2.5.0",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "com.netflix.servo" % "servo-core" % "0.10.0" exclude("com.google.guava", "guava"),
  "com.google.guava" % "guava" % "16.0.1",

  // Test deps
  (("org.bdgenomics.utils" %% "utils-misc" % utilsVersion exclude("org.apache.spark", "*")) % Test).classifier("tests"),
  "org.mockito" % "mockito-core" % "1.10.19" % "test"
)}

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
