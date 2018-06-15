group("org.hammerlab.adam")
name := "core"
v"0.23.2"
github.repo("adam")

spark
publishTestJar

dep(
  bdg.formats,
  hammerlab("bdg-utils", "cli") % "0.3.0",
  //bdg.utils.cli,
  bdg.utils.intervalrdd,
  bdg.utils.io,
  bdg.utils.metrics,
  bdg.utils.misc +testtest,
  commons.io,
  seqdoop_hadoop_bam ^ "7.9.0",
  genomics.loci ^ "2.1.0",
  genomics.reference % "1.4.3" - htsjdk +testtest,
  htsjdk,
  log4j,
  parquet_avro,
  paths ^ "1.5.0",
  spark_util ^ "1.2.1",
  "it.unimi.dsi" ^ "fastutil" ^ "6.6.5",
  "org.apache.avro" ^ "avro" ^ "1.8.1",
  "org.apache.httpcomponents" ^ "httpclient" ^ "4.5.2",
  ("org.apache.parquet" ^ "parquet-scala_2.10" ^ "1.8.1") - scala_lang
)

testDeps += "org.mockito" ^ "mockito-core" ^ "2.6.4"
