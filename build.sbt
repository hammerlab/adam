group("org.hammerlab.adam")
name := "core"
r"0.23.2"
github.repo("adam")

addSparkDeps
publishTestJar

dep(
  bdg_formats,
  bdg_utils_cli ^ "0.3.0",
  bdg_utils_intervalrdd,
  bdg_utils_io,
  bdg_utils_metrics,
  bdg_utils_misc +testtest,
  commons_io,
  seqdoop_hadoop_bam ^ "7.9.0",
  htsjdk,
  loci ^ "2.0.1",
  log4j,
  parquet_avro,
  paths ^ "1.2.0",
  reference % "1.4.0" - htsjdk +testtest,
  spark_util ^ "1.2.1",
  "it.unimi.dsi" ^ "fastutil" ^ "6.6.5",
  "org.apache.avro" ^ "avro" ^ "1.8.1",
  "org.apache.httpcomponents" ^ "httpclient" ^ "4.5.2",
  ("org.apache.parquet" ^ "parquet-scala_2.10" ^ "1.8.1") - scala_lang
)

testDeps += "org.mockito" ^ "mockito-core" ^ "2.6.4"
