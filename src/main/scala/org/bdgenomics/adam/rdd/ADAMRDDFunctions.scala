/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd

import java.io.OutputStream
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.{ SpecificDatumWriter, SpecificRecordBase }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{ OutputFormat => NewOutputFormat }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.{ InstrumentedOutputFormat, RDD }
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.utils.cli.SaveArgs
import org.bdgenomics.utils.misc.HadoopUtil
import org.bdgenomics.utils.misc.Logging
import org.apache.parquet.avro.AvroParquetOutputFormat
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.ContextUtil
import scala.reflect.ClassTag

/**
 * Argument configuration for saving any output format.
 */
trait ADAMSaveAnyArgs extends SaveArgs {

  /**
   * If true and saving as FASTQ, we will sort by read name.
   */
  var sortFastqOutput: Boolean

  /**
   * If true and saving as a legacy format, we will write shards so that they
   * can be merged into a single file.
   *
   * @see deferMerging
   */
  var asSingleFile: Boolean

  /**
   * If true and asSingleFile is true, we will not merge the shards once we
   * write them, and will leave them for the user to merge later. If false and
   * asSingleFile is true, then we will merge the shards on write. If
   * asSingleFile is false, this is ignored.
   *
   * @see asSingleFile
   */
  var deferMerging: Boolean
}

private[rdd] abstract class ADAMRDDFunctions[T <% IndexedRecord: Manifest] extends Serializable with Logging {

  val rdd: RDD[T]

  /**
   * Saves Avro data to a Hadoop file system.
   *
   * This method uses a SparkContext to identify our underlying file system,
   * which we then save to.
   *
   * Frustratingly enough, although all records generated by the Avro IDL
   * compiler have a static SCHEMA$ field, this field does not belong to
   * the SpecificRecordBase abstract class, or the SpecificRecord interface.
   * As such, we must force the user to pass in the schema.
   *
   * @tparam U The type of the specific record we are saving.
   * @param filename Path to save records to.
   * @param sc SparkContext used for identifying underlying file system.
   * @param schema Schema of records we are saving.
   * @param avro Seq of records we are saving.
   */
  protected def saveAvro[U <: SpecificRecordBase](filename: String,
                                                  sc: SparkContext,
                                                  schema: Schema,
                                                  avro: Seq[U])(implicit tUag: ClassTag[U]) {

    // get our current file system
    val path = new Path(filename)
    val fs = path.getFileSystem(sc.hadoopConfiguration)

    // get an output stream
    val os = fs.create(path)
      .asInstanceOf[OutputStream]

    // set up avro for writing
    val dw = new SpecificDatumWriter[U](schema)
    val fw = new DataFileWriter[U](dw)
    fw.create(schema, os)

    // write all our records
    avro.foreach(r => fw.append(r))

    // close the file
    fw.close()
    os.close()
  }

  protected def saveRddAsParquet(args: SaveArgs): Unit = {
    saveRddAsParquet(
      args.outputPath,
      args.blockSize,
      args.pageSize,
      args.compressionCodec,
      args.disableDictionaryEncoding
    )
  }

  /**
   * Saves an RDD of Avro data to Parquet.
   *
   * @param filePath The path to save the file to.
   * @param blockSize The size in bytes of blocks to write.
   * @param pageSize The size in bytes of pages to write.
   * @param compressCodec The compression codec to apply to pages.
   * @param disableDictionaryEncoding If false, dictionary encoding is used. If
   *   true, delta encoding is used.
   * @param schema The schema to set.
   */
  protected def saveRddAsParquet(
    filePath: String,
    blockSize: Int = 128 * 1024 * 1024,
    pageSize: Int = 1 * 1024 * 1024,
    compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
    disableDictionaryEncoding: Boolean = false,
    schema: Option[Schema] = None): Unit = SaveAsADAM.time {
    log.info("Saving data in ADAM format")

    val job = HadoopUtil.newJob(rdd.context)
    ParquetOutputFormat.setCompression(job, compressCodec)
    ParquetOutputFormat.setEnableDictionary(job, !disableDictionaryEncoding)
    ParquetOutputFormat.setBlockSize(job, blockSize)
    ParquetOutputFormat.setPageSize(job, pageSize)
    AvroParquetOutputFormat.setSchema(
      job,
      schema.getOrElse(manifest[T].runtimeClass.asInstanceOf[Class[T]].newInstance().getSchema)
    )

    // Add the Void Key
    val recordToSave = rdd.map(p => (null, p))
    // Save the values to the ADAM/Parquet file
    recordToSave.saveAsNewAPIHadoopFile(
      filePath,
      classOf[java.lang.Void], manifest[T].runtimeClass.asInstanceOf[Class[T]], classOf[InstrumentedADAMAvroParquetOutputFormat],
      ContextUtil.getConfiguration(job)
    )
  }
}

@deprecated("Extend ADAMRDDFunctions and mix in GenomicRDD wherever possible in new development.",
  since = "0.20.0")
private[rdd] class ConcreteADAMRDDFunctions[T <% IndexedRecord: Manifest](val rdd: RDD[T]) extends ADAMRDDFunctions[T] {

  /**
   * Saves an RDD of Avro data to Parquet.
   *
   * @param args The output format configuration to use when saving the data.
   */
  def saveAsParquet(args: SaveArgs): Unit = {
    saveRddAsParquet(args)
  }

  /**
   * Saves an RDD of Avro data to Parquet.
   *
   * @param filePath The path to save the file to.
   * @param blockSize The size in bytes of blocks to write.
   * @param pageSize The size in bytes of pages to write.
   * @param compressCodec The compression codec to apply to pages.
   * @param disableDictionaryEncoding If false, dictionary encoding is used. If
   *   true, delta encoding is used.
   * @param schema The schema to set.
   */
  def saveAsParquet(
    filePath: String,
    blockSize: Int = 128 * 1024 * 1024,
    pageSize: Int = 1 * 1024 * 1024,
    compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
    disableDictionaryEncoding: Boolean = false,
    schema: Option[Schema] = None): Unit = {
    saveRddAsParquet(filePath, blockSize, pageSize, compressCodec, disableDictionaryEncoding, schema)
  }
}

private[rdd] class InstrumentedADAMAvroParquetOutputFormat extends InstrumentedOutputFormat[Void, IndexedRecord] {
  override def outputFormatClass(): Class[_ <: NewOutputFormat[Void, IndexedRecord]] = classOf[AvroParquetOutputFormat[IndexedRecord]]
  override def timerName(): String = WriteADAMRecord.timerName
}