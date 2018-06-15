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

import java.io.{ FileNotFoundException, InputStream }

import htsjdk.samtools.ValidationStringency.{ LENIENT, SILENT, STRICT }
import htsjdk.samtools.util.Locatable
import htsjdk.samtools.{ SAMFileHeader, ValidationStringency }
import htsjdk.variant.vcf.{ VCFCompoundHeaderLine, VCFFormatHeaderLine, VCFHeader, VCFHeaderLine, VCFInfoHeaderLine }
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.{ SpecificDatumReader, SpecificRecord, SpecificRecordBase }
import org.apache.hadoop.fs.{ FileSystem, PathFilter, Path ⇒ HPath }
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.parquet.avro.{ AvroParquetInputFormat, AvroReadSupport }
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.util.ContextUtil.getConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import org.bdgenomics.adam.converters._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.io._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.projections.{ FeatureField, Projection }
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD.inferSequenceDictionary
import org.bdgenomics.adam.rdd.feature._
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.variant._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.util.{ ReferenceContigMap, ReferenceFile, TwoBitFile }
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.misc.{ HadoopUtil, Logging }
import org.hammerlab.genomics.loci.parsing.{ Range, LociRanges, ParsedLoci }
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.ContigName.Factory
import org.hammerlab.genomics.reference.Locus
import org.hammerlab.paths.Path
import org.seqdoop.hadoop_bam._
import org.seqdoop.hadoop_bam.util.SAMHeaderReader.{ VALIDATION_STRINGENCY_PROPERTY, readSAMHeaderFrom }
import org.seqdoop.hadoop_bam.util._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * Case class that wraps a reference region for use with the Indexed VCF/BAM loaders.
 */
private case class LocatableReferenceRegion(rr: ReferenceRegion)
  extends Locatable {

  /**
   * @return the start position in a 1-based closed coordinate system.
   */
  def getStart: Int = rr.start.toInt + 1

  /**
   * @return the end position in a 1-based closed coordinate system.
   */
  def getEnd: Int = rr.end.toInt

  /**
   * @return the reference contig this interval is on.
   */
  def getContig: String = rr.referenceName.name
}

/**
 * This singleton provides an implicit conversion from a SparkContext to the
 * ADAMContext, as well as implicit functions for the Pipe API.
 */
object ADAMContext {

  // conversion functions for pipes
  implicit def sameTypeConversionFn[T, U <: GenomicRDD[T, U]](gRdd: U,
                                                              rdd: RDD[T]): U = {
    // hijack the transform function to discard the old RDD
    gRdd.transform(_ => rdd)
  }

  implicit def readsToVCConversionFn(arRdd: AlignmentRecordRDD,
                                     rdd: RDD[VariantContext]): VariantContextRDD = {
    VariantContextRDD(rdd,
      arRdd.sequences,
      arRdd.recordGroups.toSamples)
  }

  implicit def fragmentsToReadsConversionFn(fRdd: FragmentRDD,
                                            rdd: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    AlignmentRecordRDD(rdd, fRdd.sequences, fRdd.recordGroups)
  }

  // Add ADAM Spark context methods
  implicit def sparkContextToADAMContext(sc: SparkContext)(implicit factory: Factory): ADAMContext = new ADAMContext(sc)

  // Add generic RDD methods for all types of ADAM RDDs
  implicit def rddToADAMRDD[T: Manifest](rdd: RDD[T])(implicit ev1: T => IndexedRecord): ConcreteADAMRDDFunctions[T] =
    new ConcreteADAMRDDFunctions(rdd)

  // Add implicits for the rich adam objects
  implicit def recordToRichRecord(record: AlignmentRecord): RichAlignmentRecord =
    new RichAlignmentRecord(record)
}

/**
 * A filter to run on globs/directories that finds all files with a given name.
 *
 * @param name The name to search for.
 */
private class FileFilter(private val name: String) extends PathFilter {

  /**
   * @param path Path to evaluate.
   * @return Returns true if the filename of the path matches the name passed
   *   to the constructor.
   */
  def accept(path: HPath): Boolean =
    path.getName == name
}

/**
 * The ADAMContext provides functions on top of a SparkContext for loading genomic data.
 *
 * @param sc The SparkContext to wrap.
 */
class ADAMContext(val sc: SparkContext)(implicit factory: Factory)
  extends Logging {

  implicit def pathToHPath(path: Path): HPath = new HPath(path.uri)

  /**
   * @param samHeader The header to extract a sequence dictionary from.
   * @return Returns the dictionary converted to an ADAM model.
   */
  private[rdd] def loadBamDictionary(samHeader: SAMFileHeader): SequenceDictionary =
    SequenceDictionary(samHeader)

  /**
   * @param samHeader The header to extract a read group dictionary from.
   * @return Returns the dictionary converted to an ADAM model.
   */
  private[rdd] def loadBamReadGroups(samHeader: SAMFileHeader): RecordGroupDictionary =
    RecordGroupDictionary.fromSAMHeader(samHeader)

  /**
   * @param path The (possibly globbed) filepath to load a VCF from.
   * @return Returns a tuple of metadata from the VCF header, including the
   *   sequence dictionary and a list of the samples contained in the VCF.
   */
  private[rdd] def loadVcfMetadata(path: Path): (SequenceDictionary, Seq[Sample], Seq[VCFHeaderLine]) = {
    // get the paths to all vcfs
    val files = getFsAndFiles(path)

    // load yonder the metadata
    files
      .map(loadSingleVcfMetadata)
      .reduce((p1, p2) ⇒
        (
          p1._1 ++ p2._1,
          p1._2 ++ p2._2,
          p1._3 ++ p2._3
        )
      )
  }

  /**
   * @param path The (possibly globbed) filepath to load a VCF from.
   * @return Returns a tuple of metadata from the VCF header, including the
   *   sequence dictionary and a list of the samples contained in the VCF.
   *
   * @see loadVcfMetadata
   */
  private def loadSingleVcfMetadata(path: Path): (SequenceDictionary, Seq[Sample], Seq[VCFHeaderLine]) = {
    val vcfHeader = readVcfHeader(path)
    val sd = SequenceDictionary.fromVCFHeader(vcfHeader)
    val samples =
      asScalaBuffer(vcfHeader.getGenotypeSamples)
      .map(s ⇒
        Sample.newBuilder()
          .setSampleId(s)
          .build()
      )

    (sd, samples, headerLines(vcfHeader))
  }

  private def readVcfHeader(path: Path): VCFHeader =
    VCFHeaderReader.readHeaderFrom(
      WrapSeekable.openPath(
        sc.hadoopConfiguration,
        path
      )
    )

  private def cleanAndMixInSupportedLines(
    headerLines: Seq[VCFHeaderLine],
    stringency: ValidationStringency): Seq[VCFHeaderLine] = {

    // dedupe
    val deduped = headerLines.distinct

    def auditLine(line: VCFCompoundHeaderLine,
                  defaultLine: VCFCompoundHeaderLine,
                  replaceFn: (String, VCFCompoundHeaderLine) => VCFCompoundHeaderLine): Option[VCFCompoundHeaderLine] = {
      if (line.getType != defaultLine.getType) {
        val msg = "Field type for provided header line (%s) does not match supported line (%s)".format(
          line, defaultLine)
        if (stringency == STRICT) {
          throw new IllegalArgumentException(msg)
        } else {
          if (stringency == LENIENT) {
            log.warn(msg)
          }
          Some(replaceFn("BAD_%s".format(line.getID), line))
        }
      } else {
        None
      }
    }

    // remove our supported header lines
    deduped.flatMap {
      case fl: VCFFormatHeaderLine ⇒
        val key = fl.getID
        DefaultHeaderLines.formatHeaderLines
          .find(_.getID == key)
          .fold(Some(fl).asInstanceOf[Option[VCFCompoundHeaderLine]]) {
            defaultLine ⇒
              auditLine(
                fl,
                defaultLine,
                (newId, oldLine) ⇒
                  new VCFFormatHeaderLine(
                    newId,
                    oldLine.getCountType,
                    oldLine.getType,
                    oldLine.getDescription
                  )
              )
          }
      case il: VCFInfoHeaderLine ⇒
        val key = il.getID
        DefaultHeaderLines.infoHeaderLines
          .find(_.getID == key)
          .fold(Some(il).asInstanceOf[Option[VCFCompoundHeaderLine]])(defaultLine => {
            auditLine(il, defaultLine, (newId, oldLine) => {
              new VCFInfoHeaderLine(newId,
                oldLine.getCountType,
                oldLine.getType,
                oldLine.getDescription)
            })
          })
      case l =>
        Some(l)
    } ++ DefaultHeaderLines.allHeaderLines
  }

  private def headerLines(header: VCFHeader): Seq[VCFHeaderLine] =
    header.getFilterLines ++
      header.getFormatHeaderLines ++
      header.getInfoHeaderLines ++
      header.getOtherHeaderLines

  private def loadHeaderLines(path: Path): Seq[VCFHeaderLine] =
    getFsAndFilesWithFilter(new HPath(path.uri), new FileFilter("_header"))
      .flatMap(p ⇒ headerLines(readVcfHeader(p)))
      .distinct

  /**
   * @param path The (possibly globbed) filepath to load Avro sequence
   *   dictionary info from.
   * @return Returns the SequenceDictionary representing said reference build.
   */
  private[rdd] def loadAvroSequences(path: Path): SequenceDictionary = {
    getFsAndFilesWithFilter(path, new FileFilter("_seqdict.avro"))
      .map(loadAvroSequencesFile)
      .reduce(_ ++ _)
  }

  /**
   * @param path The filepath to load a single Avro file of sequence
   *   dictionary info from.
   * @return Returns the SequenceDictionary representing said reference build.
   *
   * @see loadAvroSequences
   */
  private def loadAvroSequencesFile(path: Path): SequenceDictionary = {
    val avroSd = loadAvro[Contig](path, Contig.SCHEMA$)
    SequenceDictionary.fromAvro(avroSd)
  }

  /**
   * @param path The (possibly globbed) filepath to load Avro sample
   *   metadata descriptions from.
   * @return Returns a Seq of Sample descriptions.
   */
  private[rdd] def loadAvroSampleMetadata(path: Path): Seq[Sample] = {
    getFsAndFilesWithFilter(path, new FileFilter("_samples.avro"))
      .map(p => loadAvro[Sample](p, Sample.SCHEMA$))
      .reduce(_ ++ _)
  }

  /**
   * @param path The (possibly globbed) filepath to load Avro read group
   *   metadata descriptions from.
   * @return Returns a RecordGroupDictionary.
   */
  private[rdd] def loadAvroReadGroupMetadata(path: Path): RecordGroupDictionary = {
    getFsAndFilesWithFilter(path, new FileFilter("_rgdict.avro"))
      .map(loadAvroReadGroupMetadataFile)
      .reduce(_ ++ _)
  }

  /**
   * @param path The filepath to load a single Avro file containing read
   *   group metadata.
   * @return Returns a RecordGroupDictionary.
   *
   * @see loadAvroReadGroupMetadata
   */
  private def loadAvroReadGroupMetadataFile(path: Path): RecordGroupDictionary = {
    val avroRgd = loadAvro[RecordGroupMetadata](
      path,
      RecordGroupMetadata.SCHEMA$
    )

    // convert avro to record group dictionary
    new RecordGroupDictionary(avroRgd.map(RecordGroup.fromAvro))
  }

  /**
   * This method will create a new RDD.
   *
   * @param path The path to the input data
   * @param predicate An optional pushdown predicate to use when reading the data
   * @param projection An option projection schema to use when reading the data
   * @tparam T The type of records to return
   * @return An RDD with records of the specified type
   */
  def loadParquet[T: Manifest](path: Path,
                               predicate: Option[FilterPredicate] = None,
                               projection: Option[Schema] = None)(implicit ev: T => SpecificRecord): RDD[T] = {
    //make sure a type was specified
    //not using require as to make the message clearer
    if (manifest[T] == manifest[scala.Nothing])
      throw new IllegalArgumentException("Type inference failed; when loading please specify a specific type. " +
        "e.g.:\nval reads: RDD[AlignmentRecord] = ...\nbut not\nval reads = ...\nwithout a return type")

    log.info("Reading the ADAM file at %s to create RDD".format(path))
    val job = HadoopUtil.newJob(sc)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[T]])

    predicate.foreach { (pred) =>
      log.info("Using the specified push-down predicate")
      ParquetInputFormat.setFilterPredicate(job.getConfiguration, pred)
    }

    if (projection.isDefined) {
      log.info("Using the specified projection schema")
      AvroParquetInputFormat.setRequestedProjection(job, projection.get)
    }

    val records = sc.newAPIHadoopFile(
      path.uri.toString,
      classOf[ParquetInputFormat[T]],
      classOf[Void],
      manifest[T].runtimeClass.asInstanceOf[Class[T]],
      getConfiguration(job)
    )

    val instrumented =
      if (Metrics.isRecording)
        records.instrument()
      else
        records

    val mapped = instrumented.values

    if (predicate.isDefined)
      // Strip the nulls that the predicate returns
      mapped.filter(_ != null.asInstanceOf[T])
    else
      mapped
  }

  /**
   * Elaborates out a directory/glob/plain path.
   *
   * @param path Path to elaborate.
   * @param fs The underlying file system that this path is on.
   * @return Returns an array of Paths to load.
   *
   * @see [[getFsAndFiles]]
   *
   * @throws FileNotFoundException if the path does not match any files.
   */
  protected def getFiles(path: HPath, fs: FileSystem): Array[Path] = {

    // elaborate out the path; this returns FileStatuses
    val paths = if (fs.isDirectory(path)) fs.listStatus(path) else fs.globStatus(path)

    // the path must match at least one file
    if (paths == null || paths.isEmpty) {
      throw new FileNotFoundException(
        s"Couldn't find any files matching ${path.toUri}. If you are trying to" +
          " glob a directory of Parquet files, you need to glob inside the" +
          " directory as well (e.g., \"glob.me.*.adam/*\", instead of" +
          " \"glob.me.*.adam\"."
      )
    }

    // map the paths returned to their paths
    paths.map(status ⇒ Path(status.getPath.toUri))
  }

  /**
   * Elaborates out a directory/glob/plain path.
   *
   * @param path Path to elaborate.
   * @return Returns an array of Paths to load.
   *
   * @see getFiles
   *
   * @throws FileNotFoundException if the path does not match any files.
   */
  protected def getFsAndFiles(path: HPath): Array[Path] = {

    // get the underlying fs for the file
    val fs =
      Option(path.getFileSystem(sc.hadoopConfiguration)).getOrElse(
        throw new FileNotFoundException(
          s"Couldn't find filesystem for ${path.toUri} with Hadoop configuration ${sc.hadoopConfiguration}"
        )
      )

    getFiles(path, fs)
  }

  /**
   * Elaborates out a directory/glob/plain path.
   *
   * @param path Path to elaborate.
   * @param filter Filter to discard paths.
   * @return Returns an array of Paths to load.
   *
   * @see getFiles
   *
   * @throws FileNotFoundException if the path does not match any files.
   */
  protected def getFsAndFilesWithFilter(path: HPath, filter: PathFilter): Array[Path] = {

    // get the underlying fs for the file
    val fs = Option(path.getFileSystem(sc.hadoopConfiguration)).getOrElse(
      throw new FileNotFoundException(
        s"Couldn't find filesystem for ${path.toUri} with Hadoop configuration ${sc.hadoopConfiguration}"
      ))

    // elaborate out the path; this returns FileStatuses
    val paths = if (fs.isDirectory(path)) {
      fs.listStatus(path, filter)
    } else {
      fs.globStatus(path, filter)
    }

    // the path must match at least one file
    if (paths == null || paths.isEmpty) {
      throw new FileNotFoundException(
        s"Couldn't find any files matching ${path.toUri}"
      )
    }

    // map the paths returned to their paths
    paths.map(status ⇒ Path(status.getPath.toUri))
  }

  /**
   * Checks to see if a set of SAM/BAM/CRAM files are queryname sorted.
   *
   * If we are loading fragments and the SAM/BAM/CRAM files are sorted by the
   * read names, this implies that all of the reads in a pair are consecutive in
   * the file. If this is the case, we can configure Hadoop-BAM to keep all of
   * the reads from a fragment in a single split. This allows us to eliminate
   * an expensive groupBy when loading a BAM file as fragments.
   *
   * @param path The file path to load reads from. Globs/directories are
   *   supported.
   * @param stringency The validation stringency to use when reading the header.
   * @return Returns true if all files described by the filepath are queryname
   *   sorted.
   */
  private[rdd] def filesAreQuerynameSorted(path: Path,
                                           stringency: ValidationStringency = STRICT): Boolean = {
    val bamFiles = getFsAndFiles(path)
    val filteredFiles =
      bamFiles.filter(
        p ⇒
          (p.extension match {
            case "bam" | "cram" | "sam" ⇒ true
            case _ ⇒ false
          }) ||
            p.basename.startsWith("part-")
      )

    filteredFiles
      .forall { fp ⇒
        try {
          // the sort order is saved in the file header
          sc.hadoopConfiguration.set(VALIDATION_STRINGENCY_PROPERTY, stringency.toString)
          val samHeader = readSAMHeaderFrom(fp, sc.hadoopConfiguration)

          samHeader.getSortOrder == SAMFileHeader.SortOrder.queryname
        } catch {
          case e: Throwable ⇒
            log.error(
              s"Loading header failed for $fp:n${e.getMessage}\n\t${e.getStackTrace.take(25).mkString("\n\t")}"
            )
            false
        }
      }
  }

  /**
   * Loads a SAM/BAM file.
   *
   * This reads the sequence and record group dictionaries from the SAM/BAM file
   * header. SAMRecords are read from the file and converted to the
   * AlignmentRecord schema.
   *
   * @param path Path to the file on disk.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of reads,
   *   sequence dictionary representing the contigs these reads are aligned to
   *   if the reads are aligned, and the record group dictionary for the reads
   *   if one is available.
   * @see loadAlignments
   */
  def loadBam(path: Path,
              validationStringency: ValidationStringency = STRICT): AlignmentRecordRDD = {

    val bamFiles = getFsAndFiles(path)
    val filteredFiles =
      bamFiles.filter {
        p ⇒
          val extension = p.extension
          extension == "bam" ||
            extension == "cram" ||
            extension == "sam" ||
            p.basename.startsWith("part-")
      }

    require(filteredFiles.nonEmpty, s"Did not find any files at $path.")

    val (seqDict, readGroups) =
      filteredFiles
        .flatMap { fp ⇒
          try {
            // We need to separately read the header, so that we can inject the sequence dictionary
            // data into each individual Read (see the argument to samRecordConverter.convert,
            // below).
            sc.hadoopConfiguration.set(VALIDATION_STRINGENCY_PROPERTY, validationStringency.toString)
            val samHeader = readSAMHeaderFrom(fp, sc.hadoopConfiguration)
            log.info(s"Loaded header from $fp")
            val sd = loadBamDictionary(samHeader)
            val rg = loadBamReadGroups(samHeader)
            Some((sd, rg))
          } catch {
            case e: Throwable =>
              log.error(
                s"Loading failed for $fp:\n${e.getMessage}\n\t${e.getStackTrace.take(25).mkString("\n\t")}"
              )
              None
          }
        }
        .reduce((kv1, kv2) ⇒
          (kv1._1 ++ kv2._1, kv1._2 ++ kv2._2)
        )

    val job = HadoopUtil.newJob(sc)

    // this logic is counterintuitive but important.
    // hadoop-bam does not filter out .bai files, etc. as such, if we have a
    // directory of bam files where all the bams also have bais or md5s etc
    // in the same directory, hadoop-bam will barf. if the directory just
    // contains bams, hadoop-bam is a-ok! i believe that it is better (perf) to
    // just load from a single newAPIHadoopFile call instead of a union across
    // files, so we do that whenever possible
    val records =
      if (filteredFiles.length != bamFiles.length)
        sc.union(
          filteredFiles.map(
            p ⇒
              sc.newAPIHadoopFile(
                p.uri.toString,
                classOf[AnySAMInputFormat],
                classOf[LongWritable],
                classOf[SAMRecordWritable],
                getConfiguration(job)
              )
          )
        )
      else
        sc.newAPIHadoopFile(
          path.uri.toString,
          classOf[AnySAMInputFormat],
          classOf[LongWritable],
          classOf[SAMRecordWritable],
          getConfiguration(job)
        )

    if (Metrics.isRecording)
      records.instrument()

    val samRecordConverter = new SAMRecordConverter

    AlignmentRecordRDD(
      records
        .values
        .map(r => samRecordConverter.convert(r.get)),
      seqDict,
      readGroups
    )
  }

  /**
   * Functions like loadBam, but uses bam index files to look at fewer blocks,
   * and only returns records within the specified ReferenceRegions. Bam index file required.
   *
   * @param path The path to the input data. Currently this path must correspond to
   *        a single Bam file. The bam index file associated needs to have the same name.
   * @param parsedLoci Iterable of ReferenceRegions we are filtering on
   */
  def loadIndexedBam(path: Path,
                     parsedLoci: ParsedLoci,
                     includeUnmappedMates: Boolean = false)(implicit s: DummyImplicit): AlignmentRecordRDD = {

    val bamFiles = getFsAndFiles(path).filter(_.extension == "bam")

    require(bamFiles.nonEmpty, s"Did not find any files at $path.")

    val (seqDict, readGroups) =
      bamFiles
        .map { fp ⇒
          // We need to separately read the header, so that we can inject the sequence dictionary
          // data into each individual Read (see the argument to samRecordConverter.convert,
          // below).
          val samHeader = readSAMHeaderFrom(fp, sc.hadoopConfiguration)

          log.info("Loaded header from " + fp)
          val sd = loadBamDictionary(samHeader)
          val rg = loadBamReadGroups(samHeader)

          (sd, rg)
        }
        .reduce((kv1, kv2) =>
          (kv1._1 ++ kv2._1, kv1._2 ++ kv2._2)
        )

    val contigLengths = seqDict.contigLengths
    val loci = LociSet(parsedLoci, contigLengths)

    val job = HadoopUtil.newJob(sc)
    val conf = getConfiguration(job)
    BAMInputFormat.setIntervals(conf, loci.toHtsJDKIntervals)

    val records =
      sc.union(
        bamFiles.map(
          p ⇒
            sc.newAPIHadoopFile(
              p.uri.toString,
              classOf[BAMInputFormat],
              classOf[LongWritable],
              classOf[SAMRecordWritable],
              conf
            )
        )
      )

    if (Metrics.isRecording)
      records.instrument()

    val lociBroadcast = sc.broadcast(loci)
    val samRecordConverter = new SAMRecordConverter
    AlignmentRecordRDD(
      records
        .values
        .map(r ⇒ samRecordConverter.convert(r.get))
        .filter(r =>
          ReferenceRegion
            .opt(r)
            .orElse(
              if (includeUnmappedMates)
                ReferenceRegion.mateOpt(r)
              else
                None
            )
            .exists(lociBroadcast.value.intersects(_))
        ),
      seqDict,
      readGroups
    )
  }

  def loadIndexedBam(path: Path,
                     viewRegions: Iterable[ReferenceRegion])(implicit s: DummyImplicit): AlignmentRecordRDD =
    loadIndexedBam(
      path,
      LociRanges(
        viewRegions.map(
          region =>
            Range(region.referenceName, Locus(region.start), Locus(region.end))
        )
      )
    )

  /**
   * Functions like loadBam, but uses bam index files to look at fewer blocks,
   * and only returns records within a specified ReferenceRegion. Bam index file required.
   *
   * @param path The path to the input data. Currently this path must correspond to
   *        a single Bam file. The bam index file associated needs to have the same name.
   * @param viewRegion The ReferenceRegion we are filtering on
   */
  def loadIndexedBam(path: Path, viewRegion: ReferenceRegion): AlignmentRecordRDD =
    loadIndexedBam(path, Iterable(viewRegion))

  /**
   * Loads Avro data from a Hadoop File System.
   *
   * This method uses the SparkContext wrapped by this class to identify our
   * underlying file system. We then use the underlying FileSystem imp'l to
   * open the Avro file, and we read the Avro files into a Seq.
   *
   * Frustratingly enough, although all records generated by the Avro IDL
   * compiler have a static SCHEMA$ field, this field does not belong to
   * the SpecificRecordBase abstract class, or the SpecificRecord interface.
   * As such, we must force the user to pass in the schema.
   *
   * @tparam T The type of the specific record we are loading.
   * @param path Path to Vf file from.
   * @param schema Schema of records we are loading.
   * @return Returns a Seq containing the avro records.
   */
  private def loadAvro[T <: SpecificRecordBase: ClassTag](path: Path,
                                                          schema: Schema): Seq[T] = {

    // get our current file system
    val fs = path.getFileSystem(sc.hadoopConfiguration)

    // get an input stream
    val is = fs.open(path)
      .asInstanceOf[InputStream]

    // set up avro for reading
    val dr = new SpecificDatumReader[T](schema)
    val fr = new DataFileStream[T](is, dr)

    // get iterator and create an empty list
    val iter = fr.iterator
    var list = List.empty[T]

    // !!!!!
    // important implementation note:
    // !!!!!
    //
    // in theory, we should be able to call iter.toSeq to get a Seq of the
    // specific records we are reading. this would allow us to avoid needing
    // to manually pop things into a list.
    //
    // however! this causes odd problems that seem to be related to some sort of
    // lazy execution inside of scala. specifically, if you go
    // iter.toSeq.map(fn) in scala, this seems to be compiled into a lazy data
    // structure where the map call is only executed when the Seq itself is
    // actually accessed (e.g., via seq.apply(i), seq.head, etc.). typically,
    // this would be OK, but if the Seq[T] goes into a spark closure, the closure
    // cleaner will fail with a NotSerializableException, since SpecificRecord's
    // are not java serializable. specifically, we see this happen when using
    // this function to load RecordGroupMetadata when creating a
    // RecordGroupDictionary.
    //
    // good news is, you can work around this by explicitly walking the iterator
    // and building a collection, which is what we do here. this would not be
    // efficient if we were loading a large amount of avro data (since we're
    // loading all the data into memory), but currently, we are just using this
    // code for building sequence/record group dictionaries, which are fairly
    // small (seq dict is O(30) entries, rgd is O(20n) entries, where n is the
    // number of samples).
    while (iter.hasNext) {
      list = iter.next :: list
    }

    // close file
    fr.close()
    is.close()

    // reverse list and return as seq
    list.reverse
  }

  /**
   * Loads alignment data from a Parquet file.
   *
   * @param path The path of the file to load.
   * @param predicate An optional predicate to push down into the file.
   * @param projection An optional schema designating the fields to project.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of reads,
   *   sequence dictionary representing the contigs these reads are aligned to
   *   if the reads are aligned, and the record group dictionary for the reads
   *   if one is available.
   * @note The sequence dictionary is read from an avro file stored at
   *   filePath/_seqdict.avro and the record group dictionary is read from an
   *   avro file stored at filePath/_rgdict.avro. These files are pure avro,
   *   not Parquet.
   * @see loadAlignments
   */
  def loadParquetAlignments(path: Path,
                            predicate: Option[FilterPredicate] = None,
                            projection: Option[Schema] = None): AlignmentRecordRDD = {

    // load from disk
    val rdd = loadParquet[AlignmentRecord](path, predicate, projection)

    // convert avro to sequence dictionary
    val sd = loadAvroSequences(path)

    // convert avro to sequence dictionary
    val rgd = loadAvroReadGroupMetadata(path)

    AlignmentRecordRDD(rdd, sd, rgd)
  }

  /**
   * Loads reads from interleaved FASTQ.
   *
   * In interleaved FASTQ, the two reads from a paired sequencing protocol are
   * interleaved in a single file. This is a zipped representation of the
   * typical paired FASTQ.
   *
   * @param path Path to load.
   * @return Returns the file as an unaligned AlignmentRecordRDD.
   */
  def loadInterleavedFastq(path: Path): AlignmentRecordRDD = {

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(
      path.uri.toString,
      classOf[InterleavedFastqInputFormat],
      classOf[Void],
      classOf[Text],
      getConfiguration(job)
    )

    if (Metrics.isRecording)
      records.instrument()

    // convert records
    val fastqRecordConverter = new FastqRecordConverter
    AlignmentRecordRDD.unaligned(records.flatMap(fastqRecordConverter.convertPair))
  }

  /**
   * Loads (possibly paired) FASTQ data.
   *
   * @see loadPairedFastq
   * @see loadUnpairedFastq
   *
   * @param filePath1 The path where the first set of reads are.
   * @param filePath2Opt The path where the second set of reads are, if provided.
   * @param recordGroupOpt The optional record group name to associate to the
   *   reads.
   * @param stringency The validation stringency to use when validating the reads.
   * @return Returns the reads as an unaligned AlignmentRecordRDD.
   */
  def loadFastq(
    filePath1: Path,
    filePath2Opt: Option[Path],
    recordGroupOpt: Option[String] = None,
    stringency: ValidationStringency = STRICT): AlignmentRecordRDD = {
    filePath2Opt.fold(
      loadUnpairedFastq(
        filePath1,
        recordGroupOpt,
        stringency = stringency
      )
    )(filePath2 =>
      loadPairedFastq(
        filePath1,
        filePath2,
        recordGroupOpt,
        stringency
      )
    )
  }

  /**
   * Loads paired FASTQ data from two files.
   *
   * @see loadFastq
   *
   * @param filePath1 The path where the first set of reads are.
   * @param filePath2 The path where the second set of reads are.
   * @param recordGroupOpt The optional record group name to associate to the
   *   reads.
   * @param stringency The validation stringency to use when validating the reads.
   * @return Returns the reads as an unaligned AlignmentRecordRDD.
   */
  def loadPairedFastq(
    filePath1: Path,
    filePath2: Path,
    recordGroupOpt: Option[String],
    stringency: ValidationStringency): AlignmentRecordRDD = {
    val reads1 = loadUnpairedFastq(filePath1,
      recordGroupOpt,
      setFirstOfPair = true,
      stringency = stringency)
    val reads2 = loadUnpairedFastq(filePath2,
      recordGroupOpt,
      setSecondOfPair = true,
      stringency = stringency)

    stringency match {
      case STRICT | LENIENT =>
        val count1 = reads1.rdd.cache.count
        val count2 = reads2.rdd.cache.count

        if (count1 != count2) {
          val msg = s"Fastq 1 ($filePath1) has $count1 reads, fastq 2 ($filePath2) has $count2 reads"
          if (stringency == STRICT)
            throw new IllegalArgumentException(msg)
          else {
            // LENIENT
            logError(msg)
          }
        }
      case SILENT =>
    }

    AlignmentRecordRDD.unaligned(reads1.rdd ++ reads2.rdd)
  }

  /**
   * Loads unpaired FASTQ data from two files.
   *
   * @see loadFastq
   *
   * @param path The path where the first set of reads are.
   * @param recordGroupOpt The optional record group name to associate to the
   *   reads.
   * @param setFirstOfPair If true, sets the read as first from the fragment.
   * @param setSecondOfPair If true, sets the read as second from the fragment.
   * @param stringency The validation stringency to use when validating the reads.
   * @return Returns the reads as an unaligned AlignmentRecordRDD.
   */
  def loadUnpairedFastq(path: Path,
                        recordGroupOpt: Option[String] = None,
                        setFirstOfPair: Boolean = false,
                        setSecondOfPair: Boolean = false,
                        stringency: ValidationStringency = STRICT): AlignmentRecordRDD = {

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(
      path.uri.toString,
      classOf[SingleFastqInputFormat],
      classOf[Void],
      classOf[Text],
      getConfiguration(job)
    )

    if (Metrics.isRecording)
      records.instrument()

    // convert records
    val fastqRecordConverter = new FastqRecordConverter
    val basename = path.basename
    AlignmentRecordRDD.unaligned(
      records.map(
        fastqRecordConverter.convertRead(
          _,
          recordGroupOpt.map(
            recordGroup ⇒
              if (recordGroup.isEmpty)
                basename
              else
                recordGroup
          ),
          setFirstOfPair,
          setSecondOfPair,
          stringency
        )
      )
    )
  }

  /**
   * @param path File to read VCF records from.
   * @param viewRegions Optional intervals to push down into file using index.
   * @return Returns a raw RDD of (LongWritable, VariantContextWritable)s.
   */
  private def readVcfRecords(path: Path,
                             viewRegions: Option[Iterable[ReferenceRegion]]): RDD[(LongWritable, VariantContextWritable)] = {
    // load vcf data
    val job = HadoopUtil.newJob(sc)
    job.getConfiguration().setStrings(
      "io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName
    )

    val conf = getConfiguration(job)
    viewRegions.foreach { vr ⇒
      val intervals = vr.toList.map(LocatableReferenceRegion)
      VCFInputFormat.setIntervals(conf, intervals)
    }

    sc.newAPIHadoopFile(
      path.uri.toString,
      classOf[VCFInputFormat],
      classOf[LongWritable],
      classOf[VariantContextWritable],
      conf
    )
  }

  /**
   * Loads a VCF file into an RDD.
   *
   * @param path The file to load.
   * @param stringency The validation stringency to use when validating the VCF.
   * @return Returns a VariantContextRDD.
   *
   * @see loadVcfAnnotations
   */
  def loadVcf(path: Path,
              stringency: ValidationStringency = STRICT): VariantContextRDD = {

    // load records from VCF
    val records = readVcfRecords(path, None)

    // attach instrumentation
    if (Metrics.isRecording)
      records.instrument()

    // load vcf metadata
    val (sd, samples, headers) = loadVcfMetadata(path)

    val vcc = new VariantContextConverter(headers, stringency)
    VariantContextRDD(
      records.values.flatMap(r => vcc.convert(r.get)),
      sd,
      samples,
      cleanAndMixInSupportedLines(headers, stringency)
    )
  }

  /**
   * Loads a VCF file indexed by a tabix (tbi) file into an RDD.
   *
   * @param path The file to load.
   * @param viewRegion ReferenceRegions we are filtering on.
   * @return Returns a VariantContextRDD.
   */
  def loadIndexedVcf(path: Path,
                     viewRegion: ReferenceRegion): VariantContextRDD =
    loadIndexedVcf(path, Iterable(viewRegion))

  /**
   * Loads a VCF file indexed by a tabix (tbi) file into an RDD.
   *
   * @param path The file to load.
   * @param viewRegions Iterator of ReferenceRegions we are filtering on.
   * @param stringency The validation stringency to use when validating the VCF.
   * @return Returns a VariantContextRDD.
   */
  def loadIndexedVcf(path: Path,
                     viewRegions: Iterable[ReferenceRegion],
                     stringency: ValidationStringency = STRICT)(implicit s: DummyImplicit): VariantContextRDD = {

    // load records from VCF
    val records = readVcfRecords(path, Some(viewRegions))

    // attach instrumentation
    if (Metrics.isRecording)
      records.instrument()

    // load vcf metadata
    val (sd, samples, headers) = loadVcfMetadata(path)

    val vcc = new VariantContextConverter(headers, stringency)
    VariantContextRDD(
      records.flatMap(p => vcc.convert(p._2.get)),
      sd,
      samples,
      cleanAndMixInSupportedLines(headers, stringency)
    )
  }

  /**
   * Loads Genotypes stored in Parquet with accompanying metadata.
   *
   * @param path The path to load files from.
   * @param predicate An optional predicate to push down into the file.
   * @param projection An optional projection to use for reading.
   * @return Returns a GenotypeRDD.
   */
  def loadParquetGenotypes(path: Path,
                           predicate: Option[FilterPredicate] = None,
                           projection: Option[Schema] = None): GenotypeRDD = {
    val rdd = loadParquet[Genotype](path, predicate, projection)

    // load header lines
    val headers = loadHeaderLines(path)

    // load sequence info
    val sd = loadAvroSequences(path)

    // load avro record group dictionary and convert to samples
    val samples = loadAvroSampleMetadata(path)

    GenotypeRDD(rdd, sd, samples, headers)
  }

  /**
   * Loads Variants stored in Parquet with accompanying metadata.
   *
   * @param path The path to load files from.
   * @param predicate An optional predicate to push down into the file.
   * @param projection An optional projection to use for reading.
   * @return Returns a VariantRDD.
   */
  def loadParquetVariants(path: Path,
                          predicate: Option[FilterPredicate] = None,
                          projection: Option[Schema] = None): VariantRDD = {
    val rdd = loadParquet[Variant](path, predicate, projection)
    val sd = loadAvroSequences(path)

    // load header lines
    val headers = loadHeaderLines(path)

    VariantRDD(rdd, sd, headers)
  }

  /**
   * Loads a FASTA file.
   *
   * @param path The path to load from.
   * @param fragmentLength The length to split contigs into. This sets the
   *   parallelism achievable.
   * @return Returns a NucleotideContigFragmentRDD containing the contigs.
   */
  def loadFasta(
    path: Path,
    fragmentLength: Long): NucleotideContigFragmentRDD = {
    val fastaData: RDD[(LongWritable, Text)] =
      sc.newAPIHadoopFile(
        path.uri.toString,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text]
      )

    if (Metrics.isRecording)
      fastaData.instrument()

    val remapData = fastaData.map(kv => (kv._1.get, kv._2.toString))

    // convert rdd and cache
    val fragmentRdd = FastaConverter(remapData, fragmentLength)
      .cache()

    NucleotideContigFragmentRDD(fragmentRdd)
  }

  /**
   * Loads interleaved FASTQ data as Fragments.
   *
   * Fragments represent all of the reads from a single sequenced fragment as
   * a single object, which is a useful representation for some tasks.
   *
   * @param path The path to load.
   * @return Returns a FragmentRDD containing the paired reads grouped by
   *   sequencing fragment.
   */
  def loadInterleavedFastqAsFragments(path: Path): FragmentRDD = {

    val job = HadoopUtil.newJob(sc)
    val records =
      sc.newAPIHadoopFile(
        path.uri.toString,
        classOf[InterleavedFastqInputFormat],
        classOf[Void],
        classOf[Text],
        getConfiguration(job)
      )

    if (Metrics.isRecording)
      records.instrument()

    // convert records
    val fastqRecordConverter = new FastqRecordConverter
    FragmentRDD.fromRdd(records.map(fastqRecordConverter.convertFragment))
  }

  /**
   * Loads file of Features to a CoverageRDD.
   * Coverage is stored in the score attribute of Feature.
   *
   * @param path File path to load coverage from.
   * @return CoverageRDD containing an RDD of Coverage
   */
  def loadCoverage(path: Path): CoverageRDD = loadFeatures(path).toCoverage

  /**
   * Loads Parquet file of Features to a CoverageRDD.
   * Coverage is stored in the score attribute of Feature.
   *
   * @param path File path to load coverage from.
   * @param predicate An optional predicate to push down into the file.
   * @return CoverageRDD containing an RDD of Coverage
   */
  def loadParquetCoverage(path: Path,
                          predicate: Option[FilterPredicate] = None): CoverageRDD = {
    val proj =
      Projection(
        FeatureField.contigName,
        FeatureField.start,
        FeatureField.end,
        FeatureField.score
      )

    loadParquetFeatures(
      path,
      predicate = predicate,
      projection = Some(proj)
    )
    .toCoverage
  }

  /**
   * Loads features stored in GFF3 format.
   *
   * @param path The path to the file to load.
   * @param optStorageLevel Optional storage level to use for cache before building the SequenceDictionary.
   *   Defaults to StorageLevel.MEMORY_ONLY.
   * @param minPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism.
   * @param stringency Optional stringency to pass. LENIENT stringency will warn
   *   when a malformed line is encountered, SILENT will ignore the malformed
   *   line, STRICT will throw an exception.
   * @return Returns a FeatureRDD.
   */
  def loadGff3(path: Path,
               optStorageLevel: Option[StorageLevel] = Some(MEMORY_ONLY),
               minPartitions: Option[Int] = None,
               stringency: ValidationStringency = LENIENT): FeatureRDD = {
    val records =
      textFile(path, minPartitions)
        .flatMap(new GFF3Parser().parse(_, stringency))

    if (Metrics.isRecording)
      records.instrument()

    inferSequenceDictionary(records, optStorageLevel = optStorageLevel)
  }

  /**
   * Loads features stored in GFF2/GTF format.
   *
   * @param path The path to the file to load.
   * @param optStorageLevel Optional storage level to use for cache before building the SequenceDictionary.
   *   Defaults to StorageLevel.MEMORY_ONLY.
   * @param minPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism.
   * @param stringency Optional stringency to pass. LENIENT stringency will warn
   *   when a malformed line is encountered, SILENT will ignore the malformed
   *   line, STRICT will throw an exception.
   * @return Returns a FeatureRDD.
   */
  def loadGtf(path: Path,
              optStorageLevel: Option[StorageLevel] = Some(MEMORY_ONLY),
              minPartitions: Option[Int] = None,
              stringency: ValidationStringency = LENIENT): FeatureRDD = {
    val records =
      textFile(path, minPartitions)
        .flatMap(new GTFParser().parse(_, stringency))

    if (Metrics.isRecording)
      records.instrument()

    inferSequenceDictionary(records, optStorageLevel = optStorageLevel)
  }

  def textFile(path: Path, minPartitionsOpt: Option[Int]): RDD[String] =
    sc.textFile(path.uri.toString(), minPartitionsOpt.getOrElse(sc.defaultParallelism))

  /**
   * Loads features stored in BED6/12 format.
   *
   * @param path The path to the file to load.
   * @param optStorageLevel Optional storage level to use for cache before building the SequenceDictionary.
   *   Defaults to StorageLevel.MEMORY_ONLY.
   * @param minPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism.
   * @param stringency Optional stringency to pass. LENIENT stringency will warn
   *   when a malformed line is encountered, SILENT will ignore the malformed
   *   line, STRICT will throw an exception.
   * @return Returns a FeatureRDD.
   */
  def loadBed(path: Path,
              optStorageLevel: Option[StorageLevel] = Some(MEMORY_ONLY),
              minPartitions: Option[Int] = None,
              stringency: ValidationStringency = LENIENT): FeatureRDD = {
    val records =
      textFile(path, minPartitions)
        .flatMap(new BEDParser().parse(_, stringency))

    if (Metrics.isRecording)
      records.instrument()

    inferSequenceDictionary(records, optStorageLevel = optStorageLevel)
  }

  /**
   * Loads features stored in NarrowPeak format.
   *
   * @param path The path to the file to load.
   * @param optStorageLevel Optional storage level to use for cache before building the SequenceDictionary.
   *   Defaults to StorageLevel.MEMORY_ONLY.
   * @param minPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism.
   * @param stringency Optional stringency to pass. LENIENT stringency will warn
   *   when a malformed line is encountered, SILENT will ignore the malformed
   *   line, STRICT will throw an exception.
   * @return Returns a FeatureRDD.
   */
  def loadNarrowPeak(path: Path,
                     optStorageLevel: Option[StorageLevel] = Some(MEMORY_ONLY),
                     minPartitions: Option[Int] = None,
                     stringency: ValidationStringency = LENIENT): FeatureRDD = {
    val records =
      textFile(path, minPartitions)
        .flatMap(new NarrowPeakParser().parse(_, stringency))

    if (Metrics.isRecording)
      records.instrument()

    inferSequenceDictionary(records, optStorageLevel = optStorageLevel)
  }

  /**
   * Loads features stored in IntervalList format.
   *
   * @param path The path to the file to load.
   * @param minPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism.
   * @param stringency Optional stringency to pass. LENIENT stringency will warn
   *   when a malformed line is encountered, SILENT will ignore the malformed
   *   line, STRICT will throw an exception.
   * @return Returns a FeatureRDD.
   */
  def loadIntervalList(path: Path,
                       minPartitions: Option[Int] = None,
                       stringency: ValidationStringency = LENIENT): FeatureRDD = {

    val parsedLines =
      textFile(path, minPartitions)
        .map(new IntervalListParser().parseWithHeader(_, stringency))

    val seqDict = SequenceDictionary(parsedLines.flatMap(_._1).collect(): _*)

    val records = parsedLines.flatMap(_._2)

    if (Metrics.isRecording)
      records.instrument()

    FeatureRDD(records, seqDict)
  }

  /**
   * Loads Features stored in Parquet, with accompanying metadata.
   *
   * @param path The path to load files from.
   * @param predicate An optional predicate to push down into the file.
   * @param projection An optional projection to use for reading.
   * @return Returns a FeatureRDD.
   */
  def loadParquetFeatures(path: Path,
                          predicate: Option[FilterPredicate] = None,
                          projection: Option[Schema] = None): FeatureRDD = {
    val sd = loadAvroSequences(path)
    val rdd = loadParquet[Feature](path, predicate, projection)
    FeatureRDD(rdd, sd)
  }

  /**
   * Loads NucleotideContigFragments stored in Parquet, with metadata.
   *
   * @param path The path to load files from.
   * @param predicate An optional predicate to push down into the file.
   * @param projection An optional projection to use for reading.
   * @return Returns a NucleotideContigFragmentRDD.
   */
  def loadParquetContigFragments(path: Path,
                                 predicate: Option[FilterPredicate] = None,
                                 projection: Option[Schema] = None): NucleotideContigFragmentRDD = {
    val sd = loadAvroSequences(path)
    val rdd = loadParquet[NucleotideContigFragment](path, predicate, projection)
    NucleotideContigFragmentRDD(rdd, sd)
  }

  /**
   * Loads Fragments stored in Parquet, with accompanying metadata.
   *
   * @param path The path to load files from.
   * @param predicate An optional predicate to push down into the file.
   * @param projection An optional projection to use for reading.
   * @return Returns a FragmentRDD.
   */
  def loadParquetFragments(path: Path,
                           predicate: Option[FilterPredicate] = None,
                           projection: Option[Schema] = None): FragmentRDD = {

    // convert avro to sequence dictionary
    val sd = loadAvroSequences(path)

    // convert avro to sequence dictionary
    val rgd = loadAvroReadGroupMetadata(path)

    // load fragment data from parquet
    val rdd = loadParquet[Fragment](path, predicate, projection)

    FragmentRDD(rdd, sd, rgd)
  }

  /**
   * Loads Features from a file, autodetecting the file type.
   *
   * Loads files ending in .bed as BED6/12, .gff3 as GFF3, .gtf/.gff as
   * GTF/GFF2, .narrow[pP]eak as NarrowPeak, and .interval_list as
   * IntervalList. If none of these match, we fall back to Parquet.
   *
   * @param path The path to the file to load.
   * @param optStorageLevel Optional storage level to use for cache before building the SequenceDictionary.
   *   Defaults to StorageLevel.MEMORY_ONLY.
   * @param projection An optional projection to push down.
   * @param minPartitions An optional minimum number of partitions to use. For
   *   textual formats, if this is None, we fall back to the Spark default
   *   parallelism.
   * @return Returns a FeatureRDD.
   *
   * @see loadBed
   * @see loadGtf
   * @see loadGff3
   * @see loadNarrowPeak
   * @see loadIntervalList
   * @see loadParquetFeatures
   */
  def loadFeatures(path: Path,
                   optStorageLevel: Option[StorageLevel] = Some(MEMORY_ONLY),
                   projection: Option[Schema] = None,
                   minPartitions: Option[Int] = None): FeatureRDD =
    path.extension match {
      case "bed" ⇒
        log.info(s"Loading $path as BED and converting to features. Projection is ignored.")
        loadBed(path, optStorageLevel = optStorageLevel, minPartitions = minPartitions)
      case "gff3" ⇒
        log.info(s"Loading $path as GFF3 and converting to features. Projection is ignored.")
        loadGff3(path, optStorageLevel = optStorageLevel, minPartitions = minPartitions)
      case "gtf" ⇒
        log.info(s"Loading $path as GTF/GFF2 and converting to features. Projection is ignored.")
        loadGtf(path, optStorageLevel = optStorageLevel, minPartitions = minPartitions)
      case "narrowPeak" | "narrowpeak" ⇒
        log.info(s"Loading $path as NarrowPeak and converting to features. Projection is ignored.")
        loadNarrowPeak(path, optStorageLevel = optStorageLevel, minPartitions = minPartitions)
      case "interval_list" ⇒
        log.info(s"Loading $path as IntervalList and converting to features. Projection is ignored.")
        loadIntervalList(path, minPartitions = minPartitions)
      case _ ⇒
        log.info(s"Loading $path as Parquet containing Features.")
        loadParquetFeatures(path, predicate = None, projection = projection)
    }

  /**
   * Auto-detects the file type and loads a broadcastable ReferenceFile.
   *
   * If the file type is 2bit, loads a 2bit file. Else, uses loadSequences
   * to load the reference as an RDD, which is then collected to the driver.
   *
   * @param path The path to load.
   * @param fragmentLength The length of fragment to use for splitting.
   * @return Returns a broadcastable ReferenceFile.
   *
   * @see loadSequences
   */
  def loadReferenceFile(path: Path, fragmentLength: Long): ReferenceFile =
    if (path.extension == "2bit")
      TwoBitFile(path)
    else
      ReferenceContigMap(loadSequences(path, fragmentLength = fragmentLength).rdd)

  /**
   * Auto-detects the file type and loads contigs as a NucleotideContigFragmentRDD.
   *
   * Loads files ending in .fa/.fasta/.fa.gz/.fasta.gz as FASTA, else, falls
   * back to Parquet.
   *
   * @param path The path to load.
   * @param projection An optional subset of fields to load.
   * @param fragmentLength The length of fragment to use for splitting.
   * @return Returns a NucleotideContigFragmentRDD.
   *
   * @see loadFasta
   * @see loadParquetContigFragments
   * @see loadReferenceFile
   */
  def loadSequences(path: Path,
                    projection: Option[Schema] = None,
                    fragmentLength: Long = 10000): NucleotideContigFragmentRDD = {
    val basename = path.basename
    if (basename.endsWith(".fa") ||
      basename.endsWith(".fasta") ||
      basename.endsWith(".fa.gz") ||
      basename.endsWith(".fasta.gz")) {
      log.info(s"Loading $path as FASTA and converting to NucleotideContigFragment. Projection is ignored.")
      loadFasta(
        path,
        fragmentLength
      )
    } else {
      log.info(s"Loading $path as Parquet containing NucleotideContigFragments.")
      loadParquetContigFragments(path, None, projection)
    }
  }

  private def isVcfExt(path: Path): Boolean = {
    val basename = path.basename
    basename.endsWith(".vcf") ||
      basename.endsWith(".vcf.gz") ||
      basename.endsWith(".vcf.bgzf") ||
      basename.endsWith(".vcf.bgz")
  }

  /**
   * Auto-detects the file type and loads a GenotypeRDD.
   *
   * If the file has a .vcf/.vcf.gz/.vcf.bgzf/.vcf.bgz extension, loads as VCF. Else, falls back to
   * Parquet.
   *
   * @param path The path to load.
   * @param projection An optional subset of fields to load.
   * @param stringency The validation stringency to use when validating the VCF.
   * @return Returns a GenotypeRDD.
   *
   * @see loadVcf
   * @see loadParquetGenotypes
   */
  def loadGenotypes(path: Path,
                    projection: Option[Schema] = None,
                    stringency: ValidationStringency = STRICT): GenotypeRDD =
    if (isVcfExt(path)) {
      log.info(s"Loading $path as VCF, and converting to Genotypes. Projection is ignored.")
      loadVcf(path, stringency).toGenotypeRDD
    } else {
      log.info(s"Loading $path as Parquet containing Genotypes. Sequence dictionary for translation is ignored.")
      loadParquetGenotypes(path, None, projection)
    }

  /**
   * Auto-detects the file type and loads a VariantRDD.
   *
   * If the file has a .vcf/.vcf.gz/.vcf.bgzf/.vcf.bgz extension, loads as VCF. Else, falls back to
   * Parquet.
   *
   * @param path The path to load.
   * @param projection An optional subset of fields to load.
   * @param stringency The validation stringency to use when validating the VCF.
   * @return Returns a VariantRDD.
   *
   * @see loadVcf
   * @see loadParquetVariants
   */
  def loadVariants(path: Path,
                   projection: Option[Schema] = None,
                   stringency: ValidationStringency = STRICT): VariantRDD =
    if (isVcfExt(path)) {
      log.info(s"Loading $path as VCF, and converting to Variants. Projection is ignored.")
      loadVcf(path, stringency).toVariantRDD
    } else {
      log.info(s"Loading $path as Parquet containing Variants. Sequence dictionary for translation is ignored.")
      loadParquetVariants(path, None, projection)
    }

  /**
   * Loads alignments from a given path, and infers the input type.
   *
   * This method can load:
   *
   * * AlignmentRecords via Parquet (default)
   * * SAM/BAM/CRAM (.sam, .bam, .cram)
   * * FASTQ (interleaved, single end, paired end) (.ifq, .fq/.fastq)
   * * FASTA (.fa, .fasta)
   * * NucleotideContigFragments via Parquet (.contig.adam)
   *
   * As hinted above, the input type is inferred from the file path extension.
   *
   * @param path Path to load data from.
   * @param projection The fields to project; ignored if not Parquet.
   * @param path2Opt The path to load a second end of FASTQ data from.
   *  Ignored if not FASTQ.
   * @param recordGroupOpt Optional record group name to set if loading FASTQ.
   * @param stringency Validation stringency used on FASTQ import/merging.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of reads,
   *   sequence dictionary representing the contigs these reads are aligned to
   *   if the reads are aligned, and the record group dictionary for the reads
   *   if one is available.
   * @see loadBam
   * @see loadParquetAlignments
   * @see loadInterleavedFastq
   * @see loadFastq
   * @see loadFasta
   */
  def loadAlignments(path: Path,
                     projection: Option[Schema] = None,
                     path2Opt: Option[Path] = None,
                     recordGroupOpt: Option[String] = None,
                     stringency: ValidationStringency = STRICT): AlignmentRecordRDD =
    LoadAlignmentRecords.time {
      path.extension match {
        case "sam" | "bam" | "cram" ⇒
          log.info(s"Loading $path as SAM/BAM/CRAM and converting to AlignmentRecords. Projection is ignored.")
          loadBam(path, stringency)
        case "ifq" ⇒
          log.info(s"Loading $path as interleaved FASTQ and converting to AlignmentRecords. Projection is ignored.")
          loadInterleavedFastq(path)
        case "fq" | "fastq" ⇒
          log.info(s"Loading $path as unpaired FASTQ and converting to AlignmentRecords. Projection is ignored.")
          loadFastq(path, path2Opt, recordGroupOpt, stringency)
        case "fa" | "fasta" ⇒
          log.info(s"Loading $path as FASTA and converting to AlignmentRecords. Projection is ignored.")
          AlignmentRecordRDD.unaligned(loadFasta(path, fragmentLength = 10000).toReads)
        case _ ⇒
          if (path.endsWith("contig.adam")) {
            log.info(s"Loading $path as Parquet of NucleotideContigFragment and converting to AlignmentRecords. Projection is ignored.")
            AlignmentRecordRDD.unaligned(loadParquetContigFragments(path).toReads)
          } else {
            log.info(s"Loading $path as Parquet of AlignmentRecords.")
            loadParquetAlignments(path, None, projection)
          }
      }
    }

  /**
   * Auto-detects the file type and loads a FragmentRDD.
   *
   * This method can load:
   *
   * * Fragments via Parquet (default)
   * * SAM/BAM/CRAM (.sam, .bam, .cram)
   * * FASTQ (interleaved only --> .ifq)
   * * Autodetects AlignmentRecord as Parquet with .reads.adam extension.
   *
   * @param path Path to load data from.
   * @return Returns the loaded data as a FragmentRDD.
   */
  def loadFragments(path: Path): FragmentRDD =
    LoadFragments.time {
      val extension = path.extension
      if (extension == "sam" ||
        extension == "bam" ||
        extension == "cram") {
        log.info(s"Loading $path as SAM/BAM and converting to Fragments.")
        loadBam(path).toFragments
      } else if (path.endsWith(".reads.adam")) {
        log.info(s"Loading $path as ADAM AlignmentRecords and converting to Fragments.")
        loadAlignments(path).toFragments
      } else if (extension == "ifq") {
        log.info("Loading interleaved FASTQ " + path + " and converting to Fragments.")
        loadInterleavedFastqAsFragments(path)
      } else {
        loadParquetFragments(path)
      }
    }
}
