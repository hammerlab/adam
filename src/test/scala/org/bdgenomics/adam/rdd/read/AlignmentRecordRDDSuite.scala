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
package org.bdgenomics.adam.rdd.read

import java.nio.file.Files.exists

import htsjdk.samtools.ValidationStringency.{ LENIENT, STRICT }
import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.models.{ RecordGroupDictionary, ReferenceRegion, SequenceDictionary, SequenceRecord }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.rdd.feature.CoverageRDD
import org.bdgenomics.adam.rdd.variant.{ VCFOutFormatter, VariantContextRDD }
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._
import org.hammerlab.genomics.reference.test.LociConversions.intToLocus
import org.hammerlab.test.matchers._
import org.seqdoop.hadoop_bam.CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY
import org.seqdoop.hadoop_bam.SAMFormat.{ BAM, CRAM, SAM }

import scala.util.Random

private object SequenceIndexWithReadOrdering
  extends Ordering[((Int, Long), (AlignmentRecord, Int))] {

  def compare(a: ((Int, Long), (AlignmentRecord, Int)),
              b: ((Int, Long), (AlignmentRecord, Int))): Int =
    if (a._1._1 == b._1._1)
      a._1._2.compareTo(b._1._2)
    else
      a._1._1.compareTo(b._1._1)
}

class AlignmentRecordRDDSuite
  extends ADAMFunSuite {

  val readsPath = testFile("small.1.sam")
  val targetsPath = testFile("small.1.bed")

  val smallSam = testFile("small.sam")

  test("sorting reads") {
    val random = new Random("sorting".hashCode)
    val numReadsToCreate = 1000
    val reads = for (i <- 0 until numReadsToCreate) yield {
      val mapped = random.nextBoolean()
      val builder = AlignmentRecord.newBuilder().setReadMapped(mapped)
      if (mapped) {
        val contigName = random.nextInt(numReadsToCreate / 10).toString
        val start = random.nextInt(1000000)
        builder
          .setContigName(contigName)
          .setStart(start)
          .setEnd(start)
      }
      builder.setReadName((0 until 20).map(i => (random.nextInt(100) + 64)).mkString)
      builder.build()
    }
    val rdd = sc.parallelize(reads)

    // make seq dict 
    val contigNames = rdd.flatMap(r => Option(r.getContigName)).distinct.collect
    val sd = new SequenceDictionary(contigNames.map(v => SequenceRecord(v, 1000000)).toVector)

    val sortedReads =
      AlignmentRecordRDD(rdd, sd, RecordGroupDictionary.empty)
        .sortReadsByReferencePosition()
        .rdd
        .collect()
        .zipWithIndex

    val (mapped, unmapped) = sortedReads.partition(_._1.getReadMapped)
    // Make sure that all the unmapped reads are placed at the end
    assert(unmapped.forall(p => p._2 > mapped.takeRight(1)(0)._2))
    // Make sure that we appropriately sorted the reads
    val expectedSortedReads = mapped.sortWith(
      (a, b) => a._1.getContigName < b._1.getContigName && a._1.getStart < b._1.getStart)
    expectedSortedReads should be(mapped)
  }

  test("coverage does not fail on unmapped reads") {
    val inputPath = testFile("unmapped.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
      .transform(rdd => {
        rdd.filter(!_.getReadMapped)
      })

    val coverage = reads.toCoverage()
    ==(coverage.rdd.count, 0)
  }

  test("computes coverage") {
    val inputPath = testFile("artificial.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)

    // get pileup at position 30
    val pointCoverage = reads.filterByOverlappingRegion(ReferenceRegion("artificial", 30, 31)).rdd.count
    val coverage: CoverageRDD = reads.toCoverage(false)
    assert(coverage.rdd.filter(r => r.start == 30).first.count == pointCoverage)
  }

  test("test filterByOverlappingRegions") {
    val inputPath = testFile("artificial.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)

    // get pileup at position 30
    val pointCoverage = reads.filterByOverlappingRegions(Array(ReferenceRegion("artificial", 30, 31)).toList).rdd.count
    val coverage: CoverageRDD = reads.toCoverage(false)
    assert(coverage.rdd.filter(r => r.start == 30).first.count == pointCoverage)
  }

  test("merges adjacent records with equal coverage values") {
    val inputPath = testFile("artificial.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)

    // repartition reads to 1 partition to acheive maximal merging of coverage
    val coverage: CoverageRDD = reads.transform(_.repartition(1)).toCoverage(true)

    assert(coverage.rdd.count == 18)
    assert(coverage.flatten().rdd.count == 170)
  }

  test("sorting reads by reference index") {
    val random = new Random("sortingIndices".hashCode)
    val numReadsToCreate = 1000
    val reads = for (i <- 0 until numReadsToCreate) yield {
      val mapped = random.nextBoolean()
      val builder = AlignmentRecord.newBuilder().setReadMapped(mapped)
      if (mapped) {
        val contigName = random.nextInt(numReadsToCreate / 10).toString
        val start = random.nextInt(1000000)
        builder
          .setContigName(contigName)
          .setStart(start)
          .setEnd(start)
      }
      builder.setReadName((0 until 20).map(i => (random.nextInt(100) + 64)).mkString)
      builder.build()
    }
    val contigNames = reads.filter(_.getReadMapped).map(_.getContigName).toSet
    val sd = new SequenceDictionary(contigNames.toSeq
      .zipWithIndex
      .map(kv => {
        val (name, index) = kv
        SequenceRecord(name, Int.MaxValue, referenceIndex = Some(index))
      }).toVector)

    val rdd = sc.parallelize(reads)
    val sortedReads = AlignmentRecordRDD(rdd, sd, RecordGroupDictionary.empty)
      .sortReadsByReferencePositionAndIndex()
      .rdd
      .collect()
      .zipWithIndex
    val (mapped, unmapped) = sortedReads.partition(_._1.getReadMapped)

    // Make sure that all the unmapped reads are placed at the end
    assert(unmapped.forall(p => p._2 > mapped.takeRight(1)(0)._2))

    def toIndex(r: AlignmentRecord): Int = {
      sd(r.getContigName).get.referenceIndex.get
    }

    // Make sure that we appropriately sorted the reads
    import scala.math.Ordering._
    val expectedSortedReads = mapped.map(kv => {
      val (r, idx) = kv
      val start: Long = r.getStart
      ((toIndex(r), start), (r, idx))
    }).sortBy(_._1)
      .map(_._2)
    expectedSortedReads should be(mapped)
  }

  test("round trip from ADAM to SAM and back to ADAM produces equivalent Read values") {
    val reads12Path = testFile("reads12.sam")
    val ardd = sc.loadBam(reads12Path)
    val rdd12A = ardd.rdd

    val dir = tmpDir("reads12")
    val outputPath = dir / "reads12.sam"
    ardd.saveAsSam(
      outputPath,
      asType = Some(SAM)
    )

    val rdd12B = sc.loadBam(outputPath / "part-r-00000")

    ==(rdd12B.rdd.count(), rdd12A.rdd.count())

    val reads12A = rdd12A.rdd.collect()
    val reads12B = rdd12B.rdd.collect()

    reads12A.indices.foreach {
      i ⇒
        val (readA, readB) = (reads12A(i), reads12B(i))
        ==(readA.getSequence, readB.getSequence)
        ==(readA.getQual, readB.getQual)
        ==(readA.getCigar, readB.getCigar)
    }
  }

  test("round trip with single CRAM file produces equivalent Read values") {
    val readsPath = testFile("artificial.cram")
    val referencePath = resourceUrl("artificial.fa").toString
    hadoopConf.set(REFERENCE_SOURCE_PATH_PROPERTY,
      referencePath)

    val ardd = sc.loadBam(readsPath)
    val rddA = ardd.rdd

    val tempFile = tmpFile("artificial.cram")
    ardd.saveAsSam(
      tempFile,
      asType = Some(CRAM),
      asSingleFile = true,
      isSorted = true
    )

    val rddB = sc.loadBam(tempFile)

    ==(rddB.rdd.count(), rddA.rdd.count())

    val readsA = rddA.rdd.collect()
    val readsB = rddB.rdd.collect()

    readsA.indices.foreach {
      i ⇒
        val (readA, readB) = (readsA(i), readsB(i))
        ==(readA.getSequence, readB.getSequence)
        ==(readA.getQual, readB.getQual)
        ==(readA.getCigar, readB.getCigar)
    }
  }

  test("round trip with sharded CRAM file produces equivalent Read values") {
    val readsPath = testFile("artificial.cram")
    val referencePath = resourceUrl("artificial.fa").toString
    hadoopConf.set(REFERENCE_SOURCE_PATH_PROPERTY,
      referencePath)

    val ardd = sc.loadBam(readsPath)
    val rddA = ardd.rdd

    val tempFile = tmpFile("artificial.cram")
    ardd.saveAsSam(
      tempFile,
      asType = Some(CRAM),
      asSingleFile = false,
      isSorted = true
    )

    val rddB = sc.loadBam(tempFile + "/part-r-00000")

    ==(rddB.rdd.count(), rddA.rdd.count())

    val readsA = rddA.rdd.collect()
    val readsB = rddB.rdd.collect()

    readsA.indices.foreach {
      i ⇒
        val (readA, readB) = (readsA(i), readsB(i))
        ==(readA.getSequence, readB.getSequence)
        ==(readA.getQual, readB.getQual)
        ==(readA.getCigar, readB.getCigar)
    }
  }

  test("SAM conversion sets read mapped flag properly") {
    val filePath = testFile("reads12.sam")
    val sam = sc.loadAlignments(filePath)

    sam.rdd.collect().foreach(r => assert(r.getReadMapped))
  }

  test("convert malformed FASTQ (no quality scores) => SAM => well-formed FASTQ => SAM") {
    val noqualPath = testFile("fastq_noqual.fq")

    // read FASTQ (malformed)
    val rddA = sc.loadFastq(noqualPath, None, None, LENIENT)

    val noqualAPath = tmpLocation(".sam")

    // write SAM (fixed and now well-formed)
    rddA.saveAsSam(noqualAPath)

    // read SAM
    val rddB = sc.loadAlignments(noqualAPath)

    val noqualBPath = tmpLocation(".fastq")

    // write FASTQ (well-formed)
    rddB.saveAsFastq(noqualBPath)

    //read FASTQ (well-formed)
    val rddC = sc.loadFastq(noqualBPath, None, None, STRICT)

    val noqualA = rddA.rdd.collect()
    val noqualB = rddB.rdd.collect()
    val noqualC = rddC.rdd.collect()
    noqualA.indices.foreach {
      i ⇒
        val (readA, readB, readC) = (noqualA(i), noqualB(i), noqualC(i))
        assert(readA.getQual != "*")
        assert(readB.getQual == "B" * readB.getSequence.length)
        assert(readB.getQual == readC.getQual)
    }
  }

  test("round trip from ADAM to FASTQ and back to ADAM produces equivalent Read values") {
    val reads12Path = testFile("fastq_sample1.fq")
    val rdd12A = sc.loadAlignments(reads12Path)

    val path = tmpLocation(".fq")

    rdd12A.saveAsFastq(path)

    val rdd12B = sc.loadAlignments(path)

    ==(rdd12B.rdd.count(), rdd12A.rdd.count())

    val reads12A = rdd12A.rdd.collect()
    val reads12B = rdd12B.rdd.collect()

    reads12A.indices.foreach {
      i ⇒
        val (readA, readB) = (reads12A(i), reads12B(i))
        ==(readA.getSequence, readB.getSequence)
        ==(readA.getQual, readB.getQual)
        ==(readA.getReadName, readB.getReadName)
    }
  }

  test("round trip from ADAM to paired-FASTQ and back to ADAM produces equivalent Read values") {
    val path1 = testFile("proper_pairs_1.fq")
    val path2 = testFile("proper_pairs_2.fq")
    val rddA = sc.loadAlignments(path1).reassembleReadPairs(sc.loadAlignments(path2).rdd,
      validationStringency = STRICT)

    assert(rddA.rdd.count() == 6)

    val dir = tmpDir()
    val tempPath1 = dir / "reads1.fq"
    val tempPath2 = dir / "reads2.fq"

    rddA.saveAsPairedFastq(tempPath1, tempPath2, validationStringency = STRICT)

    val rddB = sc.loadAlignments(tempPath1).reassembleReadPairs(sc.loadAlignments(tempPath2).rdd,
      validationStringency = STRICT)

    ==(rddB.rdd.count(), rddA.rdd.count())

    val readsA = rddA.rdd.collect()
    val readsB = rddB.rdd.collect()

    readsA.indices.foreach {
      i ⇒
        val (readA, readB) = (readsA(i), readsB(i))
        ==(readA.getSequence, readB.getSequence)
        ==(readA.getQual, readB.getQual)
        ==(readA.getReadName, readB.getReadName)
    }
  }

  test("writing a small sorted file as SAM should produce the expected result") {
    val unsortedPath = testFile("unsorted.sam")
    val ardd = sc.loadBam(unsortedPath)
    val reads = ardd.rdd

    val actualSortedPath = tmpFile("sorted.sam")
    ardd.sortReadsByReferencePosition()
      .saveAsSam(actualSortedPath,
        isSorted = true,
        asSingleFile = true)

    checkFiles(actualSortedPath, "sorted.sam")
  }

  test("writing unordered sam from unordered sam") {
    val unsortedPath = testFile("unordered.sam")
    val ardd = sc.loadBam(unsortedPath)
    val reads = ardd.rdd

    val actualUnorderedPath = tmpFile("unordered.sam")
    ardd.saveAsSam(actualUnorderedPath,
      isSorted = false,
      asSingleFile = true)

    checkFiles(actualUnorderedPath, "unordered.sam")
  }

  test("writing ordered sam from unordered sam") {
    val unsortedPath = testFile("unordered.sam")
    val ardd = sc.loadBam(unsortedPath)
    val reads = ardd.sortReadsByReferencePosition()

    val actualSortedPath = tmpFile("ordered.sam")

    reads.saveAsSam(
      actualSortedPath,
      isSorted = true,
      asSingleFile = true
    )

    checkFiles(actualSortedPath, "ordered.sam")
  }

  def testBQSR(asSam: Boolean, basename: String) {
    val inputPath = testFile("bqsr1.sam")
    val dir = tmpDir("bqsr1")
    val path = dir / basename
    val rRdd = sc.loadAlignments(inputPath)
    rRdd.rdd.cache()
    rRdd.saveAsSam(
      path,
      asType =
        Some(
          if (asSam)
            SAM
          else
            BAM
        ),
      asSingleFile = true
    )

    val rdd2 = sc.loadAlignments(path)
    rdd2.rdd.cache()

    val (fsp1, fsf1) = rRdd.flagStat()
    val (fsp2, fsf2) = rdd2.flagStat()

    ==(rRdd.rdd.count, rdd2.rdd.count)
    ==(fsp1, fsp2)
    ==(fsf1, fsf2)

    val jrdd =
      rRdd.rdd.map(r => ((r.getReadName, r.getReadInFragment, r.getReadMapped), r))
        .join(rdd2.rdd.map(r => ((r.getReadName, r.getReadInFragment, r.getReadMapped), r)))
        .cache()

    ==(rRdd.rdd.count, jrdd.count)

    jrdd
      .values
      .collect
      .foreach {
        case (p1, p2) ⇒

          ==(p1.getReadInFragment, p2.getReadInFragment)
          ==(p1.getReadName, p2.getReadName)
          ==(p1.getSequence, p2.getSequence)
          ==(p1.getQual, p2.getQual)
          ==(p1.getOrigQual, p2.getOrigQual)
          ==(p1.getRecordGroupSample, p2.getRecordGroupSample)
          ==(p1.getRecordGroupName, p2.getRecordGroupName)
          ==(p1.getFailedVendorQualityChecks, p2.getFailedVendorQualityChecks)
          ==(p1.getBasesTrimmedFromStart, p2.getBasesTrimmedFromStart)
          ==(p1.getBasesTrimmedFromEnd, p2.getBasesTrimmedFromEnd)

          ==(p1.getReadMapped, p2.getReadMapped)
          // note: BQSR1.sam has reads that are unmapped, but where the mapping flags are set
          // that is why we split this check out
          // the SAM spec doesn't say anything particularly meaningful about this, other than
          // that some fields should be disregarded if the read is not mapped
          if (p1.getReadMapped && p2.getReadMapped) {
            ==(p1.getDuplicateRead, p2.getDuplicateRead)
            ==(p1.getContigName, p2.getContigName)
            ==(p1.getStart, p2.getStart)
            ==(p1.getEnd, p2.getEnd)
            ==(p1.getCigar, p2.getCigar)
            ==(p1.getOldCigar, p2.getOldCigar)
            ==(p1.getPrimaryAlignment, p2.getPrimaryAlignment)
            ==(p1.getSecondaryAlignment, p2.getSecondaryAlignment)
            ==(p1.getSupplementaryAlignment, p2.getSupplementaryAlignment)
            ==(p1.getReadNegativeStrand, p2.getReadNegativeStrand)
          }

          ==(p1.getReadPaired, p2.getReadPaired)
          // a variety of fields are undefined if the reads are not paired
          if (p1.getReadPaired && p2.getReadPaired) {
            ==(p1.getInferredInsertSize, p2.getInferredInsertSize)
            ==(p1.getProperPair, p2.getProperPair)

            // same caveat about read alignment applies to mates
            ==(p1.getMateMapped, p2.getMateMapped)
            if (p1.getMateMapped && p2.getMateMapped) {
              ==(p1.getMateNegativeStrand, p2.getMateNegativeStrand)
              ==(p1.getMateContigName, p2.getMateContigName)
              ==(p1.getMateAlignmentStart, p2.getMateAlignmentStart)
              ==(p1.getMateAlignmentEnd, p2.getMateAlignmentEnd)
            }
          }
      }
  }

  test("write single sam file back") {
    testBQSR(true, "bqsr1.sam")
  }

  test("write single bam file back") {
    testBQSR(false, "bqsr1.bam")
  }

  test("saveAsParquet with save args, sequence dictionary, and record group dictionary") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation()
    reads.saveAsParquet(TestSaveArgs(outputPath))
    assert(exists(outputPath))
  }

  test("save as SAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".sam")
    reads.save(TestSaveArgs(outputPath))
    assert(exists(outputPath))
  }

  test("save as sorted SAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".sam")
    reads.save(TestSaveArgs(outputPath), true)
    assert(exists(outputPath))
  }

  test("save as BAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".bam")
    reads.save(TestSaveArgs(outputPath))
    assert(exists(outputPath))
  }

  test("save as sorted BAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".bam")
    reads.save(TestSaveArgs(outputPath), true)
    assert(exists(outputPath))
  }

  test("save as FASTQ format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".fq")
    reads.save(TestSaveArgs(outputPath))
    assert(exists(outputPath))
  }

  test("save as ADAM parquet format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".adam")
    reads.save(TestSaveArgs(outputPath))
    assert(exists(outputPath))
  }

  test("saveAsSam SAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".sam")
    reads.saveAsSam(outputPath, asType = Some(SAM))
    assert(exists(outputPath))
  }

  test("saveAsSam SAM format single file") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".sam")
    reads.saveAsSam(outputPath,
      asType = Some(SAM),
      asSingleFile = true)
    assert(exists(outputPath))
  }

  test("saveAsSam sorted SAM format single file") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".sam")
    reads.saveAsSam(outputPath,
      asType = Some(SAM),
      asSingleFile = true,
      isSorted = true)
    assert(exists(outputPath))
  }

  test("saveAsSam BAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".bam")
    reads.saveAsSam(outputPath, asType = Some(BAM))
    assert(exists(outputPath))
  }

  test("saveAsSam BAM format single file") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".bam")
    reads.saveAsSam(
      outputPath,
      asType = Some(BAM),
      asSingleFile = true
    )
    assert(exists(outputPath))
  }

  test("saveAsSam sorted BAM format single file") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".bam")
    reads.saveAsSam(
      outputPath,
      asType = Some(BAM),
      asSingleFile = true,
      isSorted = true
    )
    assert(exists(outputPath))
  }

  test("saveAsFastq") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, None)
    assert(exists(outputPath))
  }

  test("saveAsFastq with original base qualities") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, None, true)
    assert(exists(outputPath))
  }

  test("saveAsFastq sorted by read name") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, None, false, true)
    assert(exists(outputPath))
  }

  test("saveAsFastq sorted by read name with original base qualities") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, None, true, true)
    assert(exists(outputPath))
  }

  test("saveAsFastq paired FASTQ") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath1 = tmpLocation("_1.fq")
    val outputPath2 = tmpLocation("_2.fq")
    reads.saveAsFastq(outputPath1, Some(outputPath2))
    assert(exists(outputPath1))
    assert(exists(outputPath2))
  }

  test("saveAsPairedFastq") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath1 = tmpLocation("_1.fq")
    val outputPath2 = tmpLocation("_2.fq")
    reads.saveAsPairedFastq(outputPath1, outputPath2)
    assert(exists(outputPath1))
    assert(exists(outputPath2))
  }

  test("don't lose any reads when piping as SAM") {
    val reads12Path = testFile("reads12.sam")
    val ardd = sc.loadBam(reads12Path)
    val records = ardd.rdd.count

    implicit val tFormatter = SAMInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    val pipedRdd: AlignmentRecordRDD = ardd.pipe("tee /dev/null")
    val newRecords = pipedRdd.rdd.count
    ==(records, newRecords)
  }

  test("don't lose any reads when piping as BAM") {
    val reads12Path = testFile("reads12.sam")
    val ardd = sc.loadBam(reads12Path)
    val records = ardd.rdd.count

    implicit val tFormatter = BAMInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    val pipedRdd: AlignmentRecordRDD = ardd.pipe("tee /dev/null")
    val newRecords = pipedRdd.rdd.count
    ==(records, newRecords)
  }

  test("can properly set environment variables inside of a pipe") {
    val reads12Path = testFile("reads12.sam")
    val scriptPath = testFile("env_test_command.sh")
    val ardd = sc.loadBam(reads12Path)

    ardd.rdd.count

    val smallRecords = sc.loadBam(smallSam).rdd.count
    val writePath = tmpLocation("reads12.sam")

    implicit val tFormatter = SAMInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    val pipedRdd: AlignmentRecordRDD =
      ardd.pipe(
        s"/bin/bash ${scriptPath.path}",
        environment =
          Map(
            "INPUT_PATH" → smallSam.path.toString,
            "OUTPUT_PATH" → writePath.path.toString
          )
      )

    val newRecords = pipedRdd.rdd.count
    ==(smallRecords, newRecords)
  }

  ignore("read vcf from alignment record pipe") {
    val vcfPath = testFile("small.vcf")
    val scriptPath = testFile("test_command.sh")
    val tempPath = tmpLocation(".sam")
    val ardd = sc.loadBam(readsPath)

    implicit val tFormatter = SAMInFormatter
    implicit val uFormatter = VCFOutFormatter(DefaultHeaderLines.allHeaderLines)

    val pipedRdd: VariantContextRDD =
      ardd.pipe(
        "/bin/bash $0 %s $1".format(tempPath),
        files = Seq(scriptPath, vcfPath).map(_.toString)
      )

    val newRecords = pipedRdd.rdd.count
    ==(newRecords, 6)

    val tempBam = sc.loadBam(tempPath)
    ==(tempBam.rdd.count, ardd.rdd.count)
  }

  test("use broadcast join to pull down reads mapped to targets") {

    val reads = sc.loadAlignments(readsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = reads.broadcastRegionJoin(targets)

    jRdd
      .rdd
      .collect
      .map {
        case (ar, f) ⇒
          ar.getReadName → s"${f.getContigName}:${f.getStart}-${f.getEnd}"
      } should arrMatch(
      Seq(
        "simread:1:14397233:false" → "1:14397230-26472788",
        "simread:1:20101800:true"  → "1:14397230-26472788",
        "simread:1:26472783:false" → "1:14397230-26472788",
        "simread:1:169801933:true" → "1:169801934-169801939",
        "simread:1:240997787:true" → "1:240997788-240997796"
      )
    )

    ==(jRdd.rdd.count, 5)
  }

  test("use right outer broadcast join to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = reads.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    ==(c.count(_._1.isEmpty), 1)
    ==(c.count(_._1.isDefined), 5)
  }

  test("use shuffle join to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.shuffleRegionJoin(targets)
    val jRdd0 = reads.shuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    ==(jRdd.rdd.count, 5)
    ==(jRdd0.rdd.count, 5)
  }

  test("use right outer shuffle join to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = reads.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.count(_._1.isEmpty), 1)
    ==(c0.count(_._1.isEmpty), 1)
    ==(c.count(_._1.isDefined), 5)
    ==(c0.count(_._1.isDefined), 5)
  }

  test("use left outer shuffle join to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = reads.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.count(_._2.isEmpty), 15)
    ==(c0.count(_._2.isEmpty), 15)
    ==(c.count(_._2.isDefined), 5)
    ==(c0.count(_._2.isDefined), 5)
  }

  test("use full outer shuffle join to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = reads.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.count(t => t._1.isEmpty && t._2.isEmpty), 0)
    ==(c0.count(t => t._1.isEmpty && t._2.isEmpty), 0)
    ==(c.count(t => t._1.isDefined && t._2.isEmpty), 15)
    ==(c0.count(t => t._1.isDefined && t._2.isEmpty), 15)
    ==(c.count(t => t._1.isEmpty && t._2.isDefined), 1)
    ==(c0.count(t => t._1.isEmpty && t._2.isDefined), 1)
    ==(c.count(t => t._1.isDefined && t._2.isDefined), 5)
    ==(c0.count(t => t._1.isDefined && t._2.isDefined), 5)
  }

  test("use shuffle join with group by to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = reads.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.length, 5)
    ==(c0.length, 5)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  test("use right outer shuffle join with group by to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath).transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath).transform(_.repartition(1))

    val jRdd = reads.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = reads.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd0.rdd.collect // FIXME
    val c0 = jRdd0.rdd.collect

    ==(c.count(_._1.isDefined), 20)
    ==(c0.count(_._1.isDefined), 20)
    ==(c.filter(_._1.isDefined).count(_._2.size == 1), 5)
    ==(c0.filter(_._1.isDefined).count(_._2.size == 1), 5)
    ==(c.filter(_._1.isDefined).count(_._2.isEmpty), 15)
    ==(c0.filter(_._1.isDefined).count(_._2.isEmpty), 15)
    ==(c.count(_._1.isEmpty), 1)
    ==(c0.count(_._1.isEmpty), 1)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }
}
