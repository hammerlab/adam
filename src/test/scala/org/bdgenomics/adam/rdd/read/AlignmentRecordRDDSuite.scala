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
import org.hammerlab.test.matchers.seqs.SeqMatcher.seqMatch
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

  sparkTest("sorting reads") {
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
    assert(expectedSortedReads === mapped)
  }

  sparkTest("coverage does not fail on unmapped reads") {
    val inputPath = testFile("unmapped.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
      .transform(rdd => {
        rdd.filter(!_.getReadMapped)
      })

    val coverage = reads.toCoverage()
    assert(coverage.rdd.count === 0)
  }

  sparkTest("computes coverage") {
    val inputPath = testFile("artificial.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)

    // get pileup at position 30
    val pointCoverage = reads.filterByOverlappingRegion(ReferenceRegion("artificial", 30, 31)).rdd.count
    val coverage: CoverageRDD = reads.toCoverage(false)
    assert(coverage.rdd.filter(r => r.start == 30).first.count == pointCoverage)
  }

  sparkTest("test filterByOverlappingRegions") {
    val inputPath = testFile("artificial.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)

    // get pileup at position 30
    val pointCoverage = reads.filterByOverlappingRegions(Array(ReferenceRegion("artificial", 30, 31)).toList).rdd.count
    val coverage: CoverageRDD = reads.toCoverage(false)
    assert(coverage.rdd.filter(r => r.start == 30).first.count == pointCoverage)
  }

  sparkTest("merges adjacent records with equal coverage values") {
    val inputPath = testFile("artificial.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)

    // repartition reads to 1 partition to acheive maximal merging of coverage
    val coverage: CoverageRDD = reads.transform(_.repartition(1)).toCoverage(true)

    assert(coverage.rdd.count == 18)
    assert(coverage.flatten().rdd.count == 170)
  }

  sparkTest("sorting reads by reference index") {
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
    assert(expectedSortedReads === mapped)
  }

  sparkTest("round trip from ADAM to SAM and back to ADAM produces equivalent Read values") {
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

    assert(rdd12B.rdd.count() === rdd12A.rdd.count())

    val reads12A = rdd12A.rdd.collect()
    val reads12B = rdd12B.rdd.collect()

    reads12A.indices.foreach {
      i ⇒
        val (readA, readB) = (reads12A(i), reads12B(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQual === readB.getQual)
        assert(readA.getCigar === readB.getCigar)
    }
  }

  sparkTest("round trip with single CRAM file produces equivalent Read values") {
    val readsPath = testFile("artificial.cram")
    val referencePath = resourceUrl("artificial.fa").toString
    sc.hadoopConfiguration.set(REFERENCE_SOURCE_PATH_PROPERTY,
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

    assert(rddB.rdd.count() === rddA.rdd.count())

    val readsA = rddA.rdd.collect()
    val readsB = rddB.rdd.collect()

    readsA.indices.foreach {
      i ⇒
        val (readA, readB) = (readsA(i), readsB(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQual === readB.getQual)
        assert(readA.getCigar === readB.getCigar)
    }
  }

  sparkTest("round trip with sharded CRAM file produces equivalent Read values") {
    val readsPath = testFile("artificial.cram")
    val referencePath = resourceUrl("artificial.fa").toString
    sc.hadoopConfiguration.set(REFERENCE_SOURCE_PATH_PROPERTY,
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

    assert(rddB.rdd.count() === rddA.rdd.count())

    val readsA = rddA.rdd.collect()
    val readsB = rddB.rdd.collect()

    readsA.indices.foreach {
      i ⇒
        val (readA, readB) = (readsA(i), readsB(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQual === readB.getQual)
        assert(readA.getCigar === readB.getCigar)
    }
  }

  sparkTest("SAM conversion sets read mapped flag properly") {
    val filePath = testFile("reads12.sam")
    val sam = sc.loadAlignments(filePath)

    sam.rdd.collect().foreach(r => assert(r.getReadMapped))
  }

  sparkTest("convert malformed FASTQ (no quality scores) => SAM => well-formed FASTQ => SAM") {
    val noqualPath = testFile("fastq_noqual.fq")

    //read FASTQ (malformed)
    val rddA = sc.loadFastq(noqualPath, None, None, LENIENT)

    val noqualAPath = tmpLocation(".sam")

    //write SAM (fixed and now well-formed)
    rddA.saveAsSam(noqualAPath)

    //read SAM
    val rddB = sc.loadAlignments(noqualAPath)

    val noqualBPath = tmpLocation(".fastq")

    //write FASTQ (well-formed)
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

  sparkTest("round trip from ADAM to FASTQ and back to ADAM produces equivalent Read values") {
    val reads12Path = testFile("fastq_sample1.fq")
    val rdd12A = sc.loadAlignments(reads12Path)

    val path = tmpLocation(".fq")

    rdd12A.saveAsFastq(path)

    val rdd12B = sc.loadAlignments(path)

    assert(rdd12B.rdd.count() === rdd12A.rdd.count())

    val reads12A = rdd12A.rdd.collect()
    val reads12B = rdd12B.rdd.collect()

    reads12A.indices.foreach {
      i ⇒
        val (readA, readB) = (reads12A(i), reads12B(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQual === readB.getQual)
        assert(readA.getReadName === readB.getReadName)
    }
  }

  sparkTest("round trip from ADAM to paired-FASTQ and back to ADAM produces equivalent Read values") {
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

    assert(rddB.rdd.count() === rddA.rdd.count())

    val readsA = rddA.rdd.collect()
    val readsB = rddB.rdd.collect()

    readsA.indices.foreach {
      i ⇒
        val (readA, readB) = (readsA(i), readsB(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQual === readB.getQual)
        assert(readA.getReadName === readB.getReadName)
    }
  }

  sparkTest("writing a small sorted file as SAM should produce the expected result") {
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

  sparkTest("writing unordered sam from unordered sam") {
    val unsortedPath = testFile("unordered.sam")
    val ardd = sc.loadBam(unsortedPath)
    val reads = ardd.rdd

    val actualUnorderedPath = tmpFile("unordered.sam")
    ardd.saveAsSam(actualUnorderedPath,
      isSorted = false,
      asSingleFile = true)

    checkFiles(actualUnorderedPath, "unordered.sam")
  }

  sparkTest("writing ordered sam from unordered sam") {
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

    assert(rRdd.rdd.count === rdd2.rdd.count)
    assert(fsp1 === fsp2)
    assert(fsf1 === fsf2)

    val jrdd =
      rRdd.rdd.map(r => ((r.getReadName, r.getReadInFragment, r.getReadMapped), r))
        .join(rdd2.rdd.map(r => ((r.getReadName, r.getReadInFragment, r.getReadMapped), r)))
        .cache()

    assert(rRdd.rdd.count === jrdd.count)

    jrdd
      .values
      .collect
      .foreach {
        case (p1, p2) ⇒

          assert(p1.getReadInFragment === p2.getReadInFragment)
          assert(p1.getReadName === p2.getReadName)
          assert(p1.getSequence === p2.getSequence)
          assert(p1.getQual === p2.getQual)
          assert(p1.getOrigQual === p2.getOrigQual)
          assert(p1.getRecordGroupSample === p2.getRecordGroupSample)
          assert(p1.getRecordGroupName === p2.getRecordGroupName)
          assert(p1.getFailedVendorQualityChecks === p2.getFailedVendorQualityChecks)
          assert(p1.getBasesTrimmedFromStart === p2.getBasesTrimmedFromStart)
          assert(p1.getBasesTrimmedFromEnd === p2.getBasesTrimmedFromEnd)

          assert(p1.getReadMapped === p2.getReadMapped)
          // note: BQSR1.sam has reads that are unmapped, but where the mapping flags are set
          // that is why we split this check out
          // the SAM spec doesn't say anything particularly meaningful about this, other than
          // that some fields should be disregarded if the read is not mapped
          if (p1.getReadMapped && p2.getReadMapped) {
            assert(p1.getDuplicateRead === p2.getDuplicateRead)
            assert(p1.getContigName === p2.getContigName)
            assert(p1.getStart === p2.getStart)
            assert(p1.getEnd === p2.getEnd)
            assert(p1.getCigar === p2.getCigar)
            assert(p1.getOldCigar === p2.getOldCigar)
            assert(p1.getPrimaryAlignment === p2.getPrimaryAlignment)
            assert(p1.getSecondaryAlignment === p2.getSecondaryAlignment)
            assert(p1.getSupplementaryAlignment === p2.getSupplementaryAlignment)
            assert(p1.getReadNegativeStrand === p2.getReadNegativeStrand)
          }

          assert(p1.getReadPaired === p2.getReadPaired)
          // a variety of fields are undefined if the reads are not paired
          if (p1.getReadPaired && p2.getReadPaired) {
            assert(p1.getInferredInsertSize === p2.getInferredInsertSize)
            assert(p1.getProperPair === p2.getProperPair)

            // same caveat about read alignment applies to mates
            assert(p1.getMateMapped === p2.getMateMapped)
            if (p1.getMateMapped && p2.getMateMapped) {
              assert(p1.getMateNegativeStrand === p2.getMateNegativeStrand)
              assert(p1.getMateContigName === p2.getMateContigName)
              assert(p1.getMateAlignmentStart === p2.getMateAlignmentStart)
              assert(p1.getMateAlignmentEnd === p2.getMateAlignmentEnd)
            }
          }
      }
  }

  sparkTest("write single sam file back") {
    testBQSR(true, "bqsr1.sam")
  }

  sparkTest("write single bam file back") {
    testBQSR(false, "bqsr1.bam")
  }

  sparkTest("saveAsParquet with save args, sequence dictionary, and record group dictionary") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation()
    reads.saveAsParquet(TestSaveArgs(outputPath))
    assert(exists(outputPath))
  }

  sparkTest("save as SAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".sam")
    reads.save(TestSaveArgs(outputPath))
    assert(exists(outputPath))
  }

  sparkTest("save as sorted SAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".sam")
    reads.save(TestSaveArgs(outputPath), true)
    assert(exists(outputPath))
  }

  sparkTest("save as BAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".bam")
    reads.save(TestSaveArgs(outputPath))
    assert(exists(outputPath))
  }

  sparkTest("save as sorted BAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".bam")
    reads.save(TestSaveArgs(outputPath), true)
    assert(exists(outputPath))
  }

  sparkTest("save as FASTQ format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".fq")
    reads.save(TestSaveArgs(outputPath))
    assert(exists(outputPath))
  }

  sparkTest("save as ADAM parquet format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".adam")
    reads.save(TestSaveArgs(outputPath))
    assert(exists(outputPath))
  }

  sparkTest("saveAsSam SAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".sam")
    reads.saveAsSam(outputPath, asType = Some(SAM))
    assert(exists(outputPath))
  }

  sparkTest("saveAsSam SAM format single file") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".sam")
    reads.saveAsSam(outputPath,
      asType = Some(SAM),
      asSingleFile = true)
    assert(exists(outputPath))
  }

  sparkTest("saveAsSam sorted SAM format single file") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".sam")
    reads.saveAsSam(outputPath,
      asType = Some(SAM),
      asSingleFile = true,
      isSorted = true)
    assert(exists(outputPath))
  }

  sparkTest("saveAsSam BAM format") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".bam")
    reads.saveAsSam(outputPath, asType = Some(BAM))
    assert(exists(outputPath))
  }

  sparkTest("saveAsSam BAM format single file") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".bam")
    reads.saveAsSam(
      outputPath,
      asType = Some(BAM),
      asSingleFile = true
    )
    assert(exists(outputPath))
  }

  sparkTest("saveAsSam sorted BAM format single file") {
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

  sparkTest("saveAsFastq") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, None)
    assert(exists(outputPath))
  }

  sparkTest("saveAsFastq with original base qualities") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, None, true)
    assert(exists(outputPath))
  }

  sparkTest("saveAsFastq sorted by read name") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, None, false, true)
    assert(exists(outputPath))
  }

  sparkTest("saveAsFastq sorted by read name with original base qualities") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, None, true, true)
    assert(exists(outputPath))
  }

  sparkTest("saveAsFastq paired FASTQ") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath1 = tmpLocation("_1.fq")
    val outputPath2 = tmpLocation("_2.fq")
    reads.saveAsFastq(outputPath1, Some(outputPath2))
    assert(exists(outputPath1))
    assert(exists(outputPath2))
  }

  sparkTest("saveAsPairedFastq") {
    val reads: AlignmentRecordRDD = sc.loadAlignments(smallSam)
    val outputPath1 = tmpLocation("_1.fq")
    val outputPath2 = tmpLocation("_2.fq")
    reads.saveAsPairedFastq(outputPath1, outputPath2)
    assert(exists(outputPath1))
    assert(exists(outputPath2))
  }

  sparkTest("don't lose any reads when piping as SAM") {
    val reads12Path = testFile("reads12.sam")
    val ardd = sc.loadBam(reads12Path)
    val records = ardd.rdd.count

    implicit val tFormatter = SAMInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    val pipedRdd: AlignmentRecordRDD = ardd.pipe("tee /dev/null")
    val newRecords = pipedRdd.rdd.count
    assert(records === newRecords)
  }

  sparkTest("don't lose any reads when piping as BAM") {
    val reads12Path = testFile("reads12.sam")
    val ardd = sc.loadBam(reads12Path)
    val records = ardd.rdd.count

    implicit val tFormatter = BAMInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    val pipedRdd: AlignmentRecordRDD = ardd.pipe("tee /dev/null")
    val newRecords = pipedRdd.rdd.count
    assert(records === newRecords)
  }

  sparkTest("can properly set environment variables inside of a pipe") {
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
        s"/bin/bash $scriptPath",
        environment =
          Map(
            "INPUT_PATH" → smallSam.toString,
            "OUTPUT_PATH" → writePath.toString
          )
      )

    val newRecords = pipedRdd.rdd.count
    assert(smallRecords === newRecords)
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
    assert(newRecords === 6)

    val tempBam = sc.loadBam(tempPath)
    assert(tempBam.rdd.count === ardd.rdd.count)
  }

  sparkTest("use broadcast join to pull down reads mapped to targets") {

    val reads = sc.loadAlignments(readsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = reads.broadcastRegionJoin(targets)

    jRdd
      .rdd
      .collect
      .map {
        case (ar, f) ⇒
          ar.getReadName → s"${f.getContigName}:${f.getStart}-${f.getEnd}"
      }
      .toSeq should seqMatch(
      Array(
        "simread:1:14397233:false" → "1:14397230-26472788",
        "simread:1:20101800:true"  → "1:14397230-26472788",
        "simread:1:26472783:false" → "1:14397230-26472788",
        "simread:1:169801933:true" → "1:169801934-169801939",
        "simread:1:240997787:true" → "1:240997788-240997796"
      )
    )

    assert(jRdd.rdd.count === 5)
  }

  sparkTest("use right outer broadcast join to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = reads.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    assert(c.count(_._1.isEmpty) === 1)
    assert(c.count(_._1.isDefined) === 5)
  }

  sparkTest("use shuffle join to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.shuffleRegionJoin(targets)
    val jRdd0 = reads.shuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    assert(jRdd.rdd.count === 5)
    assert(jRdd0.rdd.count === 5)
  }

  sparkTest("use right outer shuffle join to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = reads.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._1.isEmpty) === 1)
    assert(c0.count(_._1.isEmpty) === 1)
    assert(c.count(_._1.isDefined) === 5)
    assert(c0.count(_._1.isDefined) === 5)
  }

  sparkTest("use left outer shuffle join to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = reads.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._2.isEmpty) === 15)
    assert(c0.count(_._2.isEmpty) === 15)
    assert(c.count(_._2.isDefined) === 5)
    assert(c0.count(_._2.isDefined) === 5)
  }

  sparkTest("use full outer shuffle join to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = reads.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c0.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c.count(t => t._1.isDefined && t._2.isEmpty) === 15)
    assert(c0.count(t => t._1.isDefined && t._2.isEmpty) === 15)
    assert(c.count(t => t._1.isEmpty && t._2.isDefined) === 1)
    assert(c0.count(t => t._1.isEmpty && t._2.isDefined) === 1)
    assert(c.count(t => t._1.isDefined && t._2.isDefined) === 5)
    assert(c0.count(t => t._1.isDefined && t._2.isDefined) === 5)
  }

  sparkTest("use shuffle join with group by to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = reads.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.length === 5)
    assert(c0.length === 5)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  sparkTest("use right outer shuffle join with group by to pull down reads mapped to targets") {
    val reads = sc.loadAlignments(readsPath).transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath).transform(_.repartition(1))

    val jRdd = reads.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = reads.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd0.rdd.collect // FIXME
    val c0 = jRdd0.rdd.collect

    assert(c.count(_._1.isDefined) === 20)
    assert(c0.count(_._1.isDefined) === 20)
    assert(c.filter(_._1.isDefined).count(_._2.size == 1) === 5)
    assert(c0.filter(_._1.isDefined).count(_._2.size == 1) === 5)
    assert(c.filter(_._1.isDefined).count(_._2.isEmpty) === 15)
    assert(c0.filter(_._1.isDefined).count(_._2.isEmpty) === 15)
    assert(c.count(_._1.isEmpty) === 1)
    assert(c0.count(_._1.isEmpty) === 1)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }
}
