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

import java.io.FileNotFoundException

import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.adam.util.PhredUtils._
import org.bdgenomics.formats.avro._
import org.hammerlab.genomics.reference.test.ClearContigNames
import org.hammerlab.paths.Path
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.Matchers
import org.seqdoop.hadoop_bam.CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY

case class TestSaveArgs(outputPath: Path)
  extends ADAMSaveAnyArgs {
  var sortFastqOutput = false
  var asSingleFile = false
  var deferMerging = false
  var blockSize = 128 * 1024 * 1024
  var pageSize = 1 * 1024 * 1024
  var compressionCodec = CompressionCodecName.GZIP
  var logLevel = "SEVERE"
  var disableDictionaryEncoding = false
}

class ADAMContextSuite
  extends ADAMFunSuite
    with Matchers
    with ConversionCheckedTripleEquals
    with ClearContigNames {

  test("ctr is accessible") {
    new ADAMContext(sc)
  }

  test("sc.loadParquet should not fail on unmapped reads") {
    val readsFilepath = testFile("unmapped.sam")

    // Convert the reads12.sam file into a parquet file
    val bamReads: RDD[AlignmentRecord] = sc.loadAlignments(readsFilepath).rdd
    ==(bamReads.rdd.count, 200)
  }

  test("sc.loadParquet should not load a file without a type specified") {
    //load an existing file from the resources and save it as an ADAM file.
    //This way we are not dependent on the ADAM format (as we would if we used a pre-made ADAM file)
    //but we are dependent on the unmapped.sam file existing, maybe I should make a new one
    val readsFilepath = testFile("unmapped.sam")
    val bamReads = sc.loadAlignments(readsFilepath)
    //save it as an Adam file so we can test the Adam loader
    val bamReadsAdamFile = tmpLocation(".adam")
    bamReads.saveAsParquet(bamReadsAdamFile)
    intercept[IllegalArgumentException] {
      sc.loadParquet(bamReadsAdamFile)
    }
    //finally just make sure we did not break anything,we came might as well
    val returnType: RDD[AlignmentRecord] = sc.loadParquet(bamReadsAdamFile)
    assert(manifest[returnType.type] != manifest[RDD[Nothing]])
  }

  test("can read a small .SAM file") {
    val path = testFile("small.sam")
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path).rdd
    ==(reads.count(), 20)
  }

  test("can read a small .CRAM file") {
    val path = testFile("artificial.cram")
    val referencePath = resourceUrl("artificial.fa").toString
    hadoopConf.set(
      REFERENCE_SOURCE_PATH_PROPERTY,
      referencePath
    )
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path).rdd
    ==(reads.count(), 10)
  }

  test("can read a small .SAM with all attribute tag types") {
    val path = testFile("tags.sam")
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path).rdd
    ==(reads.count(), 7)
  }

  test("can filter a .SAM file based on quality") {
    val path = testFile("small.sam")
    val reads: RDD[AlignmentRecord] =
      sc
        .loadAlignments(path)
        .rdd
        .filter(a ⇒ a.getReadMapped && a.getMapq > 30)

    ==(reads.count(), 18)
  }

  test("Can convert to phred") {
    ==(successProbabilityToPhred(0.9), 10)
    ==(successProbabilityToPhred(0.99999), 50)
  }

  test("Can convert from phred") {
    // result is floating point, so apply tolerance
    assert(phredToSuccessProbability(10) > 0.89 && phredToSuccessProbability(10) < 0.91)
    assert(phredToSuccessProbability(50) > 0.99998 && phredToSuccessProbability(50) < 0.999999)
  }

  test("Can read a .gtf file") {
    val path = testFile("Homo_sapiens.GRCh37.75.trun20.gtf")
    val features: RDD[Feature] = sc.loadFeatures(path).rdd
    ==(features.count, 15)
  }

  test("Can read a .bed file") {
    // note: this .bed doesn't actually conform to the UCSC BED spec...sigh...
    val path = testFile("gencode.v7.annotation.trunc10.bed")
    val features: RDD[Feature] = sc.loadFeatures(path).rdd
    ==(features.count, 10)
  }

  test("Can read a .bed file without cache") {
    val path = testFile("gencode.v7.annotation.trunc10.bed")
    val features: RDD[Feature] = sc.loadFeatures(path, optStorageLevel = Some(StorageLevel.NONE)).rdd
    ==(features.count, 10)
  }

  test("Can read a .narrowPeak file") {
    val path = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val annot: RDD[Feature] = sc.loadFeatures(path).rdd
    ==(annot.count, 10)
  }

  test("Can read a .interval_list file") {
    val path = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val annot: RDD[Feature] = sc.loadFeatures(path).rdd
    assert(annot.count == 369)
    val arr = annot.collect

    val first = arr.find(f => f.getContigName == "chr1" && f.getStart == 14415L && f.getEnd == 14499L).get
    ==(first.getName, "gn|DDX11L1;gn|RP11-34P13.2;ens|ENSG00000223972;ens|ENSG00000227232;vega|OTTHUMG00000000958;vega|OTTHUMG00000000961")

    val last = arr.find(f => f.getContigName == "chrY" && f.getStart == 27190031L && f.getEnd == 27190210L).get
    ==(last.getName, "gn|BPY2C;ccds|CCDS44030;ens|ENSG00000185894;vega|OTTHUMG00000045199")
  }

  test("can read a small .vcf file") {
    val path = testFile("small.vcf")

    val gts = sc.loadGenotypes(path)
    val vcRdd = gts.toVariantContextRDD
    val vcs = vcRdd.rdd.collect.sortBy(_.position)
    ==(vcs.length, 6)

    val vc = vcs.head
    val variant = vc.variant.variant
    ==(variant.getContigName, "1")
    ==(variant.getStart, 14396L)
    ==(variant.getEnd, 14400L)
    ==(variant.getReferenceAllele, "CTGT")
    ==(variant.getAlternateAllele, "C")
    assert(variant.getNames.isEmpty)
    ==(variant.getFiltersApplied, true)
    ==(variant.getFiltersPassed, false)
    assert(variant.getFiltersFailed.contains("IndelQD"))

    ==(vc.genotypes.size, 3)

    val gt = vc.genotypes.head
    assert(gt.getVariantCallingAnnotations != null)
    ==(gt.getReadDepth, 20)
  }

  test("can read a gzipped .vcf file") {
    val path = testFile("test.vcf.gz")
    val vcs = sc.loadVcf(path)
    ==(vcs.rdd.count, 7)
  }

  test("can read a vcf file with an empty alt") {
    val path = testFile("test.vcf")
    val vcs = sc.loadVariants(path)
    ==(vcs.rdd.count, 7)
  }

  test("can read a BGZF gzipped .vcf file with .gz file extension") {
    val path = testFile("test.vcf.bgzf.gz")
    val vcs = sc.loadVcf(path)
    ==(vcs.rdd.count, 7)
  }

  test("can read a BGZF gzipped .vcf file with .bgz file extension") {
    val path = testFile("test.vcf.bgz")
    val vcs = sc.loadVcf(path)
    ==(vcs.rdd.count, 7)
  }

  ignore("can read an uncompressed BCFv2.2 file") { // see https://github.com/samtools/htsjdk/issues/507
    val path = testFile("test.uncompressed.bcf")
    val vcs = sc.loadVcf(path)
    ==(vcs.rdd.count, 7)
  }

  ignore("can read a BGZF compressed BCFv2.2 file") { // see https://github.com/samtools/htsjdk/issues/507
    val path = testFile("test.compressed.bcf")
    val vcs = sc.loadVcf(path)
    ==(vcs.rdd.count, 7)
  }

  test("loadIndexedVcf with 1 ReferenceRegion") {
    val path = testFile("bqsr1.vcf")
    val refRegion = ReferenceRegion("22", 16097643, 16098647)
    val vcs = sc.loadIndexedVcf(path, refRegion)
    assert(vcs.rdd.count == 17)
  }

  test("loadIndexedVcf with multiple ReferenceRegions") {
    val path = testFile("bqsr1.vcf")
    val refRegion1 = ReferenceRegion("22", 16050677, 16050822)
    val refRegion2 = ReferenceRegion("22", 16097643, 16098647)
    val vcs = sc.loadIndexedVcf(path, Iterable(refRegion1, refRegion2))
    assert(vcs.rdd.count == 23)
  }

  (1 to 4) foreach { testNumber =>
    val inputName = "interleaved_fastq_sample%d.ifq".format(testNumber)
    val path = testFile(inputName)

    test("import records from interleaved FASTQ: %d".format(testNumber)) {

      val reads = sc.loadAlignments(path)
      if (testNumber == 1) {
        ==(reads.rdd.count, 6)
        ==(reads.rdd.filter(_.getReadPaired).count, 6)
        ==(reads.rdd.filter(_.getReadInFragment == 0).count, 3)
        ==(reads.rdd.filter(_.getReadInFragment == 1).count, 3)
      } else {
        ==(reads.rdd.count, 4)
        ==(reads.rdd.filter(_.getReadPaired).count, 4)
        ==(reads.rdd.filter(_.getReadInFragment == 0).count, 2)
        ==(reads.rdd.filter(_.getReadInFragment == 1).count, 2)
      }

      reads.rdd.collect.foreach(r ⇒ ==(r.getSequence.length, 250))
      reads.rdd.collect.foreach(r ⇒ ==(r.getQual.length, 250))
    }
  }

  (1 to 4) foreach { testNumber =>
    val inputName = "fastq_sample%d.fq".format(testNumber)
    val path = testFile(inputName)

    test("import records from single ended FASTQ: %d".format(testNumber)) {

      val reads = sc.loadAlignments(path)
      if (testNumber == 1) {
        ==(reads.rdd.count, 6)
        ==(reads.rdd.filter(_.getReadPaired).count, 0)
      } else if (testNumber == 4) {
        ==(reads.rdd.count, 4)
        ==(reads.rdd.filter(_.getReadPaired).count, 0)
      } else {
        ==(reads.rdd.count, 5)
        ==(reads.rdd.filter(_.getReadPaired).count, 0)
      }

      reads.rdd.collect.foreach(r ⇒ ==(r.getSequence.length, 250))
      reads.rdd.collect.foreach(r ⇒ ==(r.getQual.length, 250))
    }
  }

  test("filter on load using the filter2 API") {
    val path = testFile("bqsr1.vcf")

    val variants = sc.loadVariants(path)
    ==(variants.rdd.count, 681)

    val loc = tmpLocation()
    variants.saveAsParquet(loc, 1024, 1024) // force more than one row group (block)

    val pred: FilterPredicate = LongColumn("start") === 16097631L
    // the following only reads one row group
    val adamVariants = sc.loadParquetVariants(loc, predicate = Some(pred))
    ==(adamVariants.rdd.count, 1)
  }

  test("saveAsParquet with file path") {
    val inputPath = testFile("small.sam")
    val reads = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation()
    reads.saveAsParquet(outputPath)
    val reloadedReads = sc.loadAlignments(outputPath)
    ==(reads.rdd.count, reloadedReads.rdd.count)
  }

  test("saveAsParquet with file path, block size, page size") {
    val inputPath = testFile("small.sam")
    val reads = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation()
    reads.saveAsParquet(outputPath, 1024, 2048)
    val reloadedReads = sc.loadAlignments(outputPath)
    ==(reads.rdd.count, reloadedReads.rdd.count)
  }

  test("saveAsParquet with save args") {
    val inputPath = testFile("small.sam")
    val reads = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation()
    reads.saveAsParquet(TestSaveArgs(outputPath))
    val reloadedReads = sc.loadAlignments(outputPath)
    ==(reads.rdd.count, reloadedReads.rdd.count)
  }

  test("read a HLA fasta from GRCh38") {
    val inputPath = testFile("HLA_DQB1_05_01_01_02.fa")
    val gRdd = sc.loadFasta(inputPath, 10000L)
    ==(gRdd.sequences.records.size, 1)
    ==(gRdd.sequences.records.head.name, "HLA-DQB1*05:01:01:02")
    val fragments = gRdd.rdd.collect
    ==(fragments.length, 1)
    ==(fragments.head.getContig.getContigName, "HLA-DQB1*05:01:01:02")
  }

  test("read a gzipped fasta file") {
    val inputPath = testFile("chr20.250k.fa.gz")
    val contigFragments: RDD[NucleotideContigFragment] = sc.loadFasta(inputPath, 10000L)
      .rdd
      .sortBy(_.getFragmentNumber.toInt)
    ==(contigFragments.rdd.count(), 26)
    val first: NucleotideContigFragment = contigFragments.first()
    ==(first.getContig.getContigName, null)
    ==(first.getDescription, "gi|224384749|gb|CM000682.1| Homo sapiens chromosome 20, GRCh37 primary reference assembly")
    ==(first.getFragmentNumber, 0)
    ==(first.getFragmentSequence.length, 10000)
    ==(first.getFragmentStartPosition, 0L)
    ==(first.getFragmentEndPosition, 9999L)
    ==(first.getNumberOfFragmentsInContig, 26)

    // 250k file actually has 251930 bases
    val last: NucleotideContigFragment = contigFragments.rdd.collect().last
    ==(last.getFragmentNumber, 25)
    ==(last.getFragmentStartPosition, 250000L)
    ==(last.getFragmentEndPosition, 251929L)
  }

  test("loadIndexedBam with 1 ReferenceRegion") {
    val refRegion = ReferenceRegion("2", 100, 101)
    val path = testFile("indexed_bams/sorted.bam")
    val reads = sc.loadIndexedBam(path, refRegion)
    assert(reads.rdd.count == 1)
  }

  test("loadIndexedBam with multiple ReferenceRegions") {
    val refRegion1 = ReferenceRegion("2", 100, 101)
    val refRegion2 = ReferenceRegion("3", 10, 17)
    val path = testFile("indexed_bams/sorted.bam")
    val reads = sc.loadIndexedBam(path, Iterable(refRegion1, refRegion2))
    assert(reads.rdd.count == 2)
  }

  lazy val indexedBamDir = testFile("indexed_bams")
  lazy val indexedBamGlob = indexedBamDir / "*.bam"
  lazy val sortedBam = indexedBamDir / "sorted.bam"

  test("loadIndexedBam with multiple ReferenceRegions and indexed bams") {
    val refRegion1 = ReferenceRegion("2", 100, 101)
    val refRegion2 = ReferenceRegion("3", 10, 17)
    val reads = sc.loadIndexedBam(indexedBamGlob, Iterable(refRegion1, refRegion2))
    assert(reads.rdd.count == 4)
  }

  test("loadIndexedBam with multiple ReferenceRegions and a directory of indexed bams") {
    val refRegion1 = ReferenceRegion("2", 100, 101)
    val refRegion2 = ReferenceRegion("3", 10, 17)
    val reads = sc.loadIndexedBam(indexedBamDir, Iterable(refRegion1, refRegion2))
    assert(reads.rdd.count == 4)
  }

  test("loadBam with a glob") {
    val path = indexedBamGlob
    val reads = sc.loadBam(path)
    assert(reads.rdd.count == 10)
  }

  test("loadBam with a directory") {
    val reads = sc.loadBam(indexedBamDir)
    assert(reads.rdd.count == 10)
  }

  test("load vcf with a glob") {
    val path = testFile("bqsr1.vcf").parent / "*.vcf"

    val variants = sc.loadVcf(path).toVariantRDD
    ==(variants.rdd.count, 722)
  }

  test("load vcf from a directory") {
    val path = testFile("vcf_dir")

    val variants = sc.loadVcf(path).toVariantRDD
    ==(variants.rdd.count, 681)
  }

  lazy val gvcfDir = testFile("gvcf_dir")

  test("load gvcf which contains a multi-allelic row from a directory") {
    val variants = sc.loadVcf(gvcfDir).toVariantRDD
    ==(variants.rdd.count, 6)
  }

  test("parse annotations for multi-allelic rows") {
    val variants = sc.loadVcf(gvcfDir).toVariantRDD
    val multiAllelicVariants = variants.rdd
      .filter(_.getReferenceAllele == "TAAA")
      .sortBy(_.getAlternateAllele.length)
      .collect()

    val mleCounts = multiAllelicVariants.map(_.getAnnotation.getAttributes.get("MLEAC"))
    //ALT    T,TA,TAA,<NON_REF>
    //MLEAC  0,1,1,0
    ==(mleCounts, Array("0", "1", "1"))
  }

  test("load parquet with globs") {
    val inputPath = testFile("small.sam")
    val reads = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".adam")
    reads.saveAsParquet(outputPath)

    val parent = outputPath.parent
    val basename = outputPath.basename
    reads.saveAsParquet(parent / basename.replace(".adam", ".2.adam"))

    val globPath = parent / basename.replace(".adam", "*.adam") / "*"

    val reloadedReads = sc.loadParquetAlignments(globPath)

    ==(2 * reads.rdd.count, reloadedReads.rdd.count)
  }

  test("bad glob should fail") {
    val inputPath = testFile("small.sam").parent / "*.sad"
    intercept[FileNotFoundException] {
      sc.loadAlignments(inputPath)
    }
  }

  test("empty directory should fail") {
    val inputPath = tmpLocation()
    intercept[FileNotFoundException] {
      sc.loadAlignments(inputPath)
    }
  }

  test("can read a SnpEff-annotated .vcf file") {
    val path = testFile("small_snpeff.vcf")
    val variantRdd = sc.loadVariants(path)
    val variants =
      variantRdd
        .rdd
        .sortBy(_.getStart)
        .collect

    variants.foreach(v => v.getStart.longValue match {
      case 14396L ⇒
        ==(v.getReferenceAllele, "CTGT")
        ==(v.getAlternateAllele, "C")
        ==(v.getAnnotation.getTranscriptEffects.size, 4)

        val te = v.getAnnotation.getTranscriptEffects.get(0)
        ==(te.getAlternateAllele, "C")
        assert(te.getEffects.contains("downstream_gene_variant"))
        ==(te.getGeneName, "WASH7P")
        ==(te.getGeneId, "ENSG00000227232")
        ==(te.getFeatureType, "transcript")
        ==(te.getFeatureId, "ENST00000488147.1")
        ==(te.getBiotype, "unprocessed_pseudogene")
      case 14521L ⇒
        ==(v.getReferenceAllele, "G")
        ==(v.getAlternateAllele, "A")
        ==(v.getAnnotation.getTranscriptEffects.size, 4)
      case 19189L ⇒
        ==(v.getReferenceAllele, "GC")
        ==(v.getAlternateAllele, "G")
        ==(v.getAnnotation.getTranscriptEffects.size, 3)
      case 63734L ⇒
        ==(v.getReferenceAllele, "CCTA")
        ==(v.getAlternateAllele, "C")
        ==(v.getAnnotation.getTranscriptEffects.size, 1)
      case 752720L ⇒
        ==(v.getReferenceAllele, "A")
        ==(v.getAlternateAllele, "G")
        ==(v.getAnnotation.getTranscriptEffects.size, 2)
      case _ ⇒
        fail("unexpected variant start " + v.getStart)
    })
  }

  test("loadAlignments should not fail on single-end and paired-end fastq reads") {
    val readsFilepath1 = testFile("bqsr1-r1.fq")
    val readsFilepath2 = testFile("bqsr1-r2.fq")
    val fastqReads1: RDD[AlignmentRecord] = sc.loadAlignments(readsFilepath1).rdd
    val fastqReads2: RDD[AlignmentRecord] = sc.loadAlignments(readsFilepath2).rdd
    val pairedReads: RDD[AlignmentRecord] = sc.loadAlignments(readsFilepath1, path2Opt = Option(readsFilepath2)).rdd
    ==(fastqReads1.rdd.count, 488)
    ==(fastqReads2.rdd.count, 488)
    ==(pairedReads.rdd.count, 976)
  }
}
