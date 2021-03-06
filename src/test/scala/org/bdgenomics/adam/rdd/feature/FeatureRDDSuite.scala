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
package org.bdgenomics.adam.rdd.feature

import com.google.common.collect.ImmutableMap
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ Feature, Strand }
import org.hammerlab.genomics.reference.test.ClearContigNames
import org.scalactic.{ Equivalence, TypeCheckedTripleEquals }

class FeatureRDDSuite
  extends ADAMFunSuite
    with TypeCheckedTripleEquals {

  implicit val strongFeatureEq = new Equivalence[Feature] {
    def areEquivalent(a: Feature, b: Feature): Boolean = {
      a.getContigName === b.getContigName &&
        a.getStart === b.getStart &&
        a.getEnd === b.getEnd &&
        a.getStrand === b.getStrand &&
        a.getFeatureId === b.getFeatureId &&
        a.getName === b.getName &&
        a.getFeatureType === b.getFeatureType &&
        a.getSource === b.getSource &&
        a.getPhase === b.getPhase &&
        a.getFrame === b.getFrame &&
        a.getScore === b.getScore &&
        a.getGeneId === b.getGeneId &&
        a.getTranscriptId === b.getTranscriptId &&
        a.getExonId === b.getExonId &&
        a.getTarget === b.getTarget &&
        a.getGap === b.getGap &&
        a.getDerivesFrom === b.getDerivesFrom &&
        a.getCircular === b.getCircular &&
        a.getAliases === b.getAliases &&
        a.getNotes === b.getNotes &&
        a.getParentIds === b.getParentIds &&
        a.getDbxrefs === b.getDbxrefs &&
        a.getOntologyTerms === b.getOntologyTerms &&
        a.getAttributes === b.getAttributes
    }
  }

  test("round trip GTF format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)

    val firstGtfRecord = FeatureRDD.toGtf(features.rdd.first)

    val gtfSplitTabs = firstGtfRecord.split('\t')
    ==(gtfSplitTabs.length, 9)
    ==(gtfSplitTabs(0), "1")
    ==(gtfSplitTabs(1), "pseudogene")
    ==(gtfSplitTabs(2), "gene")
    ==(gtfSplitTabs(3), "11869")
    ==(gtfSplitTabs(4), "14412")
    ==(gtfSplitTabs(5), ".")
    ==(gtfSplitTabs(6), "+")
    ==(gtfSplitTabs(7), ".")

    val gtfAttributes = gtfSplitTabs(8).split(";").map(_.trim)
    ==(gtfAttributes.length, 4)
    ==(gtfAttributes(0), "gene_id \"ENSG00000223972\"")
    ==(gtfAttributes(1), "gene_biotype \"pseudogene\"")
    // gene name/source move to the end
    ==(gtfAttributes(2), "gene_name \"DDX11L1\"")
    ==(gtfAttributes(3), "gene_source \"ensembl_havana\"")

    val outputPath = tmpLocation(".gtf")
    features.saveAsGtf(outputPath, asSingleFile = true)
    val reloadedFeatures = sc.loadGtf(outputPath)
    ==(reloadedFeatures.rdd.count, features.rdd.count)
    val zippedFeatures = reloadedFeatures.rdd.zip(features.rdd).collect
    zippedFeatures.foreach { p ⇒
      p._1 should be(p._2)
    }
  }

  test("save GTF as GFF3 format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tmpLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save GTF as BED format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tmpLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save GTF as IntervalList format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tmpLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save GTF as NarrowPeak format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tmpLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save GFF3 as GTF format") {
    val inputPath = testFile("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tmpLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save GFF3 as BED format") {
    val inputPath = testFile("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tmpLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save GFF3 as IntervalList format") {
    val inputPath = testFile("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tmpLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save GFF3 as NarrowPeak format") {
    val inputPath = testFile("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tmpLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("round trip GFF3 format") {
    val inputPath = testFile("dvl1.200.gff3")
    val expected = sc.loadGff3(inputPath)
    val outputPath = tmpLocation(".gff3")
    expected.saveAsGff3(outputPath, asSingleFile = true)

    val feature = expected.rdd.first
    val gff3Columns = FeatureRDD.toGff3(feature).split('\t')
    ==(gff3Columns.length, 9)
    ==(gff3Columns(0), "1")
    ==(gff3Columns(1), "Ensembl")
    ==(gff3Columns(2), "gene")
    ==(gff3Columns(3), "1331314")
    ==(gff3Columns(4), "1335306")
    ==(gff3Columns(5), ".")
    ==(gff3Columns(6), "+")
    ==(gff3Columns(7), ".")
    val attrs = gff3Columns(8).split(';')
    ==(attrs.length, 3)
    ==(attrs(0), "ID=ENSG00000169962")
    ==(attrs(1), "Name=ENSG00000169962")
    ==(attrs(2), "biotype=protein_coding")

    val actual = sc.loadGff3(outputPath)
    val pairs = expected.rdd.collect.zip(actual.rdd.collect)
    pairs.foreach { p ⇒
      p._1 should be(p._2)
    }
  }

  test("save BED as GTF format") {
    val inputPath = testFile("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tmpLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save BED as GFF3 format") {
    val inputPath = testFile("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tmpLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save BED as IntervalList format") {
    val inputPath = testFile("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tmpLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save BED as NarrowPeak format") {
    val inputPath = testFile("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tmpLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("round trip BED format") {
    val inputPath = testFile("dvl1.200.bed")
    val expected = sc.loadBed(inputPath)
    val outputPath = tmpLocation(".bed")
    expected.saveAsBed(outputPath, asSingleFile = true)

    val feature = expected.rdd.first
    val bedCols = FeatureRDD.toBed(feature).split('\t')
    ==(bedCols.length, 6)
    ==(bedCols(0), "1")
    ==(bedCols(1), "1331345")
    ==(bedCols(2), "1331536")
    ==(bedCols(3), "106624")
    ==(bedCols(4), "13.53")
    ==(bedCols(5), "+")

    val actual = sc.loadBed(outputPath)
    val pairs = expected.rdd.collect.zip(actual.rdd.collect)
    pairs.foreach { p ⇒
      p._1 should be(p._2)
    }
  }

  test("save IntervalList as GTF format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tmpLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save IntervalList as GFF3 format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tmpLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save IntervalList as BED format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tmpLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save IntervalList as IntervalList format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tmpLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  test("save IntervalList as NarrowPeak format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tmpLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("round trip IntervalList format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val expected = sc.loadIntervalList(inputPath)

    // test single record
    val feature = expected.rdd.first
    val interval = FeatureRDD.toInterval(feature).split('\t')
    ==(interval.length, 5)
    ==(interval(0), "chr1")
    ==(interval(1), "14416")
    ==(interval(2), "14499")
    ==(interval(3), "+")
    ==(interval(4), "gn|DDX11L1;gn|RP11-34P13.2;ens|ENSG00000223972;ens|ENSG00000227232;vega|OTTHUMG00000000958;vega|OTTHUMG00000000961")

    // test a record with a refseq attribute
    val refseqFeature =
      expected
        .rdd
        .filter { f ⇒
          f.getContigName == "chr7" &&
            f.getStart == 142111441L &&
            f.getEnd == 142111617L
        }
        .first

    val rsInterval = FeatureRDD.toInterval(refseqFeature).split('\t')
    rsInterval should be(
      Array(
        "chr7",
        "142111442",
        "142111617",
        "+",
        "gn|TRBV5-7;ens|ENSG00000211731;refseq|NG_001333"
      )
    )

    val outputPath = tmpLocation(".interval_list")
    expected.saveAsIntervalList(outputPath, asSingleFile = true)

    val actual = sc.loadIntervalList(outputPath)
    val pairs = expected.rdd.collect.zip(actual.rdd.collect)
    pairs.foreach { p ⇒
      p._1 should be(p._2)
    }
  }

  test("save NarrowPeak as GTF format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tmpLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save NarrowPeak as GFF3 format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tmpLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save NarrowPeak as BED format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tmpLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save NarrowPeak as IntervalList format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tmpLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    ==(features.rdd.count, reloadedFeatures.rdd.count)
  }

  test("save NarrowPeak as NarrowPeak format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tmpLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  test("round trip NarrowPeak format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val expected = sc.loadNarrowPeak(inputPath)
    val outputPath = tmpLocation(".narrowPeak")
    expected.saveAsNarrowPeak(outputPath, asSingleFile = true)

    val feature = expected.rdd.first
    val npColumns = FeatureRDD.toNarrowPeak(feature).split('\t')
    ==(npColumns.length, 10)
    ==(npColumns(0), "chr1")
    ==(npColumns(1), "713849")
    ==(npColumns(2), "714434")
    ==(npColumns(3), "chr1.1")
    ==(npColumns(4), "1000")
    ==(npColumns(5), ".")
    ==(npColumns(6), "0.2252")
    ==(npColumns(7), "9.16")
    ==(npColumns(8), "-1")
    ==(npColumns(9), "263")

    val actual = sc.loadNarrowPeak(outputPath)
    val pairs = expected.rdd.zip(actual.rdd).collect
    pairs.foreach { p ⇒
      p._1 should be(p._2)
    }
  }

  test("sort by reference") {
    val fb = Feature.newBuilder()
    val f1 = fb.setContigName("1").setStart(1L).setEnd(100L).build()
    val f2 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.FORWARD).build()
    val f3 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.REVERSE).build()
    val f4 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.INDEPENDENT).build()
    val f5 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.UNKNOWN).build()
    val f6 = fb.setContigName("1").setStart(10L).setEnd(110L).clearStrand().build() // null strand last
    val f7 = fb.setContigName("2").build()

    val features = FeatureRDD.inferSequenceDictionary(sc.parallelize(Seq(f7, f6, f5, f4, f3, f2, f1)), optStorageLevel = None)
    val sorted = features.sortByReference().rdd.collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
    assert(f6 == sorted(5))
    assert(f7 == sorted(6))
  }

  test("sort by reference and feature fields") {
    val fb = Feature.newBuilder().setContigName("1").setStart(1L).setEnd(100L)
    val f1 = fb.setFeatureId("featureId").build()
    val f2 = fb.clearFeatureId().setName("name").build()
    val f3 = fb.clearName().setPhase(0).build()
    val f4 = fb.setPhase(1).build() // Int defaults to increasing sort order
    val f5 = fb.clearPhase().setScore(0.1).build()
    val f6 = fb.setScore(0.9).build() // Double defaults to increasing sort order
    val f7 = fb.clearScore().build() // nulls last

    val features = FeatureRDD.inferSequenceDictionary(sc.parallelize(Seq(f7, f6, f5, f4, f3, f2, f1)), optStorageLevel = None)
    val sorted = features.sortByReference().rdd.collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
    assert(f6 == sorted(5))
    assert(f7 == sorted(6))
  }

  test("sort gene features by reference and gene structure") {
    val fb = Feature.newBuilder().setContigName("1").setStart(1L).setEnd(100L).setFeatureType("gene")
    val f1 = fb.setGeneId("gene1").build()
    val f2 = fb.setGeneId("gene2").build()
    val f3 = fb.clearGeneId().build() // nulls last

    val features = FeatureRDD.inferSequenceDictionary(sc.parallelize(Seq(f3, f2, f1)), optStorageLevel = None)
    val sorted = features.sortByReference().rdd.collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
  }

  test("sort transcript features by reference and gene structure") {
    val fb = Feature.newBuilder().setContigName("1").setStart(1L).setEnd(100L).setFeatureType("transcript")
    val f1 = fb.setGeneId("gene1").setTranscriptId("transcript1").build()
    val f2 = fb.setGeneId("gene1").setTranscriptId("transcript1").build()
    val f3 = fb.setGeneId("gene2").setTranscriptId("transcript1").build()
    val f4 = fb.setGeneId("gene2").setTranscriptId("transcript2").build()
    val f5 = fb.setGeneId("gene2").clearTranscriptId().build() // nulls last

    val features = FeatureRDD.inferSequenceDictionary(sc.parallelize(Seq(f5, f4, f3, f2, f1)), optStorageLevel = None)
    val sorted = features.sortByReference().rdd.collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
  }

  test("sort exon features by reference and gene structure") {
    val fb = Feature.newBuilder().setContigName("1").setStart(1L).setEnd(100L).setFeatureType("exon")
    val f1 = fb.setGeneId("gene1").setTranscriptId("transcript1").setExonId("exon1").build()
    val f2 = fb.setGeneId("gene1").setTranscriptId("transcript1").setExonId("exon2").build()
    val f3 = fb.setGeneId("gene1").setTranscriptId("transcript2").setExonId("exon1").build()
    val f4 = fb.setGeneId("gene2").setTranscriptId("transcript1").setExonId("exon1").build()
    val f5 = fb.setGeneId("gene2").setTranscriptId("transcript1").clearExonId().setAttributes(ImmutableMap.of("exon_number", "1")).build()
    val f6 = fb.setGeneId("gene2").setTranscriptId("transcript1").setAttributes(ImmutableMap.of("exon_number", "2")).build()
    val f7 = fb.setGeneId("gene2").setTranscriptId("transcript1").setAttributes(ImmutableMap.of("rank", "1")).build()
    val f8 = fb.setGeneId("gene2").setTranscriptId("transcript1").setAttributes(ImmutableMap.of("rank", "2")).build()
    val f9 = fb.setGeneId("gene2").setTranscriptId("transcript1").clearAttributes().build() // nulls last

    val features = FeatureRDD.inferSequenceDictionary(sc.parallelize(Seq(f9, f8, f7, f6, f5, f4, f3, f2, f1)), optStorageLevel = None)
    val sorted = features.sortByReference().rdd.collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
    assert(f6 == sorted(5))
    assert(f7 == sorted(6))
    assert(f8 == sorted(7))
    assert(f9 == sorted(8))
  }

  test("sort intron features by reference and gene structure") {
    val fb = Feature.newBuilder().setContigName("1").setStart(1L).setEnd(100L).setGeneId("gene1").setTranscriptId("transcript1").setFeatureType("intron")
    val f1 = fb.setAttributes(ImmutableMap.of("intron_number", "1")).build()
    val f2 = fb.setAttributes(ImmutableMap.of("intron_number", "2")).build()
    val f3 = fb.setAttributes(ImmutableMap.of("rank", "1")).build()
    val f4 = fb.setAttributes(ImmutableMap.of("rank", "2")).build()
    val f5 = fb.clearAttributes().build() // nulls last

    val features = FeatureRDD.inferSequenceDictionary(sc.parallelize(Seq(f5, f4, f3, f2, f1)), optStorageLevel = None)
    val sorted = features.sortByReference().rdd.collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
  }

  test("correctly flatmaps CoverageRDD from FeatureRDD") {
    val f1 = Feature.newBuilder().setContigName("chr1").setStart( 1).setEnd(10).setScore(3.0).build()
    val f2 = Feature.newBuilder().setContigName("chr1").setStart(15).setEnd(20).setScore(2.0).build()
    val f3 = Feature.newBuilder().setContigName("chr2").setStart(15).setEnd(20).setScore(2.0).build()

    val featureRDD: FeatureRDD = FeatureRDD.inferSequenceDictionary(sc.parallelize(Seq(f1, f2, f3)), optStorageLevel = None)
    val coverageRDD: CoverageRDD = featureRDD.toCoverage
    val coverage = coverageRDD.flatten

    assert(coverage.rdd.count == 19)
  }

  test("use broadcast join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = features.broadcastRegionJoin(targets)

    ==(jRdd.rdd.count, 5L)
  }

  test("use right outer broadcast join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = features.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    ==(c.count(_._1.isEmpty), 1)
    ==(c.count(_._1.isDefined), 5)
  }

  def sd = {
    sc.loadBam(testFile("small.1.sam"))
      .sequences
  }

  test("use shuffle join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform(_.repartition(1))
      .copy(sequences = sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = features.shuffleRegionJoin(targets)
    val jRdd0 = features.shuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    ==(jRdd.rdd.count, 5L)
    ==(jRdd0.rdd.count, 5L)
  }

  test("use right outer shuffle join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform(_.repartition(1))
      .copy(sequences = sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = features.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = features.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4))

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

  test("use left outer shuffle join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform(_.repartition(1))
      .copy(sequences = sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = features.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = features.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4))

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

  test("use full outer shuffle join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform(_.repartition(1))
      .copy(sequences = sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = features.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = features.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4))

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

  test("use shuffle join with group by to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform(_.repartition(1))
      .copy(sequences = sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = features.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = features.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

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

  test("use right outer shuffle join with group by to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform(_.repartition(1))
      .copy(sequences = sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = features.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = features.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

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

  test("estimate sequence dictionary contig lengths from GTF format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    // max(start,end) = 1 36081
    assert(features.sequences.contains("1"))
    assert(features.sequences.apply("1").isDefined)
    assert(features.sequences.apply("1").get.length >= 36081L)
  }

  test("estimate sequence dictionary contig lengths from GFF3 format") {
    val inputPath = testFile("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    // max(start, end) = 1 1356705
    assert(features.sequences.contains("1"))
    assert(features.sequences.apply("1").isDefined)
    assert(features.sequences.apply("1").get.length >= 1356705L)
  }

  test("estimate sequence dictionary contig lengths from BED format") {
    val inputPath = testFile("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    // max(start, end) = 1 1358504
    assert(features.sequences.contains("1"))
    assert(features.sequences.apply("1").isDefined)
    assert(features.sequences.apply("1").get.length >= 1358504L)
  }

  test("obtain sequence dictionary contig lengths from header in IntervalList format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    /*
@SQ	SN:chr1	LN:249250621
@SQ	SN:chr2	LN:243199373
     */
    assert(features.sequences.contains("chr1"))
    assert(features.sequences.apply("chr1").isDefined)
    assert(features.sequences.apply("chr1").get.length >= 249250621L)

    assert(features.sequences.contains("chr2"))
    assert(features.sequences.apply("chr2").isDefined)
    assert(features.sequences.apply("chr2").get.length >= 243199373L)
  }

  test("estimate sequence dictionary contig lengths from NarrowPeak format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    // max(start, end) = chr1 794336
    assert(features.sequences.contains("chr1"))
    assert(features.sequences.apply("chr1").isDefined)
    assert(features.sequences.apply("chr1").get.length >= 794336L)
  }

  test("don't lose any features when piping as BED format") {
    val inputPath = testFile("dvl1.200.bed")
    val frdd = sc.loadBed(inputPath)

    implicit val tFormatter = BEDInFormatter
    implicit val uFormatter = new BEDOutFormatter

    val pipedRdd: FeatureRDD = frdd.pipe("tee /dev/null")
    assert(pipedRdd.rdd.count >= frdd.rdd.count)
    ==(pipedRdd.rdd.distinct.count, frdd.rdd.distinct.count)
  }

  test("don't lose any features when piping as GTF format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val frdd = sc.loadGtf(inputPath)

    implicit val tFormatter = GTFInFormatter
    implicit val uFormatter = new GTFOutFormatter

    val pipedRdd: FeatureRDD = frdd.pipe("tee /dev/null")
    assert(pipedRdd.rdd.count >= frdd.rdd.count)
    ==(pipedRdd.rdd.distinct.count, frdd.rdd.distinct.count)
  }

  test("don't lose any features when piping as GFF3 format") {
    val inputPath = testFile("dvl1.200.gff3")
    val frdd = sc.loadGff3(inputPath)

    implicit val tFormatter = GFF3InFormatter
    implicit val uFormatter = new GFF3OutFormatter

    val pipedRdd: FeatureRDD = frdd.pipe("tee /dev/null")
    assert(pipedRdd.rdd.count >= frdd.rdd.count)
    ==(pipedRdd.rdd.distinct.count, frdd.rdd.distinct.count)
  }

  test("don't lose any features when piping as NarrowPeak format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val frdd = sc.loadNarrowPeak(inputPath)

    implicit val tFormatter = NarrowPeakInFormatter
    implicit val uFormatter = new NarrowPeakOutFormatter

    val pipedRdd: FeatureRDD = frdd.pipe("tee /dev/null")
    assert(pipedRdd.rdd.count >= frdd.rdd.count)
    ==(pipedRdd.rdd.distinct.count, frdd.rdd.distinct.count)
  }
}
