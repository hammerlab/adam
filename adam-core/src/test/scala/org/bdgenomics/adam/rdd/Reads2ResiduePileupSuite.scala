package org.bdgenomics.adam.rdd

import org.bdgenomics.adam.avro.{ ADAMContig, Base, ADAMRecord }
import org.bdgenomics.adam.util.SparkFunSuite
import scala.collection.JavaConversions._

class Reads2ResiduePileupSuite extends SparkFunSuite {

  sparkTest("can convert a single read with only matches") {
    val quals = List(30, 20, 40, 20, 10)
    val qualString: String = quals.map(p => (p + 33).toChar.toString).fold("")(_ + _)
    val sequence = "ACTAG"

    // build a read with 5 base pairs, all match
    val records = sc.parallelize(Seq(ADAMRecord.newBuilder()
      .setContig(ADAMContig.newBuilder.setContigName("chr1").build)
      .setStart(1L)
      .setMapq(30)
      .setSequence(sequence)
      .setCigar("5M")
      .setMismatchingPositions("5")
      .setQual(qualString)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .build()))

    val converter = new Reads2ResiduePileup

    // convert pileups
    val pileups = converter.readToPileups(records).collect

    assert(pileups.length === 5)
    assert(pileups.sortBy(_.getPosition).flatMap(p => p.getResidues.map(_.getReadBase.toString)).fold("")(_ + _) === sequence)
    assert(pileups.sortBy(_.getPosition).flatMap(p => p.getResidues.map(_.getSangerQuality)).toList === quals)

    assert(pileups.forall(_.getCountAtPosition == 1))

    pileups.flatMap(_.getResidues).forall(b => b.getReadBase.toString == b.getReferenceBase.toString)

    assert(pileups.flatMap(p => p.getResidues.map(_.getMapQuality)).forall(_ == 30))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadStart)).forall(_ == 1L))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadEnd)).forall(_ == 6L))

  }

  sparkTest("can convert a single read with insertion") {
    val quals = List(30, 20, 40, 20, 10)
    val qualString: String = quals.map(p => (p + 33).toChar.toString).fold("")(_ + _)
    val sequence = "ACTAG"

    // build a read with 5 base pairs, all match
    val records = sc.parallelize(Seq(ADAMRecord.newBuilder()
      .setContig(ADAMContig.newBuilder.setContigName("chr1").build)
      .setStart(1L)
      .setMapq(30)
      .setSequence(sequence)
      .setCigar("3M1I1M")
      .setMismatchingPositions("4")
      .setQual(qualString)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .build()))

    val converter = new Reads2ResiduePileup

    // convert pileups
    val pileups = converter.readToPileups(records).collect

    assert(pileups.length === 4)

    assert(pileups.flatMap(p => p.getResidues.map(_.getMapQuality)).forall(_ == 30))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadStart)).forall(_ == 1L))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadEnd)).forall(_ == 5L))

  }

  sparkTest("can convert a single read with deletion") {
    val quals = List(30, 20, 40, 10)
    val qualString: String = quals.map(p => (p + 33).toChar.toString).fold("")(_ + _)
    val sequence = "ACTG"

    // build a read with 5 base pairs, all match
    val records = sc.parallelize(Seq(ADAMRecord.newBuilder()
      .setContig(ADAMContig.newBuilder.setContigName("chr1").build)
      .setStart(1L)
      .setMapq(30)
      .setSequence(sequence)
      .setCigar("3M1D1M")
      .setMismatchingPositions("3^A1")
      .setQual(qualString)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .build()))

    val converter = new Reads2ResiduePileup

    // convert pileups
    val pileups = converter.readToPileups(records).collect

    assert(pileups.length === 5)
    assert(pileups.sortBy(_.getPosition).flatMap(p => p.getResidues.filter(_.getReadBase != null).map(_.getReadBase.toString)).fold("")(_ + _) === sequence)
    assert(pileups.sortBy(_.getPosition).flatMap(p => p.getResidues.filter(_.getReadBase != null).map(_.getSangerQuality)).toList === quals)

    assert(pileups.forall(_.getCountAtPosition == 1))

    pileups.flatMap(_.getResidues.filter(_.getReadBase != null)).forall(b => b.getReadBase.toString == b.getReferenceBase.toString)

    assert(pileups.flatMap(p => p.getResidues.map(_.getMapQuality)).forall(_ == 30))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadStart)).forall(_ == 1L))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadEnd)).forall(_ == 6L))

  }

  sparkTest("can convert a single read with only matches; use optimized partitioning") {
    val quals = List(30, 20, 40, 20, 10)
    val qualString: String = quals.map(p => (p + 33).toChar.toString).fold("")(_ + _)
    val sequence = "ACTAG"

    // build a read with 5 base pairs, all match
    val records = sc.parallelize(Seq(ADAMRecord.newBuilder()
      .setContig(ADAMContig.newBuilder.setContigName("chr1").build)
      .setStart(1L)
      .setMapq(30)
      .setSequence(sequence)
      .setCigar("5M")
      .setMismatchingPositions("5")
      .setQual(qualString)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .build()))

    val converter = new Reads2ResiduePileup

    // convert pileups
    val pileups = converter.readToPartitionedPileups(records).collect

    assert(pileups.length === 5)
    assert(pileups.sortBy(_.getPosition).flatMap(p => p.getResidues.map(_.getReadBase.toString)).fold("")(_ + _) === sequence)
    assert(pileups.sortBy(_.getPosition).flatMap(p => p.getResidues.map(_.getSangerQuality)).toList === quals)

    assert(pileups.forall(_.getCountAtPosition == 1))

    pileups.flatMap(_.getResidues).forall(b => b.getReadBase.toString == b.getReferenceBase.toString)

    assert(pileups.flatMap(p => p.getResidues.map(_.getMapQuality)).forall(_ == 30))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadStart)).forall(_ == 1L))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadEnd)).forall(_ == 6L))

  }

  sparkTest("can convert a single read with matches and mismatches") {
    val quals = List(30, 20, 40, 20, 10)
    val qualString: String = quals.map(p => (p + 33).toChar.toString).fold("")(_ + _)
    val sequence = "ACTAG"

    // build a read with 5 base pairs, all match
    val record = sc.parallelize(Seq(ADAMRecord.newBuilder()
      .setContig(ADAMContig.newBuilder.setContigName("chr1").build)
      .setStart(1L)
      .setMapq(30)
      .setSequence(sequence)
      .setCigar("5M")
      .setMismatchingPositions("4A0")
      .setQual(qualString)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .build()))

    val converter = new Reads2ResiduePileup

    // convert pileups
    val pileups = converter.readToPileups(record).collect

    assert(pileups.length === 5)
    assert(pileups.sortBy(_.getPosition).flatMap(p => p.getResidues.map(_.getReadBase.toString)).fold("")(_ + _) === sequence)
    assert(pileups.sortBy(_.getPosition).flatMap(p => p.getResidues.map(_.getSangerQuality)).toList === quals)

    assert(pileups.forall(_.getCountAtPosition == 1))

    assert(pileups.filter(_.getPosition < 5L).flatMap(_.getResidues).forall(b => b.getReadBase.toString == b.getReferenceBase.toString))

    assert(pileups.filter(_.getPosition == 5L).flatMap(_.getResidues).forall(b => b.getReadBase.toString != b.getReferenceBase.toString))

    assert(pileups.flatMap(p => p.getResidues.map(_.getMapQuality)).forall(_ == 30))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadStart)).forall(_ == 1L))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadEnd)).forall(_ == 6L))
  }

  sparkTest("can convert a single read with matches and mismatches; use optimized partitioning") {
    val quals = List(30, 20, 40, 20, 10)
    val qualString: String = quals.map(p => (p + 33).toChar.toString).fold("")(_ + _)
    val sequence = "ACTAG"

    // build a read with 5 base pairs, all match
    val record = sc.parallelize(Seq(ADAMRecord.newBuilder()
      .setContig(ADAMContig.newBuilder.setContigName("chr1").build)
      .setStart(1L)
      .setMapq(30)
      .setSequence(sequence)
      .setCigar("5M")
      .setMismatchingPositions("4A0")
      .setQual(qualString)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .build()))

    val converter = new Reads2ResiduePileup

    // convert pileups
    val pileups = converter.readToPartitionedPileups(record).collect

    assert(pileups.length === 5)
    assert(pileups.sortBy(_.getPosition).flatMap(p => p.getResidues.map(_.getReadBase.toString)).fold("")(_ + _) === sequence)
    assert(pileups.sortBy(_.getPosition).flatMap(p => p.getResidues.map(_.getSangerQuality)).toList === quals)

    assert(pileups.forall(_.getCountAtPosition == 1))

    assert(pileups.filter(_.getPosition < 5L).flatMap(_.getResidues).forall(b => b.getReadBase.toString == b.getReferenceBase.toString))
    assert(pileups.filter(_.getPosition == 5L).flatMap(_.getResidues).forall(b => b.getReadBase.toString != b.getReferenceBase.toString))

    assert(pileups.flatMap(p => p.getResidues.map(_.getMapQuality)).forall(_ == 30))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadStart)).forall(_ == 1L))
    assert(pileups.flatMap(p => p.getResidues.map(_.getReadEnd)).forall(_ == 6L))
  }

}
