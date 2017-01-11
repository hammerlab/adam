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
package org.bdgenomics.adam.models

import java.io.File

import htsjdk.samtools.{ SAMSequenceDictionary, SAMSequenceRecord }
import htsjdk.variant.utils.SAMSequenceDictionaryExtractor
import htsjdk.variant.vcf.VCFFileReader
import org.bdgenomics.adam.util.ADAMFunSuite
import org.hammerlab.genomics.reference.test.{ ContigNameUtil, LocusUtil }
import org.hammerlab.genomics.reference.{ ContigName, NumLoci }
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.Matchers

import scala.collection.JavaConversions._

class SequenceDictionarySuite
  extends ADAMFunSuite
    with Matchers
    with ConversionCheckedTripleEquals
    with LocusUtil
    with ContigNameUtil {

  test("Convert from sam sequence record and back") {
    val sr = new SAMSequenceRecord("1", 1000)
    sr.setAttribute(SAMSequenceRecord.URI_TAG, "http://bigdatagenomics.github.io/1")

    val asASR: SequenceRecord = SequenceRecord.fromSAMSequenceRecord(sr)

    asASR.name should === ("1")
    asASR.length should === (1000L)
    asASR.url should === (Some("http://bigdatagenomics.github.io/1"))

    val asPSR: SAMSequenceRecord = asASR.toSAMSequenceRecord

    assert(sr.isSameSequence(asPSR))
  }

  test("Convert from SAM sequence dictionary file (with extra fields)") {
    val path = testFile("dict_with_accession.dict")
    val ssd = SAMSequenceDictionaryExtractor.extractDictionary(new File(path))

    val chr1 = ssd.getSequence("1") // Validate that extra fields are parsed
    assert(chr1 != null)
    val refseq = chr1.getAttribute("REFSEQ")
    refseq should === ("NC_000001.10")

    val asd = SequenceDictionary(ssd)
    assert(asd.containsRefName("1"))
    assert(!asd.containsRefName("2"))
  }

  test("merge into existing dictionary") {
    val path = testFile("dict_with_accession.dict")
    val ssd = SAMSequenceDictionaryExtractor.extractDictionary(new File(path))

    val asd = SequenceDictionary(ssd)
    assert(asd.containsRefName("1"))
    val chr1 = asd("1").get

    val myDict = SequenceDictionary(record(chr1.name, chr1.length, md5 = chr1.md5))
    assert(asd.isCompatibleWith(myDict))
    assert(myDict.isCompatibleWith(asd))
  }

  test("Convert from SAM sequence dictionary and back") {
    val path = testFile("dict_with_accession.dict")
    val ssd = SAMSequenceDictionaryExtractor.extractDictionary(new File(path))
    val asd = SequenceDictionary(ssd)
    ssd.assertSameDictionary(asd.toSAMSequenceDictionary)
  }

  test("Can retrieve sequence by name") {
    val rec = record("chr1")
    val asd = SequenceDictionary(rec)
    val recFromName = asd(rec.name)
    recFromName should === (Some(rec))
  }

  test("SequenceDictionary's with same single element are equal") {
    val asd1 = SequenceDictionary(record("chr1"))
    val asd2 = SequenceDictionary(record("chr1"))
    asd1 should === (asd2)
  }

  test("SequenceDictionary's with same two elements are equals") {
    val asd1 = SequenceDictionary(record("chr1"), record("chr2"))
    val asd2 = SequenceDictionary(record("chr1"), record("chr2"))
    asd1 should === (asd2)
  }

  test("SequenceDictionary's with different elements are unequal") {
    val asd1 = SequenceDictionary(record("chr1"), record("chr2"))
    val asd2 = SequenceDictionary(record("chr1"), record("chr3"))
    assert(asd1 != asd2)
  }

  test("SequenceDictionaries with same elements in different order are compatible") {
    val asd1 = SequenceDictionary(record("chr1"), record("chr2"))
    val asd2 = SequenceDictionary(record("chr2"), record("chr1"))
    assert(asd1.isCompatibleWith(asd2))
  }

  test("isCompatible tests equality on overlap") {
    val s1 = SequenceDictionary(record("foo"), record("bar"))
    val s2 = SequenceDictionary(record("bar"), record("quux"))
    val s3 = SequenceDictionary(record("foo"), record("bar"))
    val s4 = SequenceDictionary(record("foo", 1001))
    assert(s1.isCompatibleWith(s2))
    assert(s1 isCompatibleWith s3)
    assert(!(s3 isCompatibleWith s4))
  }

  test("The addition + works correctly") {
    val s1 = SequenceDictionary()
    val s2 = SequenceDictionary(record("foo"))
    val s3 = SequenceDictionary(record("foo"), record("bar"))

    s1 + record("foo") should === (s2)
    s2 + record("foo") should === (s2)
    s2 + record("bar") should === (s3)
  }

  test("The append operation ++ works correctly") {
    val s1 = SequenceDictionary()
    val s2a = SequenceDictionary(record("foo"))
    val s2b = SequenceDictionary(record("bar"))
    val s3 = SequenceDictionary(record("foo"), record("bar"))

    s1 ++ s1 should === (s1)
    s1 ++ s2a should === (s2a)
    s1 ++ s2b should === (s2b)
    s2a ++ s2b should === (s3)
  }

  test("ContainsRefName works correctly for different string types") {
    val dict = SequenceDictionary(record("chr0"),
      record("chr1"),
      record("chr2"),
      record("chr3"))
    val str0: String = "chr0"
    val str1: java.lang.String = "chr1"

    assert(dict.containsRefName(str0))
    assert(dict.containsRefName(str1))
  }

  test("Apply on name works correctly for different String types") {
    val dict = SequenceDictionary(
      record("chr0"),
      record("chr1"),
      record("chr2"),
      record("chr3"))
    val str0: String = "chr0"
    val str1: java.lang.String = "chr1"

    dict(str0).get.name should === ("chr0")
    dict(str1).get.name should === ("chr1")
  }

  def record(name: ContigName, length: NumLoci = 1000, md5: Option[String] = None): SequenceRecord =
    SequenceRecord(name, length).copy(md5 = md5)

  test("convert from sam sequence record and back") {
    val sr = new SAMSequenceRecord("chr0", 1000)
    sr.setAttribute(SAMSequenceRecord.URI_TAG, "http://bigdatagenomics.github.io/chr0")

    val conv = SequenceRecord.fromSAMSequenceRecord(sr)

    conv.name should === ("chr0")
    conv.length should === (1000L)
    conv.url.get should === ("http://bigdatagenomics.github.io/chr0")

    val convSr = conv.toSAMSequenceRecord

    assert(convSr.isSameSequence(sr))
  }

  test("convert from sam sequence dictionary and back") {
    val sr0 = new SAMSequenceRecord("chr0", 1000)

    val srs = List(sr0)

    val ssd = new SAMSequenceDictionary(srs)

    val asd = SequenceDictionary(ssd)

    val toSSD = asd.toSAMSequenceDictionary

    toSSD.assertSameDictionary(ssd)
  }

  test("conversion to sam sequence dictionary has correct sort order") {
    val sd =
      new SequenceDictionary(
        Vector(
          SequenceRecord("MT", 1000),
          SequenceRecord("4", 1000),
          SequenceRecord("1", 1000),
          SequenceRecord("3", 1000),
          SequenceRecord("2", 1000),
          SequenceRecord("X", 1000)
        )
      )
      .sorted

    val ssd = sd.toSAMSequenceDictionary
    val seq = ssd.getSequences
    seq.get(0).getSequenceName should === ("1")
    seq.get(1).getSequenceName should === ("2")
    seq.get(2).getSequenceName should === ("3")
    seq.get(3).getSequenceName should === ("4")
    seq.get(4).getSequenceName should === ("MT")
    seq.get(5).getSequenceName should === ("X")
  }

  test("load sequence dictionary from VCF file") {
    val path = testFile("small.vcf")
    val fileReader = new VCFFileReader(new File(path), false)
    val sd = SequenceDictionary.fromVCFHeader(fileReader.getFileHeader)

    sd.records.size should === (1)
    sd.records.head.name should === ("1")
  }

  test("empty sequence dictionary must be empty") {
    val sd = SequenceDictionary.empty
    sd.records.size should === (0)
    assert(sd.isEmpty)
  }
}
