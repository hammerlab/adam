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

import htsjdk.samtools.{ SAMFileHeader, SAMSequenceDictionary }
import htsjdk.variant.vcf.VCFHeader
import org.bdgenomics.formats.avro.Contig
import org.hammerlab.genomics.reference.ContigName.Factory
import org.hammerlab.genomics.reference.{ ContigLengths, ContigName, NumLoci }

import scala.collection.JavaConversions.{ asScalaIterator, seqAsJavaList }

/**
 * Singleton object for creating SequenceDictionaries.
 */
object SequenceDictionary {

  val orderByName = Ordering.by[SequenceRecord, ContigName](_.name)

  val orderByRefIdx =
    new Ordering[SequenceRecord] {
      def compare(a: SequenceRecord, b: SequenceRecord): Int =
        (a.referenceIndex, b.referenceIndex) match {
          case (Some(aRefIdx), Some(bRefIdx)) ⇒ aRefIdx.compare(bRefIdx)
          case _ ⇒ throw new Exception(s"Missing reference index when comparing SequenceRecords: $a, $b")
        }
    }

  /**
   * @return Creates a new, empty SequenceDictionary.
   */
  def empty: SequenceDictionary = new SequenceDictionary()

  /**
   * Builds a sequence dictionary for a variable length collection of records.
   *
   * @param records Records to include in the dictionary.
   * @return A sequence dictionary containing these records.
   */
  def apply(records: SequenceRecord*): SequenceDictionary = new SequenceDictionary(records.toVector)

  /**
   * Builds a sequence dictionary from an htsjdk SAMSequenceDictionary.
   *
   * @param dict Htsjdk sequence dictionary to build from.
   * @return A SequenceDictionary with populated sequence records.
   */
  def apply(dict: SAMSequenceDictionary)(implicit factory: Factory): SequenceDictionary = {
    new SequenceDictionary(dict.getSequences.iterator().map(SequenceRecord.fromSAMSequenceRecord).toVector)
  }

  /**
   * Makes a SequenceDictionary from a SAMFileHeader.
   *
   * @param header htsjdk SAMFileHeader to extract sequences from.
   * @return A SequenceDictionary with populated sequence records.
   */
  def apply(header: SAMFileHeader)(implicit factory: Factory): SequenceDictionary = {
    SequenceDictionary(header.getSequenceDictionary)
  }

  /**
   * Creates a sequence dictionary from a sequence of Avro Contigs.
   *
   * @param contigs Seq of Contig records.
   * @return Returns a sequence dictionary.
   */
  def fromAvro(contigs: Seq[Contig])(implicit factory: Factory): SequenceDictionary = {
    new SequenceDictionary(contigs.map(SequenceRecord.fromADAMContig).toVector)
  }

  /**
   * Extracts a SAM sequence dictionary from a VCF header and returns an
   * ADAM sequence dictionary.
   *
   * @see fromSAMHeader
   *
   * @param header VCF file header.
   * @return Returns an ADAM style sequence dictionary.
   */
  def fromVCFHeader(header: VCFHeader)(implicit factory: Factory): SequenceDictionary = {
    val samDict = header.getSequenceDictionary

    // vcf files can have null sequence dictionaries
    Option(samDict).fold(SequenceDictionary.empty)(apply)
  }

  /**
   * Converts a picard/samtools SAMSequenceDictionary into an ADAM sequence dictionary.
   *
   * @see fromSAMHeader
   * @see fromVCFHeader
   *
   * @param samDict SAM style sequence dictionary.
   * @return Returns an ADAM style sequence dictionary.
   */
  def fromSAMSequenceDictionary(samDict: SAMSequenceDictionary)(implicit factory: Factory): SequenceDictionary = {
    val samDictRecords = samDict.getSequences
    new SequenceDictionary(samDictRecords.iterator().map(SequenceRecord.fromSAMSequenceRecord).toVector)
  }
}

/**
 * A SequenceDictionary contains metadata about the reference build genomic data
 * is aligned against.
 *
 * @see SequenceRecord
 *
 * @param records The individual reference contigs.
 */
case class SequenceDictionary(records: Vector[SequenceRecord] = Vector()) {

  @transient private lazy val byName: Map[ContigName, SequenceRecord] =
    records
      .view
      .map(r => r.name →  r)
      .toMap

  assert(
    byName.size == records.length,
    Seq(
      "SequenceRecords with duplicate names aren't permitted:",
      records.map(_.name).mkString("\t", "\n\t", "\n")
    )
    .mkString("\n")
  )

  private val hasSequenceOrdering = records.forall(_.referenceIndex.isDefined)

  def contigLengths: ContigLengths = byName.mapValues(r ⇒ NumLoci(r.length)).toMap

  /**
   * @param that Sequence dictionary to compare against.
   * @return True if each record in this dictionary exists in the other dictionary.
   */
  def isCompatibleWith(that: SequenceDictionary): Boolean =
    !that
      .records
      .exists(
        record ⇒
          byName
            .get(record.name)
            .exists(_ != record)
      )

  /**
   * @param name The name of the contig to extract.
   * @return If available, the sequence record for this contig.
   */
  def apply(name: ContigName): Option[SequenceRecord] = byName.get(name)

  /**
   * Checks to see if we have a contig with a given name.
   *
   * @param name The name of the contig to extract.
   * @return True if we have a sequence record for this contig.
   */
  def contains(name: ContigName): Boolean = byName.contains(name)

  /**
   * Adds a sequence record to this dictionary.
   *
   * @param record The sequence record to add.
   * @return A new sequence dictionary with the new record added.
   */
  def +(record: SequenceRecord): SequenceDictionary = this ++ SequenceDictionary(record)

  /**
   * Merges two sequence dictionaries.
   *
   * Filters any sequence records that exist in both dictionaries.
   *
   * @param that The sequence dictionary to add.
   * @return A new sequence dictionary that contains a record per contig in each
   *   input dictionary.
   */
  def ++(that: SequenceDictionary): SequenceDictionary =
    new SequenceDictionary(
      records ++
        that
          .records
          .filter(r => !byName.contains(r.name))
    )

  /**
   * Converts this ADAM style sequence dictionary into a SAM style sequence dictionary.
   *
   * @return Returns a SAM formatted sequence dictionary.
   */
  def toSAMSequenceDictionary: SAMSequenceDictionary =
    new SAMSequenceDictionary(
      records
        .iterator
        .map(_.toSAMSequenceRecord)
        .toList
    )

  /**
   * Strips indices from a Sequence Dictionary.
   *
   * @return This returns a new sequence dictionary devoid of indices. This is
   *   important for sorting: the default sort in ADAM is based on a lexical
   *   ordering, while the default sort in SAM is based on sequence indices. If
   *   the indices are not stripped before a file is saved back to SAM/BAM, the
   *   SAM/BAM header sequence ordering will not match the sort order of the
   *   records in the file.
   *
   * @see sorted
   */
  def stripIndices: SequenceDictionary =
    new SequenceDictionary(records.map(_.stripIndex))

  /**
   * Sort the records in a sequence dictionary.
   *
   * @return Returns a new sequence dictionary where the sequence records are
   *   sorted. If the sequence records have indices, the records will be sorted
   *   by their indices. If not, the sequence records will be sorted lexically
   *   by contig name.
   *
   * @see stripIndices
   */
  def sorted: SequenceDictionary = {
    implicit val ordering =
      if (hasSequenceOrdering)
        SequenceDictionary.orderByRefIdx
      else
        SequenceDictionary.orderByName

    new SequenceDictionary(records.sorted)
  }

  override def toString: String =
    s"SequenceDictionary(${records.mkString("\n\t", "\n\t", "\n")})"

  private[adam] def toAvro: Seq[Contig] = {
    records.map(_.toADAMContig)
  }

  /**
   * @return True if this dictionary contains no sequence records.
   */
  def isEmpty: Boolean = records.isEmpty
}

