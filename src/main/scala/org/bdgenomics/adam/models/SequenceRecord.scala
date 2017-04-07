package org.bdgenomics.adam.models

import htsjdk.samtools.SAMSequenceRecord
import org.bdgenomics.formats.avro.{ Contig, NucleotideContigFragment }
import org.hammerlab.genomics.reference.ContigName.Factory
import org.hammerlab.genomics.reference.{ ContigName, NumLoci }

/**
 * Metadata about a single reference contig.
 *
 * @param name The name of the contig.
 * @param length The length of the contig.
 * @param url If available, the URL the contig is accessible from.
 * @param md5 If available, the MD5 checksum for the contig.
 * @param refseq If available, the REFSEQ ID for the contig.
 * @param genbank If available, the Genbank ID for the contig.
 * @param assembly If available, the assembly name for the assembly this contig
 *   is from.
 * @param species If available, the species this contig was assembled from.
 * @param referenceIndex If available, the number of this contig in a set of
 *   contigs.
 */
case class SequenceRecord(name: ContigName,
                          length: NumLoci,
                          url: Option[String],
                          md5: Option[String],
                          refseq: Option[String],
                          genbank: Option[String],
                          assembly: Option[String],
                          species: Option[String],
                          referenceIndex: Option[Int]) {

  assert(name.name.nonEmpty, "SequenceRecord.name is empty")
  assert(length.num > 0, "SequenceRecord.length <= 0")

  /**
   * @return Returns a new sequence record with the index unset.
   */
  def stripIndex: SequenceRecord = copy(referenceIndex = None)

  override def toString: String =
    "%s->%s%s".format(
      name,
      length,
      referenceIndex.fold("")(", %d".format(_))
    )

  /**
   * Converts this sequence record into a SAM sequence record.
   *
   * @return A SAM formatted sequence record.
   */
  def toSAMSequenceRecord: SAMSequenceRecord = {
    val rec = new SAMSequenceRecord(name.name, length.toInt)

    // set md5 if available
    md5.foreach(s => rec.setAttribute(SAMSequenceRecord.MD5_TAG, s.toUpperCase))

    // set URL if available
    url.foreach(rec.setAttribute(SAMSequenceRecord.URI_TAG, _))

    // set species if available
    species.foreach(rec.setAttribute(SAMSequenceRecord.SPECIES_TAG, _))

    // set assembly if available
    assembly.foreach(rec.setAssembly)

    // set refseq accession number if available
    refseq.foreach(rec.setAttribute("REFSEQ", _))

    // set genbank accession number if available
    genbank.foreach(rec.setAttribute("GENBANK", _))

    referenceIndex.foreach(rec.setSequenceIndex)

    // return record
    rec
  }

  override def equals(o: Any): Boolean =
    o match {
      case that: SequenceRecord ⇒
        name == that.name &&
          length == that.length &&
          optionEq(md5, that.md5) &&
          optionEq(url, that.url)
      case _ ⇒
        false
    }

  // No md5/url is "equal" to any md5/url in this setting
  private def optionEq(o1: Option[String], o2: Option[String]) =
    (o1, o2) match {
      case (Some(c1), Some(c2)) => c1 == c2
      case _                    => true
    }

  /**
   * @return Builds an Avro contig representation from this record.
   */
  def toADAMContig: Contig = {
    val builder =
      Contig
        .newBuilder()
        .setContigName(name.name)
        .setContigLength(length.num)

    md5.foreach(builder.setContigMD5)
    url.foreach(builder.setReferenceURL)
    assembly.foreach(builder.setAssembly)
    species.foreach(builder.setSpecies)
    referenceIndex.foreach(builder.setReferenceIndex(_))

    builder.build
  }
}

/**
 * Companion object for creating Sequence Records.
 */
object SequenceRecord {
  private val REFSEQ_TAG = "REFSEQ"
  private val GENBANK_TAG = "GENBANK"

  /**
   * Java friendly apply method that wraps null strings.
   *
   * @param name The name of the contig.
   * @param length The length of the contig.
   * @param url If available, the URL the contig is accessible from.
   * @param md5 If available, the MD5 checksum for the contig.
   * @param refseq If available, the REFSEQ ID for the contig.
   * @param genbank If available, the Genbank ID for the contig.
   * @param assembly If available, the assembly name for the assembly this contig
   *   is from.
   * @param species If available, the species this contig was assembled from.
   * @param referenceIndex If available, the number of this contig in a set of
   *   contigs.
   * @return Returns a new SequenceRecord where all strings except for name are
   *   wrapped in Options to check for null values.
   */
  def apply(name: ContigName,
            length: NumLoci,
            md5: String = null,
            url: String = null,
            refseq: String = null,
            genbank: String = null,
            assembly: String = null,
            species: String = null,
            referenceIndex: Option[Int] = None): SequenceRecord =
    new SequenceRecord(
      name,
      length,
      Option(url),
      Option(md5),
      Option(refseq),
      Option(genbank),
      Option(assembly),
      Option(species),
      referenceIndex
    )

  /*
   * Generates a sequence record from a SAMSequence record.
   *
   * @param seqRecord SAM Sequence record input.
   * @return A new ADAM sequence record.
   */
  def fromSAMSequenceRecord(record: SAMSequenceRecord)(implicit factory: Factory): SequenceRecord =
    SequenceRecord(
      record.getSequenceName,
      NumLoci(record.getSequenceLength),
      md5 = record.getAttribute(SAMSequenceRecord.MD5_TAG),
      url = record.getAttribute(SAMSequenceRecord.URI_TAG),
      refseq = record.getAttribute(REFSEQ_TAG),
      genbank = record.getAttribute(GENBANK_TAG),
      assembly = record.getAssembly,
      species = record.getAttribute(SAMSequenceRecord.SPECIES_TAG),
      referenceIndex =
        if (record.getSequenceIndex == -1)
          None
        else
          Some(record.getSequenceIndex)
    )

  /**
   * Builds a sequence record from an Avro Contig.
   *
   * @param contig Contig record to build from.
   * @return This Contig record as a SequenceRecord.
   */
  def fromADAMContig(contig: Contig)(implicit factory: Factory): SequenceRecord =
    SequenceRecord(
      contig.getContigName,
      NumLoci(contig.getContigLength),
      md5 = contig.getContigMD5,
      url = contig.getReferenceURL,
      assembly = contig.getAssembly,
      species = contig.getSpecies,
      referenceIndex = Option(contig.getReferenceIndex).map(Integer2int)
    )

  /**
   * Extracts the contig metadata from a nucleotide fragment.
   *
   * @param fragment The assembly fragment to extract a SequenceRecord from.
   * @return The sequence record metadata from a single assembly fragment.
   */
  def fromADAMContigFragment(fragment: NucleotideContigFragment): SequenceRecord =
    fromADAMContig(fragment.getContig)
}

