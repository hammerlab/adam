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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.algorithms.consensus.Consensus
import org.bdgenomics.formats.avro.Variant
import org.bdgenomics.utils.misc.Logging
import org.hammerlab.genomics.reference.ContigName

private[adam] class IndelTable(private val table: Map[ContigName, Iterable[Consensus]]) extends Serializable with Logging {
  log.info("Indel table has %s contigs and %s entries".format(
    table.size,
    table.values.map(_.size).sum
  ))

  /**
   * Returns all known indels within the given reference region. If none are known, returns an empty Seq.
   *
   * @param region Region to look for known indels.
   * @return Returns a sequence of consensuses.
   */
  def getIndelsInRegion(region: ReferenceRegion): Seq[Consensus] = {
    if (table.contains(region.referenceName)) {
      val bucket = table(region.referenceName)

      bucket.filter(_.index.overlaps(region)).toSeq
    } else {
      Seq()
    }
  }
}

private[adam] object IndelTable {

  /**
   * Creates an indel table from an RDD containing known variants.
   *
   * @param variants RDD of variants.
   * @return Returns a table with known indels populated.
   */
  def apply(variants: RDD[Variant]): IndelTable = {
    val consensus: Map[ContigName, Iterable[Consensus]] =
      variants
        .filter(v => v.getReferenceAllele.length != v.getAlternateAllele.length)
        .map { v =>
          val referenceName = ContigName(v.getContigName)
          val consensus =
            if (v.getReferenceAllele.length > v.getAlternateAllele.length) {
              // deletion
              val deletionLength = v.getReferenceAllele.length - v.getAlternateAllele.length
              val start = v.getStart + v.getAlternateAllele.length

              Consensus("", ReferenceRegion(referenceName, start, start + deletionLength + 1))
            } else {
              val start = v.getStart + v.getReferenceAllele.length

              Consensus(v.getAlternateAllele.drop(v.getReferenceAllele.length),
                ReferenceRegion(referenceName, start, start + 1))
            }

          (referenceName, consensus)
        }
        .groupByKey()
        .collect()
        .toMap

    new IndelTable(consensus)
  }
}
