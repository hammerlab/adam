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

package org.bdgenomics.adam.util

import htsjdk.samtools.ValidationStringency
import htsjdk.samtools.reference.{ FastaSequenceIndex, IndexedFastaSequenceFile }
import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.bdgenomics.utils.misc.Logging
import org.hammerlab.paths.Path

/**
 * Loads and extracts sequences directly from indexed fasta or fa files. filePath requires fai index in the
 * same directory with same naming convention.
 *
 * @param path path to fasta or fa index
 */
case class IndexedFastaFile(sc: SparkContext,
                            path: Path,
                            stringency: ValidationStringency = ValidationStringency.STRICT)
    extends ReferenceFile with Logging {

  // Generate IndexedFastaSequenceFile from path and fai index
  private val ref: IndexedFastaSequenceFile =
    new IndexedFastaSequenceFile(
      path,
      new FastaSequenceIndex(path + ".fai")
    )

  // Get sequence dictionary. If sequence dictionary is not defined,
  // generate sequence dictionary from file
  val sequences =
    try {
      SequenceDictionary(ref.getSequenceDictionary)
    } catch {
      case e: Throwable => {
        if (stringency == ValidationStringency.STRICT) {
          throw e
        } else {
          if (stringency == ValidationStringency.LENIENT) {
            log.warn("Caught exception %s when loading FASTA sequence dictionary. Using empty dictionary instead.".format(e))
          }
          SequenceDictionary.empty
        }
      }
    }

  /**
   * Extracts base sequence from FastaSequenceIndex
   * @param region The desired ReferenceRegion to extract.
   * @return The reference sequence at the desired locus.
   */
  def extract(region: ReferenceRegion): String = {
    ref.getSubsequenceAt(region.referenceName.name, region.start, region.end)
      .getBaseString
  }
}

