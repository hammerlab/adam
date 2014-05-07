/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.cli

import org.apache.hadoop.mapreduce.Job
import org.bdgenomics.adam.predicates.{ UniqueMappedReadPredicate, LocusPredicate }
import org.kohsuke.args4j.{ Option => option, Argument }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.avro.{ ADAMResiduePileup, ADAMPileup, ADAMRecord }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ADAMRod, ReferencePosition }
import org.bdgenomics.adam.rich.RichADAMRecord
import org.apache.spark.SparkContext._

object Reads2Ref extends ADAMCommandCompanion {
  val commandName: String = "reads2ref"
  val commandDescription: String = "Convert an ADAM read-oriented file to an ADAM reference-oriented file"

  def apply(cmdLine: Array[String]) = {
    new Reads2Ref(Args4j[Reads2RefArgs](cmdLine))
  }
}

object Reads2RefArgs {
  val MIN_MAPQ_DEFAULT: Long = 30L
}

class Reads2RefArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(metaVar = "ADAMREADS", required = true, usage = "ADAM read-oriented data", index = 0)
  var readInput: String = _

  @Argument(metaVar = "DIR", required = true, usage = "Location to create reference-oriented ADAM data", index = 1)
  var pileupOutput: String = _

  @option(name = "-mapq", usage = "Minimal mapq value allowed for a read (default = 30)")
  var minMapq: Long = Reads2RefArgs.MIN_MAPQ_DEFAULT

  @option(name = "-aggregate", usage = "Aggregates data at each pileup position, to reduce storage cost.")
  var aggregate: Boolean = false

  @option(name = "-allowNonPrimaryAlignments", usage = "Converts reads that are not at their primary alignment positions to pileups.")
  var nonPrimary: Boolean = true

  @option(name = "-residue", usage = "Create residue pileups")
  var residue: Boolean = false

  @option(name = "-presort", usage = "Presort reads!")
  var presort: Boolean = false
}

class Reads2Ref(protected val args: Reads2RefArgs) extends ADAMSparkCommand[Reads2RefArgs] {
  val companion = Reads2Ref

  def run(sc: SparkContext, job: Job) {
    var reads: RDD[ADAMRecord] = sc.adamLoad(args.readInput, Some(classOf[UniqueMappedReadPredicate]))
    if (args.repartition != -1) {
      println("Repartitioning reads to to '%d' partitions".format(args.repartition))
      reads = reads.repartition(args.repartition)
    }
    if (args.residue) {
      val useReads: RDD[ADAMRecord] = if (args.presort) {
        reads.keyBy(r => ReferencePosition(r).get).sortByKey().mapPartitions(_.map(_._2), preservesPartitioning = true)
      } else {
        reads
      }
      val pileups: RDD[ADAMResiduePileup] = useReads.adamRecords2ResiduePileup(args.nonPrimary, args.presort)
      pileups.adamSave(args.pileupOutput, blockSize = args.blockSize, pageSize = args.pageSize,
        compressCodec = args.compressionCodec, disableDictionaryEncoding = args.disableDictionary)
    } else {

      val pileups = reads.adamRecords2Rods(secondaryAlignments = args.nonPrimary).flatMap(_.pileups)
      //      if (args.aggregate) {
      //        pileups.adamAggregatePileups(coverage.toInt).adamSave(args.pileupOutput,
      //          blockSize = args.blockSize, pageSize = args.pageSize, compressCodec = args.compressionCodec,
      //          disableDictionaryEncoding = args.disableDictionary)
      //      } else {

      pileups.adamSave(args.pileupOutput, blockSize = args.blockSize, pageSize = args.pageSize,
        compressCodec = args.compressionCodec, disableDictionaryEncoding = args.disableDictionary)
      //      }
    }
  }
}
