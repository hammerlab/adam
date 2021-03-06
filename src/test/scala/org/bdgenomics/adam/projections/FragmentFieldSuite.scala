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
package org.bdgenomics.adam.projections

import com.google.common.collect.ImmutableList
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.FragmentField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment }

class FragmentFieldSuite extends ADAMFunSuite {

  test("Use projection when reading parquet fragments") {
    val path = tmpFile("fragments.parquet")
    val rdd = sc.parallelize(Seq(Fragment.newBuilder()
      .setReadName("read_name")
      .setInstrument("instrument")
      .setRunId("run_id")
      .setFragmentSize(42)
      .setAlignments(ImmutableList.of(AlignmentRecord.newBuilder()
        .setContigName("6")
        .setStart(29941260L)
        .build()))
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      readName,
      instrument,
      runId,
      fragmentSize,
      alignments
    )

    val fragments: RDD[Fragment] = sc.loadParquet(path, projection = Some(projection))
    ==(fragments.count(), 1)
    ==(fragments.first.getReadName, "read_name")
    ==(fragments.first.getInstrument, "instrument")
    ==(fragments.first.getRunId, "run_id")
    ==(fragments.first.getFragmentSize, 42)
    ==(fragments.first.getAlignments.get(0).getContigName, "6")
  }
}
