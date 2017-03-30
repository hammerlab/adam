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

import org.bdgenomics.adam.util.ADAMFunSuite

class FileMergerSuite extends ADAMFunSuite {

  // These paths won't actually get written to.
  lazy val dir = tmpDir()
  lazy val outputPath = dir.resolve("output")
  lazy val headPath = dir.resolve("head")

  sparkTest("cannot write both empty gzip block and cram eof") {
    intercept[IllegalArgumentException] {
      FileMerger.mergeFiles(
        sc.hadoopConfiguration,
        outputPath,
        headPath,
        writeEmptyGzipBlock = true,
        writeCramEOF = true
      )
    }
  }

  sparkTest("buffer size must be non-negative") {
    intercept[IllegalArgumentException] {
      // we don't need to pass real paths here
      FileMerger.mergeFiles(
        sc.hadoopConfiguration,
        outputPath,
        headPath,
        optBufferSize = Some(0)
      )
    }
  }
}
