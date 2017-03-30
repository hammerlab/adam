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

import java.io.{ StringWriter, Writer }
import java.nio.file.{ Files, Path }

import htsjdk.samtools.{ SAMFileHeader, SAMTextHeaderCodec, SAMTextWriter }
import org.bdgenomics.adam.models.SequenceDictionary

/**
 * Helper object for writing a SAM header.
 */
private[rdd] object SAMHeaderWriter {

  /**
   * Writes a header containing just reference contig information.
   *
   * @param path The path to write the header at.
   * @param sequences The sequence dictionary to write.
   */
  def writeHeader(path: Path,
                  sequences: SequenceDictionary) {

    // create a header and attach a sequence dictionary
    val header = new SAMFileHeader
    header.setSequenceDictionary(sequences.toSAMSequenceDictionary)

    // call to our friend
    writeHeader(path, header)
  }

  /**
   * Writes a full SAM header to a filesystem.
   *
   * @param path The path to write the header at.
   * @param header The header to write.
   */
  def writeHeader(path: Path,
                  header: SAMFileHeader) {

    // get an output stream
    val os = Files.newOutputStream(path)

    // get a string writer and set up the text writer
    val sw: Writer = new StringWriter()
    val stw = new SAMTextWriter(os)
    val sthc = new SAMTextHeaderCodec()
    sthc.encode(sw, header)

    // write header to file
    stw.writeHeader(sw.toString)
    stw.close()

    // flush and close stream
    os.flush()
    os.close()
  }
}
