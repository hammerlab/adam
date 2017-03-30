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

import java.nio.file.Files.newOutputStream
import java.nio.file.Path

import htsjdk.variant.variantcontext.writer.{ Options, VariantContextWriterBuilder }
import htsjdk.variant.vcf.VCFHeader

/**
 * Utility for writing VCF headers to a file.
 */
private[rdd] object VCFHeaderUtils {
  /**
   * Writes a vcf header to a file.
   *
   * @param header The header to write.
   * @param path The path to write it to.
   */
  def write(header: VCFHeader,
            path: Path) {
    // build a vcw
    val vcw =
      new VariantContextWriterBuilder()
        .setOutputVCFStream(newOutputStream(path))
        .clearIndexCreator()
        .unsetOption(Options.INDEX_ON_THE_FLY)
        .build()

    // write the header
    vcw.writeHeader(header)

    // close the writer
    // vcw.close calls close on the underlying stream, see ADAM-1337
    vcw.close()
  }
}
