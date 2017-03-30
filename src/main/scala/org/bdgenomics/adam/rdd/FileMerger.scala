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

import java.nio.file.Files.{ newDirectoryStream, newInputStream, newOutputStream, delete }
import java.nio.file.Path

import htsjdk.samtools.cram.build.CramIO
import htsjdk.samtools.cram.common.CramVersions
import htsjdk.samtools.util.BlockCompressedStreamConstants
import org.apache.commons.io.FileUtils.deleteDirectory
import org.apache.commons.io.IOUtils.copyLarge
import org.apache.hadoop.conf.Configuration
import org.bdgenomics.utils.misc.Logging

import scala.collection.JavaConverters._

/**
 * Helper object to merge sharded files together.
 */
object FileMerger extends Logging {

  /**
   * The config entry for the buffer size in bytes.
   */
  val BUFFER_SIZE_CONF = "org.bdgenomics.adam.rdd.FileMerger.bufferSize"

  /**
   * Merges together sharded files, while preserving partition ordering.
   *
   * @param outputPath The location to write the merged file at.
   * @param tailPath The location where the sharded files have been written.
   * @param optHeaderPath Optionally, the location where a header file has
   *   been written.
   * @param writeEmptyGzipBlock If true, we write an empty GZIP block at the
   *   end of the merged file.
   * @param writeCramEOF If true, we write CRAM's EOF signifier.
   * @param optBufferSize The size in bytes of the buffer used for copying. If
   *   not set, we check the config for this value. If that is not set, we
   *   default to 4MB.
   *
   * @see mergeFilesAcrossFilesystems
   */
  private[adam] def mergeFiles(conf: Configuration,
                               outputPath: Path,
                               tailPath: Path,
                               optHeaderPath: Option[Path] = None,
                               writeEmptyGzipBlock: Boolean = false,
                               writeCramEOF: Boolean = false,
                               optBufferSize: Option[Int] = None) {
    mergeFilesAcrossFilesystems(
      conf,
      outputPath, tailPath, optHeaderPath = optHeaderPath,
      writeEmptyGzipBlock = writeEmptyGzipBlock,
      writeCramEOF = writeCramEOF,
      optBufferSize = optBufferSize)
  }

  /**
   * Merges together sharded files, while preserving partition ordering.
   *
   * Can read files from a different filesystem then they are written to.
   *
   * @param outputPath The location to write the merged file at.
   * @param tailPath The location where the sharded files have been written.
   * @param optHeaderPath Optionally, the location where a header file has
   *   been written.
   * @param writeEmptyGzipBlock If true, we write an empty GZIP block at the
   *   end of the merged file.
   * @param writeCramEOF If true, we write CRAM's EOF signifier.
   * @param optBufferSize The size in bytes of the buffer used for copying. If
   *   not set, we check the config for this value. If that is not set, we
   *   default to 4MB.
   */
  private[adam] def mergeFilesAcrossFilesystems(conf: Configuration,
                                                outputPath: Path,
                                                tailPath: Path,
                                                optHeaderPath: Option[Path] = None,
                                                writeEmptyGzipBlock: Boolean = false,
                                                writeCramEOF: Boolean = false,
                                                optBufferSize: Option[Int] = None) {

    // check for buffer size in option, if not in option, check hadoop conf,
    // if not in hadoop conf, fall back on 4MB
    val bufferSize =
    optBufferSize
      .getOrElse(
        conf.getInt(
          BUFFER_SIZE_CONF,
          4 * 1024 * 1024
        )
      )

    require(
      bufferSize > 0,
      "Cannot have buffer size < 1. %d was provided.".format(bufferSize)
    )

    require(
      !(writeEmptyGzipBlock && writeCramEOF),
      "writeEmptyGzipBlock and writeCramEOF are mutually exclusive."
    )

    val partIdxRegex = """part-(\d+)""".r

    // get a list of all of the files in the tail file
    val tailFiles =
      newDirectoryStream(tailPath, "part-*")
        .iterator()
        .asScala
        .toArray
        .sortBy {
          _.getFileName.toString match {
            case partIdxRegex(idStr) ⇒
              idStr.toInt
            case path ⇒
              throw new IllegalArgumentException(s"Bad path: $path")
          }
        }

    // open our output file
    val os = newOutputStream(outputPath)

    // here is a byte array for copying
    val buffer = new Array[Byte](bufferSize)
//
//    @tailrec def copy(is: InputStream,
//                      los: OutputStream) {
//
//      // make a read
//      val bytesRead = is.read(ba)
//
//      // did our read succeed? if so, write to output stream
//      // and continue
//      if (bytesRead >= 0) {
//        los.write(ba, 0, bytesRead)
//
//        copy(is, los)
//      }
//    }

    // optionally copy the header
    optHeaderPath.foreach { p ⇒
      log.info(s"Copying header file ($p)")

      // open our input file
      val is = newInputStream(p)
      copyLarge(is, os, buffer)
      is.close()
    }

    // loop over allllll the files and copy them
    val numFiles = tailFiles.length
    var filesCopied = 1
    tailFiles.toSeq.foreach { p =>

      // print a bit of progress logging
      log.info(s"Copying file $p, file $filesCopied of $numFiles.")

      // open our input file
      val is = newInputStream(p)
      copyLarge(is, os, buffer)
      is.close()

      // increment file copy count
      filesCopied += 1
    }

    // finish the file off
    if (writeEmptyGzipBlock) {
      os.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK)
    } else if (writeCramEOF) {
      CramIO.issueEOF(CramVersions.DEFAULT_CRAM_VERSION, os)
    }

    // flush and close the output stream
    os.flush()
    os.close()

    // delete temp files
    optHeaderPath.foreach(delete)
    deleteDirectory(tailPath.toFile)
  }
}
