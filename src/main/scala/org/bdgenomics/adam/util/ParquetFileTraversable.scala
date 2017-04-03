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

import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.spark.SparkContext
import org.bdgenomics.utils.misc.HadoopUtil

/**
 * Implements a traversable collection that is backed by a Parquet file.
 *
 * @param sc A SparkContext to use to get underlying Hadoop FileSystem config.
 * @param path The path to the Parquet file to load.
 */
class ParquetFileTraversable[T <: IndexedRecord](sc: SparkContext, path: Path) extends Traversable[T] {

  private val fs = path.getFileSystem(sc.hadoopConfiguration)

  private val paths: List[Path] = {
    if (!fs.exists(path)) {
      throw new IllegalArgumentException("The path %s does not exist".format(path))
    }
    val status = fs.getFileStatus(path)
    var paths = List[Path]()
    if (HadoopUtil.isDirectory(status)) {
      val files = fs.listStatus(path)
      files.foreach {
        file =>
          if (file.getPath.getName.contains("part")) {
            paths ::= file.getPath
          }
      }
    } else if (fs.isFile(path)) {
      paths ::= path
    } else {
      throw new IllegalArgumentException("The path '%s' is neither file nor directory".format(path))
    }
    paths
  }

  /**
   * Runs a for loop over each record in the file, and applies a function.
   *
   * @param f The function to apply to each record.
   */
  override def foreach[U](f: (T) => U) {
    var record: T = null.asInstanceOf[T]

    paths.foreach(path => {
      val parquetReader = new AvroParquetReader[T](path)
      try {
        record = parquetReader.read()
        while (record != null) {
          f(record)
          record = parquetReader.read()
        }
      } finally {
        parquetReader.close()
      }
    })
  }
}
