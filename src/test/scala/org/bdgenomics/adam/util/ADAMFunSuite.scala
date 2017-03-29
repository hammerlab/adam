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

import java.net.URL
import java.nio.file.Path

import htsjdk.samtools.util.Log
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
import org.hammerlab.genomics.reference.test.{ ClearContigNames, ContigNameCanEqualString, LocusCanEqualInt }
import org.hammerlab.spark.test.suite.KryoSparkSuite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.{ File, Url }
import org.scalactic.TypeCheckedTripleEquals
import org.hammerlab.paths

abstract class ADAMFunSuite
  extends KryoSparkSuite(classOf[ADAMKryoRegistrator], referenceTracking = true)
    with paths.Conversions
    with ContigNameCanEqualString
    with LocusCanEqualInt
    with ClearContigNames
    with TypeCheckedTripleEquals {

  // added to resolve #1280
  Log.setGlobalLogLevel(Log.LogLevel.ERROR)

  def resourceUrl(path: String): URL = Url(path)

  def testFile(name: String): String = File(name)

  def sparkTest(name: String)(body: ⇒ Unit): Unit =
    test(name) { body }

  def tmpLocation(extension: String = ".adam"): Path = tmpFile(suffix = extension)

  /**
   * Lots of tests use [[tmpFile]] to get a path to a not-yet-created temporary file.
   *
   * [[org.hammerlab.test.files.TmpFiles]] creates a file with [[tmpFile]] and returns a path with [[tmpPath]], so we
   * just reroute them here to avoid potential merge conflicts with upstream by rewriting many tests' calls.
   */
  override def tmpFile(prefix: String, suffix: String): Path = tmpPath(prefix, suffix)

  implicit val pathToString = paths.pathToString _

  def checkFiles(actualPath: Path, expectedPath: File): Unit = {
    actualPath should fileMatch(expectedPath)
  }
}

