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

import htsjdk.samtools.util.Log
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
import org.hammerlab.genomics.reference.test.{ ClearContigNames, ContigNameCanEqualString, LocusCanEqualInt }
import org.hammerlab.spark.test.suite.KryoSparkSuite
import org.hammerlab.test.matchers.files.FileMatcher.fileMatch
import org.hammerlab.test.resources.File
import org.scalactic.TypeCheckedTripleEquals

abstract class ADAMFunSuite
  extends KryoSparkSuite(classOf[ADAMKryoRegistrator], referenceTracking = true)
    with ContigNameCanEqualString
    with LocusCanEqualInt
    with ClearContigNames
    with TypeCheckedTripleEquals {

  // added to resolve #1280
  Log.setGlobalLogLevel(Log.LogLevel.ERROR)

  def resourceUrl(path: String): URL =
    Thread.currentThread().getContextClassLoader.getResource(path)

  def testFile(name: String): String = File(name)

  def sparkTest(name: String)(body: ⇒ Unit): Unit = {
    test(name) { body }
  }

  def tmpLocation(extension: String = ".adam"): String = tmpFile(suffix = extension)

  override def tmpFile(prefix: String, suffix: String): String = tmpPath(prefix, suffix)

  def checkFiles(expectedPath: String, actualPath: String): Unit = {
    expectedPath should fileMatch(actualPath)
  }
}

