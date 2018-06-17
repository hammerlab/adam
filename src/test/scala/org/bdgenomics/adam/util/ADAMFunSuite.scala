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
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
import org.hammerlab.cmp.CanEq
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.genomics.reference.test.{ ClearContigNames, LocusCanEqualInt }
import org.hammerlab.hadoop.Configuration
import org.hammerlab.paths.Path
import org.hammerlab.spark.test.suite.KryoSparkSuite
import org.hammerlab.test.Cmp
import org.hammerlab.test.resources.{ File, Url }
import org.scalactic.TypeCheckedTripleEquals

abstract class ADAMFunSuite
  extends KryoSparkSuite(
    referenceTracking = true
  )
  with LocusCanEqualInt
  with ClearContigNames
  with TypeCheckedTripleEquals {

  register(new ADAMKryoRegistrator: KryoRegistrator)

  // added to resolve #1280
  Log.setGlobalLogLevel(Log.LogLevel.ERROR)

  def hadoopConf: Configuration = ctx

  def resourceUrl(path: String): URL = Url(path)

  def testFile(name: String): Path = File(name)

  def tmpLocation(extension: String = ".adam"): Path = tmpFile(suffix = extension)

  /**
   * Lots of tests use [[tmpFile]] to get a [[Path]] to a not-yet-created temporary file.
   *
   * [[org.hammerlab.test.files.TmpFiles]] creates and returns a [[Path]] with [[tmpFile]] and only returns one with
   * [[tmpPath]], so we just reroute them here to avoid potential merge conflicts with upstream by rewriting many tests'
   * calls.
   */
  override def tmpFile(prefix: String, suffix: String): Path = tmpPath(prefix, suffix)

  def checkFiles(actualPath: Path, expectedPath: File): Unit = {
    actualPath should fileMatch(expectedPath)
  }

  def nullCmp[T](implicit ord: Ordering[T]): Cmp.Aux[T, (T, T)] =
    new Cmp[T] {
      type Diff = (T, T)
      def cmp(l: T, r: T): Option[(T, T)] =
        (l, r) match {
          case (null, null) ⇒ None
          case (_, null) | (null, _) ⇒ Some((l, r))
          case (l, r) if !ord.equiv(l, r) ⇒ Some((l, r))
          case _ ⇒ None
        }
    }

  implicit val cmpStrings: Cmp.Aux[String, (String, String)] = nullCmp
  import java.{ lang ⇒ jl }
  type JInt = jl.Integer
  implicit val cmpInteger: Cmp.Aux[JInt, (JInt, JInt)] = nullCmp
  implicit val cmpFloat: Cmp.Aux[jl.Float, (jl.Float, jl.Float)] = nullCmp
  implicit val cmpBoolean: Cmp.Aux[jl.Boolean, (jl.Boolean, jl.Boolean)] = nullCmp
  implicit val cmpLong: Cmp.Aux[jl.Long, (jl.Long, jl.Long)] = nullCmp

  implicit def intCanEqJLong(implicit cmp: Cmp[java.lang.Long]): CanEq.Aux[java.lang.Long, Int, cmp.Diff] = CanEq.withConversion[java.lang.Long, Int](cmp, _.toLong)
  implicit def numLociCanEqInt(implicit cmp: Cmp[NumLoci]): CanEq.Aux[NumLoci, Int, cmp.Diff] = CanEq.withConversion[NumLoci, Int](cmp, NumLoci(_))
}

