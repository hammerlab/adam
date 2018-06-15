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
package org.bdgenomics.adam.converters

import htsjdk.samtools._
import java.io.File
import org.scalatest.FunSuite
import scala.collection.JavaConversions._

class SAMRecordConverterSuite extends FunSuite {

  test("testing the fields in an alignmentRecord obtained from a mapped samRecord conversion") {

    val testRecordConverter = new SAMRecordConverter
    val testFileString = getClass.getClassLoader.getResource("reads12.sam").getFile
    val testFile = new File(testFileString)

    // Iterator of SamReads in the file that each have a samRecord for conversion
    val testIterator = SamReaderFactory.makeDefault().open(testFile)
    val testSAMRecord = testIterator.iterator().next()

    // set the oq, md, oc, and op attributes
    testSAMRecord.setOriginalBaseQualities("*****".getBytes.map(v => (v - 33).toByte))
    testSAMRecord.setAttribute("MD", "100")
    testSAMRecord.setAttribute("OC", "100M")
    testSAMRecord.setAttribute("OP", 1)

    // Convert samRecord to alignmentRecord
    val testAlignmentRecord = testRecordConverter.convert(testSAMRecord)

    // Validating Conversion
    ==(testAlignmentRecord.getCigar, testSAMRecord.getCigarString)
    ==(testAlignmentRecord.getDuplicateRead, testSAMRecord.getDuplicateReadFlag)
    ==(testAlignmentRecord.getEnd.toInt, testSAMRecord.getAlignmentEnd)
    ==(testAlignmentRecord.getMapq.toInt, testSAMRecord.getMappingQuality)
    ==(testAlignmentRecord.getStart.toInt, (testSAMRecord.getAlignmentStart - 1))
    assert(testAlignmentRecord.getReadInFragment == 0)
    ==(testAlignmentRecord.getFailedVendorQualityChecks, testSAMRecord.getReadFailsVendorQualityCheckFlag)
    ==(!testAlignmentRecord.getPrimaryAlignment, testSAMRecord.getNotPrimaryAlignmentFlag)
    ==(!testAlignmentRecord.getReadMapped, testSAMRecord.getReadUnmappedFlag)
    ==(testAlignmentRecord.getReadName, testSAMRecord.getReadName)
    ==(testAlignmentRecord.getReadNegativeStrand, testSAMRecord.getReadNegativeStrandFlag)
    assert(!testAlignmentRecord.getReadPaired)
    assert(testAlignmentRecord.getReadInFragment != 1)
    ==(testAlignmentRecord.getSupplementaryAlignment, testSAMRecord.getSupplementaryAlignmentFlag)
    ==(testAlignmentRecord.getOrigQual, "*****")
    ==(testAlignmentRecord.getMismatchingPositions, "100")
    ==(testAlignmentRecord.getOldCigar, "100M")
    ==(testAlignmentRecord.getOldPosition, 0L)
    ==(testAlignmentRecord.getAttributes, "XS:i:0\tAS:i:75\tNM:i:0")
  }

  test("testing the fields in an alignmentRecord obtained from an unmapped samRecord conversion") {

    val testRecordConverter = new SAMRecordConverter
    val testFileString = getClass.getClassLoader.getResource("reads12.sam").getFile
    val testFile = new File(testFileString)

    // Iterator of SamReads in the file that each have a samRecord for conversion
    val testIterator = SamReaderFactory.makeDefault().open(testFile)
    val testSAMRecord = testIterator.iterator().next()

    // Convert samRecord to alignmentRecord
    val testAlignmentRecord = testRecordConverter.convert(testSAMRecord)

    // Validating Conversion
    ==(testAlignmentRecord.getCigar, testSAMRecord.getCigarString)
    ==(testAlignmentRecord.getDuplicateRead, testSAMRecord.getDuplicateReadFlag)
    ==(testAlignmentRecord.getEnd.toInt, testSAMRecord.getAlignmentEnd)
    ==(testAlignmentRecord.getMapq.toInt, testSAMRecord.getMappingQuality)
    ==(testAlignmentRecord.getStart.toInt, (testSAMRecord.getAlignmentStart - 1))
    assert(testAlignmentRecord.getReadInFragment == 0)
    ==(testAlignmentRecord.getFailedVendorQualityChecks, testSAMRecord.getReadFailsVendorQualityCheckFlag)
    ==(!testAlignmentRecord.getPrimaryAlignment, testSAMRecord.getNotPrimaryAlignmentFlag)
    ==(!testAlignmentRecord.getReadMapped, testSAMRecord.getReadUnmappedFlag)
    ==(testAlignmentRecord.getReadName, testSAMRecord.getReadName)
    ==(testAlignmentRecord.getReadNegativeStrand, testSAMRecord.getReadNegativeStrandFlag)
    assert(!testAlignmentRecord.getReadPaired)
    assert(testAlignmentRecord.getReadInFragment != 1)
    ==(testAlignmentRecord.getSupplementaryAlignment, testSAMRecord.getSupplementaryAlignmentFlag)
  }

  test("'*' quality gets nulled out") {

    val newRecordConverter = new SAMRecordConverter
    val newTestFile = new File(getClass.getClassLoader.getResource("unmapped.sam").getFile)
    val newSAMReader = SamReaderFactory.makeDefault().open(newTestFile)

    // Obtain SAMRecord
    val newSAMRecordIter = {
      val samIter = asScalaIterator(newSAMReader.iterator())
      samIter.toIterable.dropWhile(!_.getReadUnmappedFlag)
    }
    val newSAMRecord = newSAMRecordIter.toIterator.next()

    // null out quality
    newSAMRecord.setBaseQualityString("*")

    // Conversion
    val newAlignmentRecord = newRecordConverter.convert(newSAMRecord)

    // Validating Conversion
    ==(newAlignmentRecord.getQual, null)
  }

  test("don't keep denormalized fields") {
    val rc = new SAMRecordConverter

    assert(rc.skipTag("MD"))
    assert(rc.skipTag("OQ"))
    assert(rc.skipTag("OP"))
    assert(rc.skipTag("OC"))
  }
}
