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
package org.bdgenomics.adam.models

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variant.VariantRDD
import org.bdgenomics.adam.util.ADAMFunSuite

class SnpTableSuite
  extends ADAMFunSuite {

  test("create an empty snp table") {
    val table = SnpTable()
    assert(table.indices.isEmpty)
    assert(table.sites.isEmpty)
  }

  test("create a snp table from variants on multiple contigs") {
    val inputPath = testFile("random.vcf")
    val table = SnpTable(sc.loadVariants(inputPath))
    ==(table.indices.size, 3)
    ==(table.indices("1"), (0, 2))
    ==(table.indices("2"), (3, 3))
    ==(table.indices("13"), (4, 5))
    ==(table.sites.length, 6)
    ==(table.sites(0), 14396L)
    ==(table.sites(1), 14521L)
    ==(table.sites(2), 63734L)
    ==(table.sites(3), 19189L)
    ==(table.sites(4), 752720L)
    ==(table.sites(5), 752790L)
  }

  test("create a snp table from a larger set of variants") {
    val inputPath = testFile("bqsr1.vcf")
    val variants = sc.loadVariants(inputPath)
    val numVariants = variants.rdd.count
    val table = SnpTable(variants)
    ==(table.indices.size, 1)
    ==(table.indices("22"), (0, numVariants.toInt - 1))
    ==(table.sites.length.toLong, numVariants)
    val variantsByPos = variants.rdd
      .map(v => v.getStart.toInt)
      .collect
      .sorted
    table.sites
      .zip(variantsByPos)
      .foreach(p => {
        ==(p._1, p._2)
      })
  }

  def lookUpVariants(rdd: VariantRDD): SnpTable = {
    val table = SnpTable(rdd)
    val variants = rdd.rdd.collect

    variants.foreach(v => {
      val sites = table.maskedSites(ReferenceRegion(v))
      ==(sites.size, 1)
    })

    table
  }

  test("perform lookups on multi-contig snp table") {
    val inputPath = testFile("random.vcf")
    val variants = sc.loadVariants(inputPath)
    val table = lookUpVariants(variants)

    val s1 = table.maskedSites(ReferenceRegion("1", 14390L, 14530L))
    ==(s1.size, 2)
    assert(s1(14396L))
    assert(s1(14521L))

    val s2 = table.maskedSites(ReferenceRegion("13", 752700L, 752800L))
    ==(s2.size, 2)
    assert(s2(752720L))
    assert(s2(752790L))
  }

  test("perform lookups on larger snp table") {
    val inputPath = testFile("bqsr1.vcf")
    val variants = sc.loadVariants(inputPath)
    val table = lookUpVariants(variants)

    val s1 = table.maskedSites(ReferenceRegion("22", 16050670L, 16050690L))
    ==(s1.size, 2)
    assert(s1(16050677L))
    assert(s1(16050682L))

    val s2 = table.maskedSites(ReferenceRegion("22", 16050960L, 16050999L))
    ==(s2.size, 3)
    assert(s2(16050966L))
    assert(s2(16050983L))
    assert(s2(16050993L))

    val s3 = table.maskedSites(ReferenceRegion("22", 16052230L, 16052280L))
    ==(s3.size, 4)
    assert(s3(16052238L))
    assert(s3(16052239L))
    assert(s3(16052249L))
    assert(s3(16052270L))
  }
}
