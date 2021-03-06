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
package org.bdgenomics.adam.rdd.variant

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.hammerlab.genomics.reference.test.ClearContigNames

class VariantRDDSuite
  extends ADAMFunSuite
    with ClearContigNames {

  test("use broadcast join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = variants.broadcastRegionJoin(targets)

    ==(jRdd.rdd.count, 3L)
  }

  test("use right outer broadcast join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = variants.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    ==(c.count(_._1.isEmpty), 3)
    ==(c.count(_._1.isDefined), 3)
  }

  test("use shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.shuffleRegionJoin(targets)
    val jRdd0 = variants.shuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    ==(jRdd.rdd.count, 3L)
    ==(jRdd0.rdd.count, 3L)
  }

  test("use right outer shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = variants.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.count(_._1.isEmpty), 3)
    ==(c0.count(_._1.isEmpty), 3)
    ==(c.count(_._1.isDefined), 3)
    ==(c0.count(_._1.isDefined), 3)
  }

  test("use left outer shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = variants.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.count(_._2.isEmpty), 3)
    ==(c0.count(_._2.isEmpty), 3)
    ==(c.count(_._2.isDefined), 3)
    ==(c0.count(_._2.isDefined), 3)
  }

  test("use full outer shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = variants.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.count(t => t._1.isEmpty && t._2.isEmpty), 0)
    ==(c0.count(t => t._1.isEmpty && t._2.isEmpty), 0)
    ==(c.count(t => t._1.isDefined && t._2.isEmpty), 3)
    ==(c0.count(t => t._1.isDefined && t._2.isEmpty), 3)
    ==(c.count(t => t._1.isEmpty && t._2.isDefined), 3)
    ==(c0.count(t => t._1.isEmpty && t._2.isDefined), 3)
    ==(c.count(t => t._1.isDefined && t._2.isDefined), 3)
    ==(c0.count(t => t._1.isDefined && t._2.isDefined), 3)
  }

  test("use shuffle join with group by to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = variants.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.size, 3)
    ==(c0.size, 3)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  test("use right outer shuffle join with group by to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = variants.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd0.rdd.collect
    val c0 = jRdd0.rdd.collect

    ==(c.count(_._1.isDefined), 6)
    ==(c0.count(_._1.isDefined), 6)
    ==(c.filter(_._1.isDefined).count(_._2.size == 1), 3)
    ==(c0.filter(_._1.isDefined).count(_._2.size == 1), 3)
    ==(c.filter(_._1.isDefined).count(_._2.isEmpty), 3)
    ==(c0.filter(_._1.isDefined).count(_._2.isEmpty), 3)
    ==(c.count(_._1.isEmpty), 3)
    ==(c0.count(_._1.isEmpty), 3)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }

  test("convert back to variant contexts") {
    val variantsPath = testFile("small.vcf")
    val variants = sc.loadVariants(variantsPath)
    val variantContexts = variants.toVariantContextRDD

    assert(variantContexts.sequences.contains("1"))
    assert(variantContexts.samples.isEmpty)

    val vcs = variantContexts.rdd.collect
    ==(vcs.size, 6)

    val vc = vcs.head
    ==(vc.position.referenceName, "1")
    ==(vc.variant.variant.contigName, "1")
    assert(vc.genotypes.isEmpty)
  }
}
