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

class GenotypeRDDSuite extends ADAMFunSuite {

  test("use broadcast join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = genotypes.broadcastRegionJoin(targets)

    ==(jRdd.rdd.count, 9L)
  }

  test("use right outer broadcast join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = genotypes.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    ==(c.count(_._1.isEmpty), 3)
    ==(c.count(_._1.isDefined), 9)
  }

  test("use shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.shuffleRegionJoin(targets)
    val jRdd0 = genotypes.shuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    ==(jRdd.rdd.count, 9L)
    ==(jRdd0.rdd.count, 9L)
  }

  test("use right outer shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = genotypes.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.count(_._1.isEmpty), 3)
    ==(c0.count(_._1.isEmpty), 3)
    ==(c.count(_._1.isDefined), 9)
    ==(c0.count(_._1.isDefined), 9)
  }

  test("use left outer shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = genotypes.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.count(_._2.isEmpty), 9)
    ==(c0.count(_._2.isEmpty), 9)
    ==(c.count(_._2.isDefined), 9)
    ==(c0.count(_._2.isDefined), 9)
  }

  test("use full outer shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = genotypes.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.count(t => t._1.isEmpty && t._2.isEmpty), 0)
    ==(c0.count(t => t._1.isEmpty && t._2.isEmpty), 0)
    ==(c.count(t => t._1.isDefined && t._2.isEmpty), 9)
    ==(c0.count(t => t._1.isDefined && t._2.isEmpty), 9)
    ==(c.count(t => t._1.isEmpty && t._2.isDefined), 3)
    ==(c0.count(t => t._1.isEmpty && t._2.isDefined), 3)
    ==(c.count(t => t._1.isDefined && t._2.isDefined), 9)
    ==(c0.count(t => t._1.isDefined && t._2.isDefined), 9)
  }

  test("use shuffle join with group by to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = genotypes.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    ==(c.size, 9)
    ==(c0.size, 9)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  test("use right outer shuffle join with group by to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = genotypes.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    ==(jRdd.rdd.partitions.length, 1)
    ==(jRdd0.rdd.partitions.length, 5)

    val c = jRdd0.rdd.collect
    val c0 = jRdd0.rdd.collect

    ==(c.count(_._1.isDefined), 18)
    ==(c0.count(_._1.isDefined), 18)
    ==(c.filter(_._1.isDefined).count(_._2.size == 1), 9)
    ==(c0.filter(_._1.isDefined).count(_._2.size == 1), 9)
    ==(c.filter(_._1.isDefined).count(_._2.isEmpty), 9)
    ==(c0.filter(_._1.isDefined).count(_._2.isEmpty), 9)
    ==(c.count(_._1.isEmpty), 3)
    ==(c0.count(_._1.isEmpty), 3)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }

  test("convert back to variant contexts") {
    val genotypesPath = testFile("small.vcf")
    val genotypes = sc.loadGenotypes(genotypesPath)
    val variantContexts = genotypes.toVariantContextRDD

    assert(variantContexts.sequences.contains("1"))
    assert(variantContexts.samples.nonEmpty)

    val vcs = variantContexts.rdd.collect
    ==(vcs.size, 6)

    val vc = vcs.head
    ==(vc.position.referenceName, "1")
    ==(vc.variant.variant.contigName, "1")
    assert(vc.genotypes.nonEmpty)
  }
}
