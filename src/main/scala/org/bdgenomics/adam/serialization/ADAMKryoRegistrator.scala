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
package org.bdgenomics.adam.serialization

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference
import org.hammerlab.kryo._

class ADAMKryoRegistrator extends spark.Registrar(
  // Register Avro classes using fully qualified class names
  // Sort alphabetically and add blank lines between packages

  // htsjdk.samtools
  cls[htsjdk.samtools.CigarElement],
  cls[htsjdk.samtools.CigarOperator],
  cls[htsjdk.samtools.Cigar],
  cls[htsjdk.samtools.SAMSequenceDictionary],
  cls[htsjdk.samtools.SAMFileHeader],
  cls[htsjdk.samtools.SAMSequenceRecord],

  // htsjdk.variant.vcf
  cls[htsjdk.variant.vcf.VCFContigHeaderLine],
  cls[htsjdk.variant.vcf.VCFFilterHeaderLine],
  cls[htsjdk.variant.vcf.VCFFormatHeaderLine],
  cls[htsjdk.variant.vcf.VCFInfoHeaderLine],
  cls[htsjdk.variant.vcf.VCFHeader],
  cls[htsjdk.variant.vcf.VCFHeaderLine],
  cls[htsjdk.variant.vcf.VCFHeaderLineCount],
  cls[htsjdk.variant.vcf.VCFHeaderLineType],
  "htsjdk.variant.vcf.VCFCompoundHeaderLine$SupportedHeaderLineType",

  // java.lang
  cls[java.lang.Class[_]],

  // java.util
  cls[java.util.ArrayList[_]],
  cls[java.util.LinkedHashMap[_, _]],
  cls[java.util.LinkedHashSet[_]],
  cls[java.util.HashMap[_, _]],
  cls[java.util.HashSet[_]],

  // org.apache.avro
  "org.apache.avro.Schema$RecordSchema",
  "org.apache.avro.Schema$Field",
  "org.apache.avro.Schema$Field$Order",
  "org.apache.avro.Schema$UnionSchema",
  "org.apache.avro.Schema$Type",
  "org.apache.avro.Schema$LockableArrayList",
  "org.apache.avro.Schema$BooleanSchema",
  "org.apache.avro.Schema$NullSchema",
  "org.apache.avro.Schema$StringSchema",
  "org.apache.avro.Schema$IntSchema",
  "org.apache.avro.Schema$FloatSchema",
  "org.apache.avro.Schema$EnumSchema",
  "org.apache.avro.Schema$Name",
  "org.apache.avro.Schema$LongSchema",
  "org.apache.avro.generic.GenericData$Array",

  // org.apache.hadoop.conf
  cls[org.apache.hadoop.conf.Configuration] → new WritableSerializer[org.apache.hadoop.conf.Configuration],
  cls[org.apache.hadoop.yarn.conf.YarnConfiguration] → new WritableSerializer[org.apache.hadoop.yarn.conf.YarnConfiguration],

  // org.apache.hadoop.io
  cls[org.apache.hadoop.io.Text],
  cls[org.apache.hadoop.io.LongWritable],

  // org.bdgenomics.adam.algorithms.consensus
  cls[org.bdgenomics.adam.algorithms.consensus.Consensus],

  // org.bdgenomics.adam.converters
  cls[org.bdgenomics.adam.converters.FastaConverter.FastaDescriptionLine],
  cls[org.bdgenomics.adam.converters.FragmentCollector],

  // org.bdgenomics.adam.models
  cls[org.bdgenomics.adam.models.Coverage],
  cls[org.bdgenomics.adam.models.IndelTable],
  cls[org.bdgenomics.adam.models.MdTag],
  cls[org.bdgenomics.adam.models.MultiContigNonoverlappingRegions],
  cls[org.bdgenomics.adam.models.NonoverlappingRegions],
  cls[org.bdgenomics.adam.models.RecordGroup],
  cls[org.bdgenomics.adam.models.RecordGroupDictionary],
  cls[org.bdgenomics.adam.models.ReferencePosition] → new org.bdgenomics.adam.models.ReferencePositionSerializer,
  cls[org.bdgenomics.adam.models.ReferenceRegion],
  cls[org.bdgenomics.adam.models.SAMFileHeaderWritable],
  cls[org.bdgenomics.adam.models.SequenceDictionary],
  cls[org.bdgenomics.adam.models.SequenceRecord],
  cls[org.bdgenomics.adam.models.SnpTable] → new org.bdgenomics.adam.models.SnpTableSerializer,
  cls[org.bdgenomics.adam.models.VariantContext] → new org.bdgenomics.adam.models.VariantContextSerializer,

  // org.bdgenomics.adam.rdd
  cls[org.bdgenomics.adam.rdd.GenomeBins],
  "org.bdgenomics.adam.rdd.SortedIntervalPartitionJoinAndGroupByLeft$$anonfun$postProcessHits$1",

  // IntervalArray registrations for org.bdgenomics.adam.rdd
  cls[org.bdgenomics.adam.rdd.read.AlignmentRecordArray] → new org.bdgenomics.adam.rdd.read.AlignmentRecordArraySerializer,
  cls[org.bdgenomics.adam.rdd.feature.CoverageArray] → ((kryo: Kryo) ⇒ new org.bdgenomics.adam.rdd.feature.CoverageArraySerializer(kryo)),
  cls[org.bdgenomics.adam.rdd.feature.FeatureArray] → new org.bdgenomics.adam.rdd.feature.FeatureArraySerializer,
  cls[org.bdgenomics.adam.rdd.fragment.FragmentArray] → new org.bdgenomics.adam.rdd.fragment.FragmentArraySerializer,
  cls[org.bdgenomics.adam.rdd.variant.GenotypeArray] → new org.bdgenomics.adam.rdd.variant.GenotypeArraySerializer,
  cls[org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentArray] → new org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentArraySerializer,
  cls[org.bdgenomics.adam.rdd.variant.VariantArray] → new org.bdgenomics.adam.rdd.variant.VariantArraySerializer,
  cls[org.bdgenomics.adam.rdd.variant.VariantContextArray] → new org.bdgenomics.adam.rdd.variant.VariantContextArraySerializer,

  // org.bdgenomics.adam.rdd.read
  cls[org.bdgenomics.adam.rdd.read.FlagStatMetrics],
  cls[org.bdgenomics.adam.rdd.read.DuplicateMetrics],
  cls[org.bdgenomics.adam.rdd.read.SingleReadBucket] → new org.bdgenomics.adam.rdd.read.SingleReadBucketSerializer,
  cls[org.bdgenomics.adam.rdd.read.ReferencePositionPair] → new org.bdgenomics.adam.rdd.read.ReferencePositionPairSerializer,

  // org.bdgenomics.adam.rdd.read.realignment
  cls[org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTarget] → new org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTargetSerializer,
  cls[scala.Array[org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTarget]] → new org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTargetArraySerializer,
  cls[org.bdgenomics.adam.rdd.read.realignment.TargetSet] → new org.bdgenomics.adam.rdd.read.realignment.TargetSetSerializer,

  // org.bdgenomics.adam.rdd.read.recalibration.
  cls[org.bdgenomics.adam.rdd.read.recalibration.CovariateKey],
  cls[org.bdgenomics.adam.rdd.read.recalibration.CycleCovariate],
  cls[org.bdgenomics.adam.rdd.read.recalibration.DinucCovariate],
  cls[org.bdgenomics.adam.rdd.read.recalibration.RecalibrationTable],
  cls[org.bdgenomics.adam.rdd.read.recalibration.Observation],

  // org.bdgenomics.adam.rich
  cls[org.bdgenomics.adam.rich.RichAlignmentRecord],
  cls[org.bdgenomics.adam.rich.RichVariant],

  // org.bdgenomics.adam.util
  cls[org.bdgenomics.adam.util.ReferenceContigMap] → new org.bdgenomics.adam.util.ReferenceContigMapSerializer,
  cls[org.bdgenomics.adam.util.TwoBitFile] → new org.bdgenomics.adam.util.TwoBitFileSerializer,

  // org.bdgenomics.formats.avro
  cls[org.bdgenomics.formats.avro.AlignmentRecord] → new AvroSerializer[org.bdgenomics.formats.avro.AlignmentRecord],
  cls[org.bdgenomics.formats.avro.Contig] → new AvroSerializer[org.bdgenomics.formats.avro.Contig],
  cls[org.bdgenomics.formats.avro.Dbxref] → new AvroSerializer[org.bdgenomics.formats.avro.Dbxref],
  cls[org.bdgenomics.formats.avro.Feature] → new AvroSerializer[org.bdgenomics.formats.avro.Feature],
  cls[org.bdgenomics.formats.avro.Fragment] → new AvroSerializer[org.bdgenomics.formats.avro.Fragment],
  cls[org.bdgenomics.formats.avro.Genotype] → new AvroSerializer[org.bdgenomics.formats.avro.Genotype],
  cls[org.bdgenomics.formats.avro.GenotypeAllele],
  cls[org.bdgenomics.formats.avro.GenotypeType],
  cls[org.bdgenomics.formats.avro.NucleotideContigFragment] → new AvroSerializer[org.bdgenomics.formats.avro.NucleotideContigFragment],
  cls[org.bdgenomics.formats.avro.OntologyTerm] → new AvroSerializer[org.bdgenomics.formats.avro.OntologyTerm],
  cls[org.bdgenomics.formats.avro.Read] → new AvroSerializer[org.bdgenomics.formats.avro.Read],
  cls[org.bdgenomics.formats.avro.RecordGroupMetadata] → new AvroSerializer[org.bdgenomics.formats.avro.RecordGroupMetadata],
  cls[org.bdgenomics.formats.avro.Sample] → new AvroSerializer[org.bdgenomics.formats.avro.Sample],
  cls[org.bdgenomics.formats.avro.Sequence] → new AvroSerializer[org.bdgenomics.formats.avro.Sequence],
  cls[org.bdgenomics.formats.avro.Slice] → new AvroSerializer[org.bdgenomics.formats.avro.Slice],
  cls[org.bdgenomics.formats.avro.Strand],
  cls[org.bdgenomics.formats.avro.TranscriptEffect] → new AvroSerializer[org.bdgenomics.formats.avro.TranscriptEffect],
  cls[org.bdgenomics.formats.avro.Variant] → new AvroSerializer[org.bdgenomics.formats.avro.Variant],
  cls[org.bdgenomics.formats.avro.VariantAnnotation] → new AvroSerializer[org.bdgenomics.formats.avro.VariantAnnotation],
  cls[org.bdgenomics.formats.avro.VariantAnnotationMessage],
  cls[org.bdgenomics.formats.avro.VariantCallingAnnotations] → new AvroSerializer[org.bdgenomics.formats.avro.VariantCallingAnnotations],

  // org.codehaus.jackson.node
  cls[org.codehaus.jackson.node.NullNode],
  cls[org.codehaus.jackson.node.BooleanNode],
  cls[org.codehaus.jackson.node.TextNode],

  // scala
  cls[scala.Array[htsjdk.variant.vcf.VCFHeader]],
  cls[scala.Array[java.lang.Long]],
  cls[scala.Array[java.lang.Object]],
  cls[scala.Array[org.bdgenomics.formats.avro.AlignmentRecord]],
  cls[scala.Array[org.bdgenomics.formats.avro.Contig]],
  cls[scala.Array[org.bdgenomics.formats.avro.Dbxref]],
  cls[scala.Array[org.bdgenomics.formats.avro.Feature]],
  cls[scala.Array[org.bdgenomics.formats.avro.Fragment]],
  cls[scala.Array[org.bdgenomics.formats.avro.Genotype]],
  cls[scala.Array[org.bdgenomics.formats.avro.GenotypeAllele]],
  cls[scala.Array[org.bdgenomics.formats.avro.OntologyTerm]],
  cls[scala.Array[org.bdgenomics.formats.avro.NucleotideContigFragment]],
  cls[scala.Array[org.bdgenomics.formats.avro.Read]],
  cls[scala.Array[org.bdgenomics.formats.avro.RecordGroupMetadata]],
  cls[scala.Array[org.bdgenomics.formats.avro.Sample]],
  cls[scala.Array[org.bdgenomics.formats.avro.Sequence]],
  cls[scala.Array[org.bdgenomics.formats.avro.Slice]],
  cls[scala.Array[org.bdgenomics.formats.avro.TranscriptEffect]],
  cls[scala.Array[org.bdgenomics.formats.avro.Variant]],
  cls[scala.Array[org.bdgenomics.formats.avro.VariantAnnotation]],
  cls[scala.Array[org.bdgenomics.formats.avro.VariantAnnotationMessage]],
  cls[scala.Array[org.bdgenomics.formats.avro.VariantCallingAnnotations]],
  cls[scala.Array[org.bdgenomics.adam.algorithms.consensus.Consensus]],
  cls[scala.Array[org.bdgenomics.adam.models.Coverage]],
  cls[scala.Array[org.bdgenomics.adam.models.ReferencePosition]],
  cls[scala.Array[org.bdgenomics.adam.models.ReferenceRegion]],
  cls[scala.Array[org.bdgenomics.adam.models.SequenceRecord]],
  cls[scala.Array[org.bdgenomics.adam.models.VariantContext]],
  cls[scala.Array[org.bdgenomics.adam.rdd.read.recalibration.CovariateKey]],
  cls[scala.Array[org.bdgenomics.adam.rich.RichAlignmentRecord]],
  cls[scala.Array[scala.collection.Seq[_]]],
  cls[scala.Array[Int]],
  cls[scala.Array[Long]],
  cls[scala.Array[String]],
  cls[scala.Array[Option[_]]],
  "scala.Tuple2$mcCC$sp",

  // scala.collection
  "scala.collection.Iterator$$anon$11",
  "scala.collection.Iterator$$anonfun$toStream$1",

  // scala.collection.convert
  "scala.collection.convert.Wrappers$",

  // scala.collection.immutable
  cls[scala.collection.immutable.::[_]],
  cls[scala.collection.immutable.Range],
  "scala.collection.immutable.Stream$Cons",
  "scala.collection.immutable.Stream$Empty$",
  "scala.collection.immutable.Set$EmptySet$",

  // scala.collection.mutable
  cls[scala.collection.mutable.ArrayBuffer[_]],
  cls[scala.collection.mutable.ListBuffer[_]],
  "scala.collection.mutable.ListBuffer$$anon$1",
  cls[scala.collection.mutable.WrappedArray.ofInt],
  cls[scala.collection.mutable.WrappedArray.ofLong],
  cls[scala.collection.mutable.WrappedArray.ofByte],
  cls[scala.collection.mutable.WrappedArray.ofChar],
  cls[scala.collection.mutable.WrappedArray.ofRef[_]],

  // scala.math
  scala.math.Numeric.LongIsIntegral.getClass,

  // This seems to be necessary when serializing a RangePartitioner, which writes out a ClassTag:
  //
  //  https://github.com/apache/spark/blob/v1.5.2/core/src/main/scala/org/apache/spark/Partitioner.scala#L220
  //
  // See also:
  //
  //   https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
  "scala.reflect.ClassTag$$anon$1",

  // needed for manifests
  "scala.reflect.ManifestFactory$ClassTypeManifest",

  // Added to Spark in 1.6.0; needed here for Spark < 1.6.0.
  cls[Array[Tuple1[Any]]],
  cls[Array[(Any, Any)]],
  cls[Array[(Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],
  cls[Array[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]],

  Map.empty[Any, Nothing].getClass,
  Nil.getClass,
  None.getClass,

  new reference.Registrar(),

  // https://issues.apache.org/jira/browse/SPARK-21569
  cls[TaskCommitMessage],
  cls[LociSet]
)
