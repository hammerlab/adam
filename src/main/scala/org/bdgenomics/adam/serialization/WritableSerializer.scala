package org.bdgenomics.adam.serialization

import com.esotericsoftware.kryo.io.{ Input, KryoDataInput, KryoDataOutput, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.apache.hadoop.io.Writable

/**
 * A Kryo serializer for Hadoop writables.
 *
 * Lifted from the Apache Spark user email list
 * (http://apache-spark-user-list.1001560.n3.nabble.com/Hadoop-Writable-and-Spark-serialization-td5721.html)
 * which indicates that it was originally copied from Shark itself, back when
 * Spark 0.9 was the state of the art.
 *
 * @tparam T The class to serialize, which implements the Writable interface.
 */
class WritableSerializer[T <: Writable] extends Serializer[T] {
  override def write(kryo: Kryo, output: Output, writable: T) {
    writable.write(new KryoDataOutput(output))
  }

  override def read(kryo: Kryo, input: Input, cls: java.lang.Class[T]): T = {
    val writable = cls.newInstance()
    writable.readFields(new KryoDataInput(input))
    writable
  }
}
