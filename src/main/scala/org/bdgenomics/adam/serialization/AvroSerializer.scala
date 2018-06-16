package org.bdgenomics.adam.serialization

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import it.unimi.dsi.fastutil.io.{ FastByteArrayInputStream, FastByteArrayOutputStream }
import org.apache.avro.io.{ BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory }
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter, SpecificRecord }

import scala.reflect.ClassTag
// NOTE: This class is not thread-safe; however, Spark guarantees that only a single thread will access it.
class AvroSerializer[T <: SpecificRecord: ClassTag] extends Serializer[T] {
  val reader = new SpecificDatumReader[T](scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]])
  val writer = new SpecificDatumWriter[T](scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]])
  var in = InputStreamWithDecoder(1024)
  val outstream = new FastByteArrayOutputStream()
  val encoder = EncoderFactory.get().directBinaryEncoder(outstream, null.asInstanceOf[BinaryEncoder])

  setAcceptsNull(false)

  def write(kryo: Kryo, kryoOut: Output, record: T) = {
    outstream.reset()
    writer.write(record, encoder)
    kryoOut.writeInt(outstream.array.length, true)
    kryoOut.write(outstream.array)
  }

  def read(kryo: Kryo, kryoIn: Input, klazz: Class[T]): T = this.synchronized {
    val len = kryoIn.readInt(true)
    if (len > in.size) {
      in = InputStreamWithDecoder(len + 1024)
    }
    in.stream.reset()
    // Read Kryo bytes into input buffer
    kryoIn.readBytes(in.buffer, 0, len)
    // Read the Avro object from the buffer
    reader.read(null.asInstanceOf[T], in.decoder)
  }
}

case class InputStreamWithDecoder(size: Int) {
  val buffer = new Array[Byte](size)
  val stream = new FastByteArrayInputStream(buffer)
  val decoder = DecoderFactory.get().directBinaryDecoder(stream, null.asInstanceOf[BinaryDecoder])
}
