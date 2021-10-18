package org.apache.spark.sql.sparkcube.util

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonMappingException, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.io.IOException
import java.nio.ByteBuffer
import org.apache.spark.internal.Logging
import scala.util.control.NonFatal

object JacksonUtil extends Logging {

  private val _mapper = new ObjectMapper()
  _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  _mapper.registerModule(DefaultScalaModule)

  def toJson[T](obj: T): String = {
    _mapper.writeValueAsString(obj)
  }

  def fromJson[T](json: String, `class`: Class[T]): T = {
    try {
      _mapper.readValue(json, `class`)
    } catch {
      case NonFatal(e) =>
        logError(s"can not convert json: [ $json ] to class [ ${`class`} ].", e)
        null.asInstanceOf[T]
    }
  }

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    try {
      _mapper.readValue(json, m.runtimeClass).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        logError(s"can not convert json: [ $json ] to class [ ${m.runtimeClass} ].", e)
        null.asInstanceOf[T]
    }
  }

  def fromJson[T](jsonNode: JsonNode)(implicit m: Manifest[T]): T = {
    try {
      _mapper.readerFor(m.runtimeClass).readValue(jsonNode)
    } catch {
      case NonFatal(e) =>
        logError(s"can not convert json: [ $jsonNode ] to class [ ${m.runtimeClass} ].", e)
        null.asInstanceOf[T]
    }
  }

  def prettyPrint[T](obj: T): String = {
    _mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
  }

  def serialize[K](data: K): Array[Byte] = {
    _mapper.writeValueAsBytes(data)
  }

  def serializeByteBuffer[K](data: K): ByteBuffer = {
    ByteBuffer.wrap(_mapper.writeValueAsBytes(data))
  }

  @throws(classOf[IOException])
  @throws(classOf[JsonParseException])
  @throws(classOf[JsonMappingException])
  def deserialize[K](bytes: Array[Byte], `class`: Class[K]): K = {
    _mapper.readValue(bytes, `class`)
  }

  @throws(classOf[IOException])
  @throws(classOf[JsonParseException])
  @throws(classOf[JsonMappingException])
  def deserializeByteBuffer[K](buffer: ByteBuffer, `class`: Class[K]): K = {
    _mapper.readValue(buffer.array(), `class`)
  }

  def serializeObject[K](obj: K, `class`: Class[K]): Array[Byte] = {
    _mapper.writerFor(`class`).writeValueAsBytes(obj)
  }

  def deserializeObject[K](bytes: Array[Byte], `class`: Class[K]): K = {
    _mapper.readerFor(`class`).readValue(bytes)
  }
}
