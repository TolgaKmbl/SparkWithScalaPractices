package com.tolgakumbul

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.core.JsonProcessingException
import scala.collection.mutable

object JsonConverterHelper {
  private val mapperInstances = mutable.Map[Class[_], ObjectMapper]()

  def convertToJson(obj: Any): String = {
    if (obj == null) {
      println("JsonConverterHelper error on convertToJson: Object is null")
      throw new RuntimeException("JsonConverterHelper error on convertToJson")
    }

    val mapperInstance = getInstance(obj.getClass)

    try {
      mapperInstance.writeValueAsString(obj)
    } catch {
      case e: JsonProcessingException =>
        println(s"JsonConverterHelper error on convertToJsonString ${e.getMessage}")
        throw new RuntimeException(e)
    }
  }

  private def getInstance(clazz: Class[_]): ObjectMapper = {
    mapperInstances.getOrElse(clazz, {
      val mapper = new ObjectMapper()
      mapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
      mapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
      mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
      mapper.registerModule(new DefaultScalaModule)
      mapper.registerModule(new SimpleModule)
      mapperInstances(clazz) = mapper
      mapper
    })
  }
}

