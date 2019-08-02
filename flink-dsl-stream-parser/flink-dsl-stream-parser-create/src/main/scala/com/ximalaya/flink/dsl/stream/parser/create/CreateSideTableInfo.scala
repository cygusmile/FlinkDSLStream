package com.ximalaya.flink.dsl.stream.parser.create

import com.ximalaya.flink.dsl.stream.`type`.SourceField
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.api.field.encoder.FieldEncoder
import com.ximalaya.flink.dsl.stream.side.{All, CacheStrategy, PhysicsSideInfo, SideTableInfo}
import com.ximalaya.flink.dsl.stream.parser._
import java.util.{Map ⇒ JMap}

import com.google.common.collect.Maps
/**
  *
  * @author martin.dong
  *
  **/


//维表创建

object CreateSideTableInfo {




  def toLogicSchema(physicsSchema: Seq[SourceField]):JMap[String,Class[_]]={
    val javaMap=Maps.newHashMap[String,Class[_]]()
    physicsSchema.map(f⇒(f.getAliasName.getOrElse(f.getFieldName),f.getFieldType.getClazz)).foreach{
      case (k,v)⇒javaMap.put(k,v)
    }
    javaMap
  }

  /**
    *
    * @param tableName     表名
    * @param physicsSchema 字段信息
    * @param key           键信息
    * @param cacheStrategy 缓存策略
    * @param required      必填参数
    * @param sideProps     可选参数
    * @return
    */
  def apply(tableName: String,
            physicsSchema: Seq[SourceField],
            key: String,
            cacheStrategy: CacheStrategy,
            required: SideRequired,
            sideProps: Map[String, String],
            dynamicEncoders: Map[String, FieldEncoder],
            dynamicDecoders: Map[String, FieldDecoder]): SideTableInfo = {
    required.sourceType match {
      case "hbase" ⇒
        val physicsSideInfo = PhysicsSideInfo(required.sourceType, key,
          sideProps,
          physicsSchema,
          Some(required.keyEncoder.toEncoder(dynamicEncoders)),
          Some(sideProps("decoder").toDecoder(dynamicDecoders)))
        SideTableInfo(tableName, physicsSideInfo, toLogicSchema(physicsSchema), key,
          cacheStrategy, physicsSchema.toJavaList, required.castError)
      case "mysql" ⇒
        val physicsSideInfo = PhysicsSideInfo(required.sourceType, key,
          sideProps, physicsSchema, None, None)
        SideTableInfo(tableName, physicsSideInfo, toLogicSchema(physicsSchema),
          key, cacheStrategy, physicsSchema.toJavaList, required.castError)
      case "redis" ⇒
        val physicsSideInfo = PhysicsSideInfo(required.sourceType, key, sideProps, physicsSchema,
          Some(required.keyEncoder.toEncoder(dynamicEncoders)), Some(sideProps("decoder").toDecoder(dynamicDecoders)))
        SideTableInfo(tableName, physicsSideInfo, toLogicSchema(physicsSchema),
          key, cacheStrategy, physicsSchema.toJavaList, required.castError)
      case "hdfs" ⇒
        val physicsSideInfo = PhysicsSideInfo(required.sourceType, key, sideProps, physicsSchema, None, None)
        SideTableInfo(tableName, physicsSideInfo, toLogicSchema(physicsSchema),
          key, cacheStrategy, physicsSchema.toJavaList, required.castError)
    }
  }
}


