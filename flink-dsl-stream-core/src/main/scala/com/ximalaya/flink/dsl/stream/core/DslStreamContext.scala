package com.ximalaya.flink.dsl.stream.core

import com.ximalaya.flink.dsl.stream.api.message.encoder.MessageEncoder
import com.ximalaya.flink.dsl.stream.api.message.decoder.MessageDecoder
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.api.field.encoder.FieldEncoder
/**
  *
  * @author martin.dong
  *
  **/
class DslStreamContext(val dslSteamClassLoader:ClassLoader,
                       val dynamicDecoders:Map[String,FieldDecoder] = Map(),
                       val dynamicEncoders:Map[String,FieldEncoder] = Map(),
                       val dynamicSourceDataTypes:Map[String,MessageDecoder[_]] = Map(),
                       val dynamicSinkDataTypes:Map[String,MessageEncoder] = Map()) {


}
