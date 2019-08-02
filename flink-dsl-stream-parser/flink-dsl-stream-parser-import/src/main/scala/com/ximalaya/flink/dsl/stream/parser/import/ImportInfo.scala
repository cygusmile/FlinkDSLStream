package com.ximalaya.flink.dsl.stream.parser.`import`

import com.ximalaya.flink.dsl.stream.`type`.FieldType

/**
  *
  * @author martin.dong
  *
  **/

case class ImportInfo(`type`:String,clazzName:String,typeValues:List[(FieldType,String)],udfName:String)
