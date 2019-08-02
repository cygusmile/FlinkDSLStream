package com.ximalaya.flink.dsl.stream.parser

import com.ximalaya.flink.dsl.stream.`type`.{FieldType, SourceField}
import com.ximalaya.flink.dsl.stream.`type`.FieldType._
import org.parboiled2._

/**
  *
  * @author martin.don
  *
  **/
case class HBaseSourceField(columnFamily:String,
                            column:String,
                            alias:Option[String],
                            fieldType:FieldType,
                            default:Option[AnyRef]){
  override def equals(obj: scala.Any): Boolean = {
      obj match {
        case other:HBaseSourceField⇒ columnFamily == other.columnFamily && column == other.column &&
          alias == other.alias && fieldType == other.fieldType && com.ximalaya.flink.dsl.stream.`type`.
          SourceField.equalsDefaultValue(default,other.default)
        case _ ⇒ false
      }
  }
}

case class HBaseSinkField(name:String,columnFamily:String,column:String)

case class MysqlSourceField(column:String,
                            alias:Option[String])

case class MysqlSinkField(name:String,column:String)

case class RedisStringSourceField(column:String, fieldType: FieldType)

case class RedisHashSourceField(column:String,
                                alias:Option[String],
                                fieldType: FieldType,
                                default:Option[String]){
  override def equals(obj: scala.Any): Boolean = {
     obj match {
       case other:RedisHashSourceField ⇒ column == other.column && alias == other.alias && fieldType == other.fieldType && com.ximalaya.flink.dsl.stream.`type`.
         SourceField.equalsDefaultValue(default,other.default)
       case _ ⇒ false

     }
  }
}

case class RedisHashSinkField(name:String,columnName:String)

case class SinkField(name:String,path:Option[List[String]],columnName:String)

object SourceField{
  def apply(paths:Seq[String],aliasName:String,fieldType:String,default:String):SourceField={
    val (mayPaths,fieldName)=paths.splitPaths
    //extract alias name
    val mayAlias = if(aliasName.trim=="") None else Some(aliasName.trim)
    //extract field type
    val `type` = fieldType.toFieldType

    var defaultOption: Option[AnyRef] = None
    if(default.trim != "") {
      defaultOption = Some(default.parseDefault(`type`))
    }
    com.ximalaya.flink.dsl.stream.`type`.SourceField.constructCreateField(mayPaths.map(_.toArray),fieldName,mayAlias,defaultOption,`type`)
  }
}

class ParseWrong(val message:java.lang.String) extends Exception(message)

trait DslStreamParser extends Parser{

  /**
    * 解析动作
    * @return
    */
  def action:Rule0 = MATCH

  /**
    * 匹配optional 并将值押入堆栈
    * @return
    */
  def optional:Rule1[Map[String,String]]=rule{
    "optional" ~ mayWhiteSpace ~ '(' ~ mayWhiteSpace ~ keyValue.+.separatedBy(mayWhiteSpace~','~mayWhiteSpace)~ mayWhiteSpace ~ ')' ~> ((kvs:Seq[(String,String)]) ⇒{
      push(kvs.toMap)
    })
  }

  /**
    * 匹配布尔值
    * @return
    */
  def bool:Rule0 = rule {
    str("true") | str("false")
  }

  /**
    * 匹配空值
    */
  def empty:Rule0 = rule {
    str("null")
  }

  /**
    * 匹配一个字符串
    * @return
    */
  def string:Rule0=rule{
    ch(''') ~ noneOf("'").* ~ ch(''')
  }

  /**
    * 匹配一个字符串 并推入值栈
    * @return
    */
  def captureString:Rule1[String] = rule{
    capture(string)
  }

  /**
    * 匹配选项的键值规则
    * @return
    */
  def keyValue:Rule1[(String,String)] = rule {
    capture(oneOrMore(CharPredicate.AlphaNum.+("_$")) ~ mayWhiteSpace ~
      '=' ~ mayWhiteSpace ~string ) ~> ((kv:String)⇒{
       val fields=kv.split("=",2)
      (fields.head.cleanString(),fields.last.cleanString( true))
    })
  }

  /**
    * 匹配0个或多个空白符
    * @return
    */
  def mayWhiteSpace:Rule0 = rule {
    anyOf(" \t\n").*
  }

  /**
    * 匹配1个或多个空白符
    * @return
    */
  def mustWhiteSpace:Rule0 = rule {
    anyOf(" \t\n").+
  }

  /**
    * 匹配单个路径名规则 并将路径名推入值栈
    * @return
    */
  def path:Rule1[String] = rule {
    capture(oneOrMore(CharPredicate.AlphaNum.+("_$")))
  }


  /**
    * 匹配路径别名规则 并将别名推入值栈
    * @return
    */
  def alias:Rule1[String] = rule {
    path ~> ((name: String) ⇒ {
      if(name.matches("\\d+")){
        throw new RuntimeException("field alias name cannot be all digits: "+name)
      }
      push(name)
    })
  }


  /**
    * 匹配默认值 并将默认值推入值栈
    * @return
    */
  def default:Rule1[String] = rule {
      capture(ch(''') ~ noneOf("'").* ~ ch(''')) |
      capture(ch('[') ~ noneOf("]").* ~ ch(']')) |
      capture(ch('{') ~ noneOf("}").* ~ ch('}')) |
      capture(optional( ch('-') | ch('+')) ~ CharPredicate.Digit.+ ~ optional(ch('.') ~ CharPredicate.Digit.+)) |
      capture(bool) | capture(empty)
  }



  def defaultHelperOne:Rule1[String]=rule{
    mustWhiteSpace ~ default ~ mayWhiteSpace
  }

  def defaultHelperTwo:Rule1[String] = rule{
    defaultHelperOne | capture(mayWhiteSpace)
  }

  def aliasHelperOne:Rule1[String] = rule{
    mustWhiteSpace ~ as ~ mustWhiteSpace ~ alias ~ mustWhiteSpace
  }

  def aliasHelperTwo:Rule1[String] = rule {
    aliasHelperOne | capture(mustWhiteSpace)
  }

  /**
    * 匹配一个源表字段定义 并将字段定义值推入值栈
    * @return
    */
  def sourceFieldDefinition:Rule1[SourceField]= rule{
      morePath ~ aliasHelperTwo ~
        fieldType ~ defaultHelperTwo ~> ((paths:Seq[String],aliasName:String,fieldType:String,default:String)⇒{
        push(SourceField(paths,aliasName,fieldType,default))
    })
  }

  /**
    * 匹配一个结果表字段定义 并将字段定义值推入值栈
    * @return
    */
  def sinkFieldDefinition:Rule1[SinkField] = rule {
    path ~ mustWhiteSpace ~ "as" ~ mustWhiteSpace ~ morePath ~> ((name: String, paths: Seq[String]) ⇒ {
      val (p, n) = paths.splitPaths
      push(SinkField(name, p, n))
    })
  }

  /**
    * 匹配键信息 并将键值推入值栈
    * @return
    */
  def key:Rule1[String] = rule {
    "key" ~ mayWhiteSpace ~ "(" ~mayWhiteSpace ~ alias ~ mayWhiteSpace ~")"
  }

  /**
    *  匹配多个路径名规则 并将多个路径名推入值栈
    * @return
    */
  def morePath:Rule1[Seq[String]] = rule {
     path.+('.')
  }

  /**
    * 匹配as字符串规则
    * @return
    */
  def as:Rule0= rule {
    ignoreCase("as")
  }


  /**
    * 匹配类型定义
    * @return
    */
  def fieldType:Rule1[String] = rule{
    capture(ignoreCase(BOOL.getTypeName) | ignoreCase(INT_ARRAY.getTypeName) | ignoreCase(LONG_ARRAY.getTypeName) |
      ignoreCase(DOUBLE_ARRAY.getTypeName) | ignoreCase(FLOAT_ARRAY.getTypeName) | ignoreCase(STRING_ARRAY.getTypeName) |
      ignoreCase(BYTE_ARRAY.getTypeName) | ignoreCase(OBJECT_ARRAY.getTypeName) | ignoreCase(INT.getTypeName) |
      ignoreCase(LONG.getTypeName) | ignoreCase(DOUBLE.getTypeName) | ignoreCase(FLOAT.getTypeName) |
    ignoreCase(STRING.getTypeName) | ignoreCase(BYTE.getTypeName) | ignoreCase(OBJECT.getTypeName) |
    ignoreCase(MAP.getTypeName))
  }

  /**
    * 处理错误
    * @return
    */
  def convertError[T](failed:Throwable):Either[ParseWrong,T]={
    failed match {
      case _:ParseError ⇒ Left(new ParseWrong(formatError(failed.asInstanceOf[ParseError])))
      case _ ⇒ Left(new ParseWrong(failed.getMessage))
    }
  }
}
