package com.ximalaya.flink.dsl.stream.udf

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.google.common.collect.Lists
import org.apache.flink.table.functions.ScalarFunction
import java.util.{Map ⇒ JMap}
import java.lang.{Long ⇒ JLong}
import java.lang.{Double ⇒ JDouble}
import java.lang.{Float ⇒ JFloat}
import java.lang.{Boolean ⇒ JBool}

import com.ximalaya.flink.dsl.stream.udf.annotation.UdfInfo

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
/**
  *
  * @author martin.dong
  *
  **/
object ArrayFunctions {

  /**
    *计算数组长度JSONObject
    */
  @UdfInfo(name="arraySize",desc="compute array size")
  class  ArraySize extends ScalarFunction{
    /*
     * 数值类型
     */
    def eval(array: Array[Int]):Int = {
      array.length
    }
    def eval(array: Array[Long]):Int = {
      array.length
    }
    def eval(array: Array[Double]):Int = {
      array.length
    }
    def eval(array: Array[Float]):Int = {
      array.length
    }
    def eval(array: Array[Byte]):Int = {
      array.length
    }
    def eval(array: Array[Boolean]):Int={
      array.length
    }
    /**
      * 引用类型
      */
    def eval(array: Array[Any]):Int = {
      array.length
    }
  }

  /**
    * 数组求和
    */
  @UdfInfo(name = "arraySum",desc = "calculate the sum of an array")
  class ArraySum extends ScalarFunction{
    def eval(array: Array[Int]):Int = {
      array.sum
    }
    def eval(array: Array[Long]):Long = {
      array.sum
    }
    def eval(array: Array[Double]):Double = {
      array.sum
    }
    def eval(array: Array[Float]):Float = {
      array.sum
    }
    def eval(array: Array[Array[Int]]):Int = {
      array.flatten.sum
    }
    def eval(array: Array[Array[Long]]):Long = {
      array.flatten.sum
    }
    def eval(array: Array[Array[Double]]):Double = {
      array.flatten.sum
    }
    def eval(array: Array[Array[Float]]):Float = {
      array.flatten.sum
    }
  }

  /**
    * 返回索引位置的数组元素
    */
  @UdfInfo(name="arrayIndex",desc="returns the array element at the index position")
  class ArrayIndex extends ScalarFunction{
    /**
      * 基本类型
      */
    def eval(array:Array[Int],index:Int):Int={
      array(index)
    }
    def eval(array:Array[Long],index:Int):Long={
      array(index)
    }
    def eval(array:Array[Float],index:Int):Float={
      array(index)
    }
    def eval(array:Array[Double],index:Int):Double={
      array(index)
    }
    def eval(array:Array[Boolean],index:Int):Boolean={
      array(index)
    }
    def eval(array:Array[String],index:Int):String={
      array(index)
    }
    def eval(array:Array[Byte],index:Int):Byte={
      array(index)
    }
    def eval(array:Array[Any],index:Int):Any={
      array(index)
    }
    /**
      * 数组类型
      */
    def eval(array: Array[Array[Int]],index:Int):Array[Int] = {
      array(index)
    }
    def eval(array: Array[Array[Long]],index:Int):Array[Long] = {
      array(index)
    }
    def eval(array: Array[Array[Double]],index:Int):Array[Double]= {
      array(index)
    }
    def eval(array: Array[Array[Float]],index:Int):Array[Float] = {
      array(index)
    }
    def eval(array: Array[Array[Byte]],index:Int):Array[Byte] = {
      array(index)
    }
    def eval(array: Array[Array[String]],index:Int):Array[String]= {
      array(index)
    }
    def eval(array: Array[Array[Object]],index:Int):Array[Object] = {
      array(index)
    }
    /**
      * Map类型
      */
    def eval(array: Array[JMap[_,_]],index:Int):JMap[_,_] = {
      array(index)
    }
  }

  /**
    * 连接一个数组
    */
  @UdfInfo(name="arrayJoin",desc="connect an array")
  class ArrayJoin extends ScalarFunction{
    /**
      * 基本类型
      */
    def eval(array:Array[Int],separator:String):String={
      array.mkString(separator)
    }
    def eval(array:Array[Long],separator:String):String={
      array.mkString(separator)
    }
    def eval(array:Array[Float],separator:String):String={
      array.mkString(separator)
    }
    def eval(array:Array[Double],separator:String):String={
      array.mkString(separator)
    }
    def eval(array:Array[Boolean],separator:String):String={
      array.mkString(separator)
    }
    def eval(array:Array[Any],separator:String):String={
      array.mkString(separator)
    }
  }

  /**
    * 获取一个数组头部元素
    */
  @UdfInfo(name="arrayHead",desc="get an array header element")
  class ArrayHead extends ScalarFunction{
    /**
      * 基本类型
      */
    def eval(array:Array[Int]):Int={
      array.head
    }
    def eval(array:Array[Long]):Long={
      array.head
    }
    def eval(array:Array[Float]):Float={
      array.head
    }
    def eval(array:Array[Double]):Double={
      array.head
    }
    def eval(array:Array[Boolean]):Boolean={
      array.head
    }
    def eval(array:Array[String]):String={
      array.head
    }
    def eval(array:Array[Byte]):Byte={
      array.head
    }
    def eval(array:Array[Any]):Any={
      array.head
    }
    /**
      * 数组类型
      */
    def eval(array: Array[Array[Int]]):Array[Int] = {
      array.head
    }
    def eval(array: Array[Array[Long]]):Array[Long] = {
      array.head
    }
    def eval(array: Array[Array[Double]]):Array[Double]= {
      array.head
    }
    def eval(array: Array[Array[Float]]):Array[Float] = {
      array.head
    }
    def eval(array: Array[Array[Byte]]):Array[Byte] = {
      array.head
    }
    def eval(array: Array[Array[String]]):Array[String]= {
      array.head
    }
    def eval(array: Array[Array[Object]]):Array[Object] = {
      array.head
    }
    /**
      * Map类型
      */
    def eval(array: Array[JMap[_,_]]):JMap[_,_] = {
      array.head
    }
  }

  /**
    * 计算数组各个值出现的频度
    */
  @UdfInfo(name="arrayGroupCount",desc="get an array header element")
  class ArrayGroupCount extends ScalarFunction{
    def eval(array:Array[Int]):Map[Int,Int]={
      array.groupBy(e⇒e).mapValues(_.length)
    }
    def eval(array:Array[Long]):Map[Long,Int]={
      array.groupBy(e⇒e).mapValues(_.length)
    }
    def eval(array:Array[Float],index:Int):Map[Float,Int]={
      array.groupBy(e⇒e).mapValues(_.length)
    }
    def eval(array:Array[Double]):Map[Double,Int]={
      array.groupBy(e⇒e).mapValues(_.length)
    }
    def eval(array:Array[Boolean]):Map[Boolean,Int]={
      array.groupBy(e⇒e).mapValues(_.length)
    }
    def eval(array:Array[String]):Map[String,Int]={
      array.groupBy(e⇒e).mapValues(_.length)
    }
    def eval(array:Array[Byte]):Map[Byte,Int]={
      array.groupBy(e⇒e).mapValues(_.length)
    }
    def eval(array:Array[Any]):Map[Any,Int]={
      array.groupBy(e⇒e).mapValues(_.length)
    }
  }

  /**
    * 对一个数组去重
    */
  @UdfInfo(name="arrayDistinct",desc="remove duplicates from an array")
  class ArrayDistinct extends ScalarFunction{
    def eval(array:Array[Int]):Array[Int]={
      array.distinct
    }
    def eval(array:Array[Long]):Array[Long]={
      array.distinct
    }
    def eval(array:Array[Float],index:Int):Array[Float]={
      array.distinct
    }
    def eval(array:Array[Double]):Array[Double]={
      array.distinct
    }
    def eval(array:Array[Boolean]):Array[Boolean]={
      array.distinct
    }
    def eval(array:Array[String]):Array[String]={
      array.distinct
    }
    def eval(array:Array[Any]):Array[Any]={
      array.distinct
    }
  }

  /**
    * 计算数组中出现次数最高的元素
    */
  @UdfInfo(name="arrayMost",desc="calculate the most frequently occurring element in the array")
  class ArrayMost extends ScalarFunction{
    def evalHelp[T](array:Array[T]):T={
      array.groupBy(e⇒e).map{
        case (key,value)⇒(key,value.length)
      }.toList.sortBy(_._2)(Ordering.Int.reverse).head._1
    }
    def eval(array:Array[Int]):Int={
      evalHelp(array)
    }
    def eval(array:Array[Long]):Long={
      evalHelp(array)
    }
    def eval(array:Array[Float]):Float={
      evalHelp(array)
    }
    def eval(array:Array[Double]):Double={
      evalHelp(array)
    }
    def eval(array:Array[Boolean]):Boolean={
      evalHelp(array)
    }
    def eval(array:Array[String]):String={
      evalHelp(array)
    }
    def eval(array:Array[Any]):Any={
      evalHelp(array)
    }
  }

  /**
    * 合并多个数组
    */
  @UdfInfo(name="arrayMerge",desc="merge multiple arrays")
  class ArrayMerge extends ScalarFunction{
    def evalHelper[T](arrays:Array[T]*)(implicit tag:ClassTag[T]):Array[T]={
      val result = ArrayBuffer[T]()
      arrays.foreach(e⇒result.append(e:_*))
      result.toArray
    }
    def eval(arrays:Array[Int]*):Array[Int]={
       evalHelper(arrays:_*)
    }
    def eval(arrays:Array[Long]*):Array[Long]={
      evalHelper(arrays:_*)
    }
    def eval(arrays:Array[Float]*):Array[Float]={
      evalHelper(arrays:_*)
    }
    def eval(arrays:Array[Double]*):Array[Double]={
      evalHelper(arrays:_*)
    }
    def eval(arrays:Array[Boolean]*):Array[Boolean]={
      evalHelper(arrays:_*)
    }
    def eval(arrays:Array[String]*):Array[String]={
      evalHelper(arrays:_*)
    }
    def eval(arrays:Array[Byte]*):Array[Byte]={
      evalHelper(arrays:_*)
    }
    def eval(arrays:Array[Any]*):Array[Any]={
      evalHelper(arrays:_*)
    }
  }

  /**
    *对数组进行过滤
    */
  @UdfInfo(name="arrayFilter",
    desc="filter arrays through a filter function",
    constructFields = "intFilters,longFilters,floatFilters,doubleFilers,stringFilters,anyFilters")
  class ArrayFilter(intFilters:Map[String,Int⇒Boolean],
                    longFilters:Map[String,Long⇒Boolean],
                    floatFilters:Map[String,Float⇒Boolean],
                    doubleFilers:Map[String,Double⇒Boolean],
                    stringFilters:Map[String,String⇒Boolean],
                    anyFilters:Map[String,Any⇒Boolean]) extends ScalarFunction{
    def eval(array:Array[Int],filter:String):Array[Int]={
        array.filter(intFilters(filter))
    }
    def eval(array:Array[Long],filter:String):Array[Long]={
      array.filter(longFilters(filter))
    }
    def eval(array:Array[Float],filter:String):Array[Float]={
      array.filter(floatFilters(filter))
    }
    def eval(array:Array[Double],filter:String):Array[Double]={
      array.filter(doubleFilers(filter))
    }
    def eval(array:Array[String],filter:String):Array[String]={
      array.filter(stringFilters(filter))
    }
    def eval(array:Array[Any],filter:String):Array[Any]={
      array.filter(anyFilters(filter))
    }
  }


  /**
    *对数组进行映射
    */
  @UdfInfo(name="arrayMap",
    desc="mapping arrays through a map function",
    constructFields = "intMappers,longMappers,floatMappers,doubleMappers,stringMappers,anyMappers")
  class ArrayMap(intMappers:Map[String,Int⇒Int],
                    longMappers:Map[String,Long⇒Long],
                    floatMappers:Map[String,Float⇒Float],
                    doubleMappers:Map[String,Double⇒Double],
                    stringMappers:Map[String,String⇒String],
                    anyMappers:Map[String,Any⇒Any]) extends ScalarFunction{
    def eval(array:Array[Int],mapper:String):Array[Int]={
      array.map(intMappers(mapper))
    }
    def eval(array:Array[Long],mapper:String):Array[Long]={
      array.map(longMappers(mapper))
    }
    def eval(array:Array[Float],mapper:String):Array[Float]={
      array.map(floatMappers(mapper))
    }
    def eval(array:Array[Double],mapper:String):Array[Double]={
      array.map(doubleMappers(mapper))
    }
    def eval(array:Array[String],mapper:String):Array[String]={
      array.map(stringMappers(mapper))
    }
    def eval(array:Array[Any],mapper:String):Array[Any]={
      array.map(anyMappers(mapper))
    }
  }

  /**
    *对数组进行聚合
    */
  @UdfInfo(name="arrayAggregate",
    desc="aggregating arrays through a aggregate function",
    constructFields = "intAggregators,longAggregators,floatAggregators,doubleAggregators,stringAggregators,anyAggregators")
  class ArrayAggregate(intAggregators:Map[String,(Int,Int)⇒Int],
                    longAggregators:Map[String,(Long,Long)⇒Long],
                    floatAggregators:Map[String,(Float,Float)⇒Float],
                    doubleAggregators:Map[String,(Double,Double)⇒Double],
                    stringAggregators:Map[String,(String,String)⇒String],
                    anyAggregators:Map[String,(Any,Any)⇒Any]) extends ScalarFunction{
    def eval(array:Array[Int],default:Int,aggregator:String):Int={
      array.fold(default)(intAggregators(aggregator))
    }
    def eval(array:Array[Long],default:Long,aggregator:String):Long={
      array.fold(default)(longAggregators(aggregator))
    }
    def eval(array:Array[Float],default:Float,aggregator:String):Float={
      array.fold(default)(floatAggregators(aggregator))
    }
    def eval(array:Array[Double],default:Double,aggregator:String):Double={
      array.fold(default)(doubleAggregators(aggregator))
    }
    def eval(array:Array[String],default:String,aggregator:String):String={
      array.fold(default)(stringAggregators(aggregator))
    }
    def eval(array:Array[Any],default:Any,aggregator:String):Any={
      array.fold(default)(anyAggregators(aggregator))
    }
  }

  /**
    * long型数组和json字符串转换
    */
  @UdfInfo(name="jsonLongArrayConvert",desc="long array and json string conversion")
  class JsonLongArrayConvert extends ScalarFunction{
    def eval(json:String):Array[Long] = {
      JSON.parseArray(json,classOf[JLong]).toArray(Array[JLong]()).map(e⇒Long.unbox(e))
    }
    def eval(array:Array[Long]):String = {
      new JSONArray().fluentAddAll(Lists.newArrayList(array.map(e⇒Long.box(e)):_*)).toJSONString
    }
  }

  /**
    * int型数组和json字符串转换
    */
  @UdfInfo(name="jsonIntArrayConvert",desc="int array and json string conversion")
  class JsonIntArrayConvert extends ScalarFunction{
    def eval(json:String):Array[Int] = {
      JSON.parseArray(json,classOf[Integer]).toArray(Array[Integer]()).map(e⇒Int.unbox(e))
    }
    def eval(array:Array[Int]):String = {
      new JSONArray().fluentAddAll(Lists.newArrayList(array.map(e⇒Int.box(e)):_*)).toJSONString
    }
  }

  /**
    * double型数组和json字符串转换
    */
  @UdfInfo(name="jsonDoubleArrayConvert",desc="double array and json string conversion")
  class JsonDoubleArrayConvert extends ScalarFunction{
    def eval(json:String):Array[Double] = {
      JSON.parseArray(json,classOf[JDouble]).toArray(Array[JDouble]()).map(e⇒Double.unbox(e))
    }
    def eval(array:Array[Double]):String = {
      new JSONArray().fluentAddAll(Lists.newArrayList(array.map(e⇒Double.box(e)):_*)).toJSONString
    }
  }

  /**
    * float型数组和json字符串转换
    */
  @UdfInfo(name="jsonFloatArrayConvert",desc="float array and json string conversion")
  class JsonFloatArrayConvert extends ScalarFunction{
    def eval(json:String):Array[Float] = {
      JSON.parseArray(json,classOf[JFloat]).toArray(Array[JFloat]()).map(e⇒Float.unbox(e))
    }
    def eval(array:Array[Float]):String = {
      new JSONArray().fluentAddAll(Lists.newArrayList(array.map(e⇒Float.box(e)):_*)).toJSONString
    }
  }

  /**
    * string型数组和json字符串转换
    */
  @UdfInfo(name="jsonStringArrayConvert",desc="string array and json string conversion")
  class JsonStringArrayConvert extends ScalarFunction{
    def eval(json:String):Array[String] = {
      JSON.parseArray(json,classOf[String]).toArray(Array[String]())
    }
    def eval(array:Array[String]):String = {
      new JSONArray().fluentAddAll(Lists.newArrayList(array:_*)).toJSONString
    }
  }

  /**
    * bool型数组和json字符串转换
    */
  @UdfInfo(name="jsonBoolArrayConvert",desc="bool array and json string conversion")
  class JsonBooleanArrayConvert extends ScalarFunction{
    def eval(json:String):Array[Boolean] = {
      JSON.parseArray(json,classOf[JBool]).toArray(Array[JBool]()).map(e⇒Boolean.unbox(e))
    }
    def eval(array:Array[Boolean]):String = {
      new JSONArray().fluentAddAll(Lists.newArrayList(array:_*)).toJSONString
    }
  }

  /**
    * 数组型数组和json字符串转换
    */
  @UdfInfo(name="jsonArrayArrayConvert",desc="array array and json string conversion")
  class JsonArrayArrayConvert extends ScalarFunction{
    def eval(json:String):Array[JSONArray] = {
      JSON.parseArray(json,classOf[JSONArray]).toArray(Array[JSONArray]())
    }
    def eval(array:Array[JSONArray]):String = {
      new JSONArray().fluentAddAll(Lists.newArrayList(array:_*)).toJSONString
    }
  }

  class JsonMapArrayConvert extends ScalarFunction{
    def eval(json:String):Array[JSONObject] = {
      JSON.parseArray(json,classOf[JSONObject]).toArray(Array[JSONObject]())
    }
    def eval(array:Array[JSONObject]):String = {
      new JSONArray().fluentAddAll(Lists.newArrayList(array:_*)).toJSONString
    }
  }

  class ArrayJsonMapName extends ScalarFunction{
    /*
   * 数值类型
   */

  }


  /**
    * 拼接多个值为一个数组
    */
  @UdfInfo(name="arraySlice",desc = "splicing multiple values into an array")
  class ArraySlice extends ScalarFunction{
    def eval(e:Int*):Array[Int]={
        Array(e:_*)
    }
    def eval(e:Long*):Array[Long]={
      Array(e:_*)
    }
    def eval(e:Float*):Array[Float]={
      Array(e:_*)
    }
    def eval(e:Double*):Array[Double]={
      Array(e:_*)
    }
    def eval(e:Byte*):Array[Byte]={
      Array(e:_*)
    }
    def eval(e:Boolean*):Array[Boolean]={
      Array(e:_*)
    }
    def eval(e:String*):Array[String]={
      Array(e:_*)
    }
    def eval(e:Any*):Array[Any]={
      Array(e:_*)
    }
  }

  class CsvIntArrayConvert extends ScalarFunction{
      def eval(str:String,separator:String):Array[Int]={
          str.split(separator).map(_.toInt)
      }
      def eval(array:Array[Int],separator:String):String={
         array.mkString(separator)
      }
  }

  class CsvLongArrayConvert extends ScalarFunction{
    def eval(str:String,separator:String):Array[Long]={
      str.split(separator).map(_.toLong)
    }
    def eval(array:Array[Long],separator:String):String={
      array.mkString(separator)
    }
  }

  class CsvFloatArrayConvert extends ScalarFunction{
    def eval(str:String,separator:String):Array[Float]={
      str.split(separator).map(_.toFloat)
    }
    def eval(array:Array[Float],separator:String):String={
      array.mkString(separator)
    }
  }

  class CsvDoubleArrayConvert extends ScalarFunction{
    def eval(str:String,separator:String):Array[Double]={
      str.split(separator).map(_.toDouble)
    }
    def eval(array:Array[Double],separator:String):String={
      array.mkString(separator)
    }
  }

  class CsvStringArrayConvert extends ScalarFunction{
    def eval(str:String,separator:String):Array[String]={
      str.split(separator)
    }
    def eval(array:Array[String],separator:String):String={
      array.mkString(separator)
    }
  }

  class CsvBooleanArrayConvert extends ScalarFunction{
    def eval(str:String,separator:String):Array[Boolean]={
      str.split(separator).map(_.toBoolean)
    }
    def eval(array:Array[Boolean],separator:String):String={
      array.mkString(separator)
    }
  }

  /**
    * 判断数组是否为空
    */
  @UdfInfo(name="arrayEmpty",desc = "determine if the array is empty")
  class ArrayEmpty extends ScalarFunction{
    /*
    * 数值类型
    */
    def eval(array: Array[Int]):Boolean = {
      array.isEmpty
    }
    def eval(array: Array[Long]):Boolean = {
      array.isEmpty
    }
    def eval(array: Array[Double]):Boolean = {
      array.isEmpty
    }
    def eval(array: Array[Float]):Boolean = {
      array.isEmpty
    }
    def eval(array: Array[Byte]):Boolean = {
      array.isEmpty
    }
    def eval(array: Array[Boolean]):Boolean = {
      array.isEmpty
    }
    def eval(array: Array[Any]): Boolean = {
      array.isEmpty
    }
  }

  /**
    * 数组求平均值
    */
  @UdfInfo(name="ArrayAvg",desc = "calculate the average of an array")
  class ArrayAvg extends ScalarFunction {
    //todo
    //类型系统在Flink SQL UDF中不适用
    //    def eval[T:Numeric](array: Array[T]):Double = {
    //      implicitly[Numeric[T]].toDouble(array.sum)/array.length
    //    }
    //    def eval[T:Numeric](array: Array[Array[T]]):Double = {
    //      val sum = array.foldLeft(implicitly[Numeric[T]].zero)((x,y)⇒implicitly[Numeric[T]].plus(x,y.sum))
    //      implicitly[Numeric[T]].toDouble(sum)/array.foldLeft(0)((x,y)⇒x+y.length)
    //    }
    def eval(array: Array[Int]): Double = {
      array.sum.toDouble/array.length
    }
    def eval(array: Array[Long]): Double = {
      array.sum.toDouble/array.length
    }
    def eval(array: Array[Double]): Double = {
      array.sum/array.length
    }
    def eval(array: Array[Float]): Double = {
      array.sum.doubleValue()/array.length
    }
    def eval(array: Array[Array[Int]]): Double = {
      array.flatten.sum.toDouble/array.length
    }
    def eval(array: Array[Array[Long]]): Double = {
      array.flatten.sum.toDouble/array.length
    }
    def eval(array: Array[Array[Double]]): Double = {
      array.flatten.sum/array.length
    }
    def eval(array: Array[Array[Float]]): Double = {
      array.flatten.sum.doubleValue()/array.length
    }
  }

  /**
    * 数组求最小值
    */
  @UdfInfo(name="arrayMin",desc = "calculate the min of an array")
  class ArrayMin extends ScalarFunction {
    def eval(array: Array[Int]): Int = {
      array.min
    }
    def eval(array: Array[Long]): Long = {
      array.min
    }
    def eval(array: Array[Double]): Double = {
      array.min
    }
    def eval(array: Array[Float]): Float = {
      array.min
    }
    def eval(array: Array[Array[Int]]): Int = {
      array.flatten.min
    }
    def eval(array: Array[Array[Long]]): Long = {
      array.flatten.min
    }
    def eval(array: Array[Array[Double]]): Double = {
      array.flatten.min
    }
    def eval(array: Array[Array[Float]]): Float = {
      array.flatten.min
    }
  }

  /**
    * 数组求最大值
    */
  @UdfInfo(name="arrayMax",desc = "calculate the max of an array")
  class ArrayMax extends ScalarFunction {
    def eval(array: Array[Int]): Int = {
      array.max
    }
    def eval(array: Array[Long]): Long = {
      array.max
    }
    def eval(array: Array[Double]): Double = {
      array.max
    }
    def eval(array: Array[Float]): Float = {
      array.max
    }
    def eval(array: Array[Array[Int]]): Int = {
      array.flatten.max
    }
    def eval(array: Array[Array[Long]]): Long = {
      array.flatten.max
    }
    def eval(array: Array[Array[Double]]): Double = {
      array.flatten.max
    }
    def eval(array: Array[Array[Float]]): Float = {
      array.flatten.max
    }
  }
}
