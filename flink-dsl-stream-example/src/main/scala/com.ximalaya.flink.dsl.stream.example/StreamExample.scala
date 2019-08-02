//package com.ximalaya.flink.dsl.stream.example
//
//import java.io.File
//import java.net.{URL, URLClassLoader}
//
//import com.ximalaya.flink.dsl.stream.udf.ArrayFunctions.{ArrayHead, CsvIntArrayConvert}
//import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters
//import org.apache.flink.api.common.functions.RichMapFunction
//import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
//import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
//import org.apache.flink.table.api.TableEnvironment
//import org.apache.flink.table.api.scala._
//import org.apache.flink.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.sink.SinkFunction
//import org.apache.flink.table.functions.ScalarFunction
//import java.util.Map
//
//import com.alibaba.fastjson.JSONArray
//import com.google.common.collect.{Lists, Maps}
//import com.ximalaya.flink.dsl.stream.calcite.flink.SqlCompiler
//import com.ximalaya.flink.dsl.stream.calcite.flink.context.{CompileContext, QueryRuntimeContext, SideCatalog}
//import com.ximalaya.flink.dsl.stream.classloader.DslStreamClassLoader
//import com.ximalaya.flink.dsl.stream.client.DslStreamLocalStreamEnvironment
//import com.ximalaya.flink.dsl.stream.connector.hbase.{HBaseAsyncSideClient, HBaseSideLoader}
//import com.ximalaya.flink.dsl.stream.connector.redis.RedisAsyncSideClient
//import com.ximalaya.flink.dsl.stream.side.{AsyncSideClient, SideLoader, SideTableInfo}
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.typeutils.RowTypeInfo
//import org.apache.flink.core.fs.Path
//import org.apache.flink.runtime.blob.PermanentBlobKey
//import org.apache.flink.streaming.api.functions.ProcessFunction
//import org.apache.flink.types.Row
//import org.apache.flink.util.Collector
//
//import scala.collection.mutable.ArrayBuffer
///**
//  *
//  * @author martin.dong
//  *
//  **/
//
//class XXX extends ScalarFunction{
//  def eval():Long={
//    System.currentTimeMillis()
//  }
//}
//
//
//object StreamExample {
//
//
//  def main(args: Array[String]): Unit = {
//
//
//    val config = new Configuration()
//    // set up execution environment
//
//    val configuration = new org.apache.hadoop.conf.Configuration
//    configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
//    configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
//    configuration.set("fs.default.name", "192.168.60.38:8020")
//
//
//    val cls = new DslStreamClassLoader(configuration,Array("/recsys/fxql/dynamicJars/jinzhongyong"),ClassLoader.getSystemClassLoader)
//
//    val env = new StreamExecutionEnvironment(new DslStreamLocalStreamEnvironment(new Configuration(),Lists.newArrayList(cls.getURLs:_*)))
//
//    ExecutionEnvironment.createLocalEnvironment()
////   val cls = new URLClassLoader(Array(new File("/Users/nali/Desktop/testUDFS/flink-udf-test-2.0.1.jar").toURL))
//    val xx = cls.loadClass("com.ximalaya.fxql.test.udf.AddPreString").newInstance().asInstanceOf[ScalarFunction]
//    //val xx = new ArrayHead()
//
//    Thread.currentThread().setContextClassLoader(cls)
//    val tEnv = TableEnvironment.getTableEnvironment(env)
//
//
//    env.getConfig.setGlobalJobParameters(config)
//
//    tEnv.registerFunction("avgx",new CsvIntArrayConvert)
//
//    val orderA: DataStream[Order] = env.fromCollection(Seq(
//      Order(1L, "beer", Array(1,23)),
//      Order(1L, "diaper", Array(12,3)),
//      Order(3L, "rubber", Array(11,30,221))))
//    //
////    val orderB: DataStream[Order] = env.fromCollection(Seq(
////      Order(2L, "pen", 3),
////      Order(2L, "rubber", 3),
////      Order(4L, "beer", 1)))
//
//    // convert DataStream to Table
//    tEnv.registerDataStream("tableA",orderA,'user,'product,'aa)
//
//    val udfs:java.util.Map[String,ScalarFunction] = Maps.newHashMap()
//    udfs.put("testUDF",xx)
//
//
//    tEnv.registerFunction("testUDF",xx)
//  //  Thread.currentThread().setContextClassLoader(cls)
//
//    val sideClient = Maps.newHashMap[String,Class[_<:AsyncSideClient]]()
//
//    sideClient.put("hbase",classOf[HBaseAsyncSideClient])
//    sideClient.put("redis",classOf[RedisAsyncSideClient])
//
//    val sideLoader = Maps.newHashMap[String,Class[_<:SideLoader]]()
//
//    sideLoader.put("hbase",classOf[HBaseSideLoader])
//
//    val sideCatalog = SideCatalog(sideClient,sideLoader)
//
//    val sideTables = Maps.newHashMap[String,SideTableInfo]()
//
//
//    val compileContext = new CompileContext(tEnv,sideCatalog,udfs,null)
// //   import com.ximalaya.flink.dsl.stream.calcite.register._
// //   compileContext.register("select user,product,'xx' as mm,a,testUDF(product,'xx','yy') as uuu from tableA lateral view explode(aa) explodeTable as a where user <> 3 as newMyTable")
//
//    // register DataStream as Table
//   // tEnv.registerDataStream("OrderB", orderB, 'user, 'product, 'amount)
//    // union the two tables
//    val result = tEnv.sqlQuery(
//      s"SELECT testUDF(product,'xx','yy') as uuu from tableA")
//
////    result.toAppendStream[Row].process(new ProcessFunction[Row,Row] {
////
////      override def processElement(value: Row, ctx: ProcessFunction[Row, Row]#Context, out: Collector[Row]): Unit = ???
////    })
//
//
//  //  val classLoader = ClassLoader.getSystemClassLoader
//    result.toAppendStream[Row].addSink(new SinkFunction[Row] {
//      override def invoke(value: Row, context: SinkFunction.Context[_]): Unit = {
//        println(value)
//      }
//    })
////    env.getStreamGraph.getJobGraph().setClasspaths(Lists.newArrayList(new File("/Users/nali/Desktop/testUDFS/flink-udf-test-2.0.1.jar").toURL))
//
////    env.getStreamGraph.getJobGraph(env.getStreamGraph.j).addJar(new Path(new File("/Users/nali/Desktop/testUDFS/flink-udf-test-2.0.1.jar").toURI))
//
//  //  env.getStreamGraph.getJobGraph.addUserArtifact()
//
// //   env.getStreamGraph.getJobGraph(env.getStreamGraph.getJobGraph().getJobID).setClasspaths(Lists.newArrayList(new File("/Users/nali/Desktop/testUDFS").toURL))
//    env.execute()
//  }
//}
