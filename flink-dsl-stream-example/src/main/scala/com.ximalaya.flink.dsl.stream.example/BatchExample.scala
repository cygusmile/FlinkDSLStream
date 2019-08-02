package com.ximalaya.flink.dsl.stream.example

import com.google.common.collect.{Lists, Maps}
import com.ximalaya.flink.dsl.stream.classloader.DslStreamClassLoader
import com.ximalaya.flink.dsl.stream.client.DslStreamLocalEnvironment
import com.ximalaya.flink.dsl.stream.udf.ArrayFunctions.CsvIntArrayConvert
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.types.Row
/**
  *
  * @author martin.dong
  * @mail martin.dong@ximalaya.com
  * @date 2019/6/14
  *
  **/

object BatchExample {

  val dslEnv:DslStreamLocalEnvironment = new DslStreamLocalEnvironment(new Configuration(),Lists.newArrayList())
  val env:ExecutionEnvironment = {
    val executionEnvironment = new ExecutionEnvironment(dslEnv)
    executionEnvironment.setParallelism(1)
    executionEnvironment
  }

  def main(args: Array[String]): Unit = {


 //   val config = new Configuration()
    // set up execution environment

    val configuration = new org.apache.hadoop.conf.Configuration
    configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
    configuration.set("fs.default.name", "192.168.60.38:8020")


    val cls = new DslStreamClassLoader(configuration, Array("/recsys/fxql/dynamicJars/jinzhongyong"), ClassLoader.getSystemClassLoader)

  //  Thread.currentThread().setContextClassLoader(cls)

  //  val env = ExecutionEnvironment.createLocalEnvironment(1)

//    val dslEnv = new DslStreamLocalEnvironment(new Configuration(), Lists.newArrayList())
//    val env = new ExecutionEnvironment(dslEnv)
    env.setParallelism(1)

    val xx = cls.loadClass("com.ximalaya.fxql.test.udf.AddPreString").newInstance().asInstanceOf[ScalarFunction]
    //val xx = new ArrayHead()

  //  Thread.currentThread().setContextClassLoader(cls)
    val tEnv = new BatchTableEnvironment(env,new TableConfig())


 //   env.getConfig.setGlobalJobParameters(config)

    tEnv.registerFunction("avgx", new CsvIntArrayConvert)

    val orderA: DataSet[Order] = env.fromCollection(Seq(
      Order(1L, "beer", Array(1, 23)),
      Order(1L, "diaper", Array(12, 3)),
      Order(3L, "rubber", Array(11, 30, 221))))


    tEnv.registerDataSet("tableA", orderA, 'user, 'product, 'aa)

//    val udfs: java.util.Map[String, ScalarFunction] = Maps.newHashMap()
//    udfs.put("testUDF", xx)


    tEnv.registerFunction("testUDF", xx)

    dslEnv.setExtendJars(Lists.newArrayList(cls.getURLs:_*))
    val result = tEnv.sqlQuery(
      s"SELECT testUDF(product,'xx','yy') as uuu from tableA")

    val schema=result.getSchema.getColumnNames.zipWithIndex
    val schemaNames=result.getSchema.getColumnNames.toList


    println(result.first(2000).collect().toList)

   // env.execute()
  }
}
