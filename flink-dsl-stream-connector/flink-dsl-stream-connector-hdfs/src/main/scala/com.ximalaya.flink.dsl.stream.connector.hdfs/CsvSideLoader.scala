package com.ximalaya.flink.dsl.stream.connector.hdfs

import java.io.{BufferedReader, InputStreamReader}
import java.util

import com.google.common.collect.Maps
import com.ximalaya.flink.dsl.stream.`type`.SourceField
import com.ximalaya.flink.dsl.stream.side._
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.hadoop.fs.{FileSystem, Path, PathNotFoundException}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @author martin.dong
  *
  **/

class CsvSideLoader extends SideLoader {

  private var fileSystem: FileSystem = _
  private var paths: List[Path] = _
  private var zipStrategy: ZipStrategy.Value = _

  /**
    * 加载维表数据方法
    *
    * @param keyPattern 键范围
    * @return 被加载的数据
    */
  override def load(keyPattern: String): util.Map[Array[Byte], util.Map[String, Object]] = {
      var data = Maps.newHashMap()
      paths.foreach(path⇒{
          val input = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
          input
      })
     null
  }

  /**
    * 初始化方法 比如创建数据库连接等
    *
    * @param key               key field name 维表主键字段名
    * @param sideTable         side table name 维表名
    * @param physicsSchema     physics schema 维表物理层schema信息
    * @param castErrorStrategy cast error strategy 维表数据转换错误处理策略 忽略或报错
    * @param physicsSideInfo   physics side info 维表物理层数据源连接信息
    * @param runtimeContext    runtime context Flink运行时对象
    */
  override def open(key: String, sideTable: String, physicsSchema: util.List[SourceField], castErrorStrategy: CastErrorStrategy, physicsSideInfo: PhysicsSideInfo, runtimeContext: RuntimeContext): Unit = {
    val hdfsSideInfo = physicsSideInfo.asInstanceOf[HDFSSideInfo]
    val dataType = hdfsSideInfo.dataType
    require(dataType == FileType.Json, "data type must be json")

    val configuration = new org.apache.hadoop.conf.Configuration
    configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
    configuration.set("fs.default.name", hdfsSideInfo.fsDefaultName)

  //  physicsSchema

    this.fileSystem = FileSystem.get(configuration)
    this.zipStrategy = zipStrategy
    val path = new Path(hdfsSideInfo.path)

    if (fileSystem.exists(path)) {
      if (fileSystem.isFile(path)) {
        this.paths = List(path)
      } else {
        var paths = ArrayBuffer[Path]()
        val iterable = fileSystem.listFiles(path, false)
        while (iterable.hasNext) {
          paths.append(iterable.next().getPath)
        }
        this.paths = paths.toList
      }
    } else {
      throw new PathNotFoundException(hdfsSideInfo.path)
    }

  }

  /**
    * 关闭方法 比如关闭数据库连接等
    */
  override def close(): Unit = {
    fileSystem.close()
  }
}
