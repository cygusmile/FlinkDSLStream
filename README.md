# FlinkDSLStream

[语法](https://github.com/dongjiaqiang/FlinkDSLStream#%E8%AF%AD%E6%B3%95)
* [创建表](https://github.com/dongjiaqiang/FlinkDSLStream#%E5%88%9B%E5%BB%BA%E8%A1%A8)
    - [流表](https://github.com/dongjiaqiang/FlinkDSLStream#%E6%B5%81%E8%A1%A8)
    - [维表](https://github.com/dongjiaqiang/FlinkDSLStream#%E7%BB%B4%E8%A1%A8)
    - [批表](https://github.com/dongjiaqiang/FlinkDSLStream#%E6%89%B9%E8%A1%A8)
* [保存表](https://github.com/dongjiaqiang/FlinkDSLStream#保存表)
* [创建缓存](https://github.com/dongjiaqiang/FlinkDSLStream#创建缓存)
* [查询解析](https://github.com/dongjiaqiang/FlinkDSLStream#查询解析)
* [动态依赖](https://github.com/dongjiaqiang/FlinkDSLStream#动态依赖)
* [注释](https://github.com/dongjiaqiang/FlinkDSLStream#注释)
* [数据类型和解析](https://github.com/dongjiaqiang/FlinkDSLStream#数据类型和解析)
* [UDF](https://github.com/dongjiaqiang/FlinkDSLStream#UDF)
* [扩展语法](https://github.com/dongjiaqiang/FlinkDSLStream#扩展语法)

## 语法

#### 创建表

##### 流表

###### 含义

**在实时计算中流表代表一个流式计算存储 其可以驱动一个流式计算的执行 在一个流式计算作业中必须指定一个流表**

###### 语法

```sql
create stream tableName (
     [pathName]*.fieldName (as aliasName)* type (default)*,
     ......
     (key(aliasName))*
     (waterMark = strategy)*
) required( source = sourceType,
                   dataType = dataType, 
                   keyDecoder = keyDecoder,
                   parallel = parallelNum,
                   castError = ignore | stop ) 
  optional( key = value,[ key = value ]*) as tableName
```

###### 说明

**定义字段时可选的指定一个或多个路径 可选定义字段别名 定义字段类型和可选定义字段默认值**

* key

    读取源表数据源数据的键 需要设置一个别名

* required parameters (定义源表必填参数)

    | 名字 | 含义 |
    | ---- | ----- |
    | source | 源表数据类型 |
    | dataType | 源表数据源数据类型 表示源表数据源存储的数据格式 |
    | keyDecoder | 如果源表数据源数据类型是键值形式且需要获取到则需要指定键的解码方式 否则填none |
    | parallel |  源表并行度 |
    | castError | 源表数据转换错误处理策略 ignore或stop |

* optional parameters （特定于源表数据源的参数)

* WaterMark

    内置Strategy

    | 名字 | 含义 |
    | ---- | ------- |
    | maxOuterOfOder(columnName,columnName,maxDelay) | 允许固定时间延迟的水位生成策略 |
    | ascending(columnName,noMonotony) | 递增的水位生成策略 如果违法水位递增规则选择的策略(fail or ignore) |
    | ingestion | 基于摄入时间的水位生成策略 |

    自定义Strategy

###### Kafka流表 (source = kafka)

* optional parameters

    | 名字 | 含义 |
    | ---- | ----- |
    |topic	| Kafka Topic名|
    |zookeeper|	Kafka Zookeeper连接信息|
    |broker	|Kafka Broker地址信息 |
    |offset	|Kafka Offset设置 latest earliest and alwayLatest|
    |groupId|	Kafka组名|

###### HDFS流表 (source = hdfs)

* optional parameters

    | 名字 | 含义 |
    | ---- | ----- |
    |path	| 位于HDFS上的文件路径 |
    |fsDefaultName|	HDFS DefaultName|
    |zip	| HDFS文件数据压缩类型 none or zip|

###### Socket流表 (source = socket)

* optional parameters

    | 名字 | 含义 |
    | ---- | ------- |
    |ip	 |socket ip地址|
    |port	|socket 端口号|
    |maxRetry	|socket 最大重试次数|

###### RabbitMQ流表 (source = rabbitmq)

* optional parameters

    | 名字 | 含义 |
    | ----- | ----- |
    |host	|rabbitMQ 主机地址|
    |port	|rabbitMQ 端口号|
    |queue	|rabbitMQ 队列名|
    |useCorrelationIds	|true or false|
    
##### 维表

###### 含义

**维表是一张不断变化的表 源表可以left join或join 一张或多张维表**

###### 语法

```sql
create side tableName (
     [pathName]*.fieldName (as aliasName)* type (default)*,
     ......
     key(aliasName)
     cache = strategy
) required( source = sourceType, 
                   keyDecoder  = keyDecoder,
                   castError = ignore | stop ) 
  optional( key = value,[ key = value ]*) as tableName
```

###### 说明

* key

    读取维表数据需要设置一个主键字段 并设置一个别名

* required parameters (定义源表必填参数)

   | 名字 | 含义 |
    | ---- | ----- |
    | source | 维表数据类型 |
    | keyDecoder | 维表主键字段解码方式 |
    | castError | 维表数据转换错误处理策略 ignore或stop |
    
* optional parameters （特定于源表数据源的参数)

* cache

    对于维表需要定义缓存策略
    
    | 名字 | 含义 |
    | ---- | ----- |
    | none	| 不缓存 |
    |all(keyPattern,ttl,timeUnit)	|全缓存 可以选择需要缓存的数据范围 如*表示全缓存 ttl表示缓存更新时间|
    |lru(keyPattern,cacheSize,ttl,timeUnit)	|LRU缓存大小 ttl表示缓存更新时间|
    

###### HBASE维表 （source = hbase )

* optional parameters

    | 名字	| 含义 |
    | ----- | ----- |
    |zookeeper	| HBase zookeeper连接信息 |
    |tableName|	HBase表名称|
    | decoder |	HBase数据解码方式 String or selfDefinition|
    
###### MySQL维表 (source = mysql)

* optional parameters

    | 名字	| 含义 |
    | ----- | ---- |
    |jdbcUrl|MySQL jdbc url地址|
    |dbName	|MySQL db name|
    |tableName|	MySQL table name|
    |username|	MySQL user name|
    |password|	MySQL password|
    
###### HDFS维表 (source = hdfs )

* optional parameters

    | 名字 | 含义 |
    | ---- | ---- |
    |path	|HDFS Path|
    |fsDefaultName	|HDFS DefaultName|
    | dataType	|HDFS dataType |
    |zip	|HDFS zip type none or zip|
    
###### Redis维表 ( source = redis )

* optional parameters

    |名字	|含义 |
    | ----- | ----- |
    | url	|主机名和端口号|
    |password|	密码|
    |username|	用户名|
    |database|	数据库|
    |dataType|	hash or string|
    |decoder |	string or selfDefinition|
    
##### 批表

###### 含义

**在离线计算中代表一个离线计算所需要的一个输入 其可以驱动离线作业的执行 在一个离线计算作业中必须指定一个离线表**

###### 语法

```sql
create batch tableName (
       [pathName]*.fieldName (as aliasName)* type (default)*,
       ....
       (key(aliasName))*
) required( source = sourceType,
                   keyDecoder = keyDecoder,
                   parallel = parallelNum)
  optional( key = value,[ key = value ]*) as tableName
```

###### 说明

**定义字段时可选的指定一个或多个路径 可选定义字段别名 可选定义字段类型和可选定义字段默认值
required parameters (定义源表必填参数)**

* key

    读取源表数据源数据的键 需要设置一个别名

* required parameters (定义源表必填参数)

    | 名字 | 含义 |
    | ---- | ----- |
    | source | 源表数据类型 |
    | keyDecoder | 如果源表数据源数据类型是键值形式且需要获取到则需要指定键的解码方式 否则填none |
    | parallel |  源表并行度 |
    
* optional parameters （特定于源表数据源的参数)

###### HBASE维表 （source = hbase )

* optional parameters

    | 名字	| 含义 |
    | ----- | ----- |
    |zookeeper	| HBase zookeeper连接信息 |
    |tableName|	HBase表名称|
    | decoder |	HBase数据解码方式 String or selfDefinition|
    |keyPattern	| HBase键范围 |
    
###### HDFS批表 (source = hdfs )

* optional parameters

    |名字|	含义|
    | --- | ---- |
    |path|	HDFS Path|
    |fsDefaultName|	HDFS DefaultName|
    | dataType | HDFS data type|
    |zip	|HDFS zip type none or zip|
    
###### MySQL批表(source = mysql)

* optional parameters

    |名字|	含义|
    | --- | ---- |
    |jdbcUrl	|MySQL jdbc url|
    |dbName|	MySQL db name|
    | tableName	|MySQL table name|
    | username|	MySQL user name|
    | keyPattern|	MySQL键范围|
    |password|	MySQL password|
    
#### 保存表

###### 含义

**流计算结果表有Append类型和Update类型**

* Append类型

**如果输出存储是日志系统、消息系统、数据流的输出结果会以追加的方式写入到存储中，不会修改存储中原有的数据**

* Update类型

**如果输出存储是声明了主键（primary key）的数据库，数据流的输出结果有以下2种情况。如果根据主键查询的数据在数据库中不存在，则会将该数据插入数据库。如果根据主键查询的数据在数据库中存在，则会根据主键更新数据**

###### 语法

```sql
save tableName(
     fieldName as [pathName]*.columnName,
    .........
    cache = strategy
    (key(fieldName))*
) required( sink = sinkType,
                   keyEncoder = keyEncoder
                   parallel = parallelNum) 
  optional( key = value,[ key = value ]*)
```

###### 说明

**保存字段时可选的指定一个或多个路径**

* cache 

    对于结果表需要定义缓存写策略
    
    | 名字 | 含义 |
    | ---- | ---  |
    |none	|不缓存策略 一条条写数据|
    |cache(cacheSize)|	缓存写策略 可以累积一批写数据|
    
* key

    对于将结果表数据保存到键值数据库 需要设置一个主键字段
    
* required parameters (定义源表必填参数)

   | 名字 | 含义 |
    | ---- | ----- |
    | sink |  结果表数据类型 |
    | keyEncoder | 结果表主键字段编码方式 |
    | parallel | 结果表存储并行度 |

* optional parameters （特定于源表数据源的参数)

###### Kafka结果表(sink = kafka)

* optional parameters

    |名字	|含义|
    | ----- | ---- |
    |topic	|Kafka Topic|
    |broker	|Kafka Broker|
    |dataType	|写入Kafka的数据格式|
    
###### HBase结果表(sink = hbase)

* optional parameters

    |名字	|含义|
    | ----- | ---- |
    |zookeeper	|HBase zookeeper|
    |tableName	|HBase表名称|
    |encoder |	HBase数据字段编码方式|
    
###### Redis结果表(sink = redis)

* optional parameters

   |名字	|含义|
    | ----- | ---- |
    |url	|主机名和端口号|
    |password	|密码|
    |username	|用户名|
    |database|	数据库|
    |dataType	|hash or string|
    | encoder | 数据字段编码方式 |
    |ttl	|设置过期时间 -1表示不过期|
    
###### Debug结果表(sink = debug)
 
 * optional parameters
 
      |名字	|含义|
      | ----- | ---- |
      | formatter | 数据编码方式 |
 
###### Dummy结果表(sink = dummy)

 * optional parameters
    
     无

#### 创建缓存

###### 含义

**实现在作业中缓存外部数据源数据 可通过缓存函数查询外部缓存值**

###### 语法

```sql
create cache cacheName(
         [pathName]*.fieldName  as key  type ,
         [pathName]*.fieldName  as value  type
) required ( source = sourceType,
                    keyPattern = keyPattern,
                    keyDecoder = keyDecoder,
                    updatePeriod=  updatePeriod,
                    updateTimeUnit = updateTimeUnit,
                    castError = ignore | stop)   
  optional( key = value,[ key = value ]*)  
```

###### 说明

* required parameters (定义缓存必填参数)

    | 名字 | 含义 |
    | ----- | ----- |
    | source |  缓存源 |
    | keyPattern | 符合进行缓存的键匹配模式 如*代表全匹配 |
    | keyDecoder | 缓存键解析器 |
     | updatePeriod | 缓存更新时间 |
     |updateTimeUnit | 缓存更新时间单位 |
     | castError | 数据类型转换出错时 是忽略还是停止 |
    
* optional parameters （特定于源表数据源的参数)

###### HDFS缓存 (source = hdfs)

* optional parameters

   |名字	|含义|
    | ----- | ---- |
    |path	|HDFS Path|
    |fsDefaultName|	HDFS DefaultName|
    |dataType	|HDFS data type|
    | zip | HDFS zip type none or zip |
    
###### Redis缓存(source = redis )

* optional parameters

    |名字|	含义|
    | ---- | ----- |
    |url|	主机名和端口号|
    |password|	密码|
    |username|	用户名|
    |database|	数据库|
    |dataType|	hash or string|
    |decoder|	值解析器|
    
###### HBase缓存(source = hbase)

* optional parameters

    |名字|	含义|
    | ---- | ---- |
    |zookeeper|	HBase zookeeper|
    |tableName|	HBase表名称|
    |decoder|值解析器	|
    
###### MySQL缓存(source = mysql)

* optional parameters

    |名字|	含义|
    | --- | ---- |
    |dbName	|MySQL db name|
    |tableName|	MySQL table name|
    |username|	MySQL user name|
    |password|	MySQL password|
    

#### 查询解析

###### 含义

**用于筛选和转换数据**

###### 语法

```sql
select name,length(article) as len,md5(article) as md5 from article as table
```

#### 动态依赖

###### 含义

**可以动态依赖外部引入的UDF和数据解析器**

###### 语法

```sql
--引入外部一个依赖jar
include /user/ximalaya/functions.jar

--引入外部一个依赖路径 路径内含有依赖jar
include /user/ximalaya

--动态引入udf
import clazz from 'com.ximalaya.Person as person'
import object from 'com.ximalaya.Info' with(string='li',int='23') as 'info'
import udf from 'com.ximalaya.SegmentWord' with(int='1',bool='true',clazz='person',object='info') as 'SegmentWord'

--动态引入decoder
import decoder from 'com.ximalaya.SimpleDecoder'   as 'SimpleDecoder'
```

###### xlink-yarn_2.11

**为实现动态依赖 必须能够实现将include语法所引入的依赖传递给Flink作业 由于原生Flink实现只支持从本地将所有依赖传递到Flink Yarn上的hdfs目录中 所以必须替换Flink Yarn依赖**

[Xlink](https://github.com/dongjiaqiang/Xlink)

#### 注释

###### 含义

**可以在sql作业中添加注释**

###### 语法

```sql
-- comment here
```

#### 数据类型和解析

##### decoder

###### 说明

**支持字段级别的数据解码**

##### encoder

###### 说明

**支持字段级别的数据编码**

#### dataType

###### 说明

**支持事件级别的数据编码和解码**

#### UDF

#### 扩展语法

###### explode语法
 
```sql
/** 支持将数组元素扁平化 
 **例子
/** table
 * | name |   interests  |
 * |------|------------- |
 * |  li  | read,run,talk|
 * 
 ** table1
 * | name | interest |
 * | ---- | ---------|
 * |  li  |  read    |
 * |  li  |  run     |
 * | li   |  talk    |
 */

select explode(interests) [as interest] from table [where = condition] as table1;
```

###### lateral view explode语法

```sql
/**
 * myTable:
 *
 * | myCol1   | myCol2 |
 * | ------  | ------ |
 * |     a | [1,2,3]   ]
 *
 * myNewTable:
 *
 * | myCol1 | explodeMyCol2 |
 * | -------- | ------- |
 * |    a     |   1    |
 * |    a     |   2    |
 * |    a     |    3   |
 *
 * 注: myCol也可以是一个返回Array的函数调用
 */
 
select myCol1,explodeMyCol2 from myTable lateral view explode(myCol2) explodeTable as explodeMyCol2 [where = condition] as myNewTable;
```
 
 ###### 维流表Join语法
 
 ```sql
-- 实现维表和流表的内连接
select side stream a.* from streamTable a join dimensionTable b on a.id = b.id where a.name <> 'hello' as joinTable;

-- 实现维表和流表的左外连接
select side stream a.* from streamTable a left join dimensionTable b on a.id = b.id where a.name <> 'hello' as joinTable;
  
-- 实现多表关联
select side stream a.* from streamTable a join dimensionTableOne b join dimensionTableTwo c  on a.id = b.id and a.id = c.id as joinTable; 
```

###### 状态流语法

```sql
select state stream user,location,state('stateFunction',8,'hour') as state from streamTable group by user,location [ where = condition ] as stateTable;
```