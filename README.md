## 介绍

flink同步库，主要功能是表同步，格式基于[debezium](https://debezium.io/)。

![数据流](https://github.com/kongkongye/flink-sync/blob/main/flink-sync.png?raw=true)

## 快速使用

### 前置操作

代码下载下来后，先手动将一些通用的依赖包添加到flink的lib目录下，这样打出的包就不会很大，包括：

1. flink连接器
   1. flink-connector-kafka-1.15.0.jar
   2. flink-connector-jdbc-1.15.0.jar
2. jdbc数据库驱动
   1. kafka-clients-2.8.1.jar
   2. mysql-connector-java-8.0.29.jar
   3. mssql-jdbc-8.2.2.jre8.jar

### 表同步

#### 1. maven打包

会打包出`flink-table-sync.jar`

``` shell
mvn clean package -DskipTests
```

#### 2. 上传运行

在flink web ui界面上传，参数`并行度`填个合适的值，
更高的并行度就同步更快（比如cpu核数），
程序参数填`--file <file>`，file是文件的绝对路径。

文件格式可以参考`example/table_sync`目录下的json文件。

### sql同步

#### 1. 修改pom文件

修改pom文件里`build.finalName`为`flink-sql-sync`，
`mainClass`为`com.kongkongye.flink.sync.sql.SqlSyncJob`。

#### 2. maven打包

会打包出`flink-sql-sync.jar`

``` shell
mvn clean package -DskipTests
```

#### 3. 上传运行

在flink web ui界面上传，参数`并行度`填个合适的值，
更高的并行度就同步更快（比如cpu核数），
程序参数填`--file <file>`，file是文件的绝对路径。

文件格式可以参考`example/sql_sync`目录下的sql文件。

## 文档

此库发布时，flink最新的版本是1.15.0。

表同步跟sql同步相比，表同步不用占内存，数据只是在flink里过一下，而sql同步占内存，数据都存在flink状态里（配置合适的状态后端也不占内存？）。

### 本地与线上运行
如果本地运行，请将pom文件里的`runtime.scope`修改为`compile`，如果线上运行，则改为`provided`。

### 上游kafka要求
此库假设的是kafka上存的都是debezium格式的json数据。

放个kafka连接器配置片段参考一下(不相关配置已省略)：

``` json
{
  "name": "sqlserver-orders",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "decimal.handling.mode": "string",
    "tombstones.on.delete": "false"
    ...
  }
}
```

### 下游数据库支持

目前支持`mysql`与`sqlserver`，其他数据库需要自行适配。

在支持程度上，目前只支持部分字段类型，碰到新的字段类型就改代码添加支持。

#### 关于sqlserver支持
官方jdbc连接器目前不支持sqlserver，因此复制了连接器mysql支持的源码改了改来支持。

目前没发现问题，如果后面官方jdbc连接器支持sqlserver了，可以把这部分删掉。

### 表同步
#### Converter转换器
json里可以配置converters参数来添加多个转换器，处理逻辑为按顺序进行检测，如果一个转换器转换成功，后续就不再检测其他转换器。

星号(`*`)代表匹配任意列名。

### sql同步
这个很简单，用的是flink自带的sql功能。

#### sql文件
语法是flink sql的语法，此库做的就是将写在一个文件里的多个sql用`----`分隔，然后分别执行。

## 问题

### kafka source并发超过1会有问题吗？

开始以为有问题，因为不管表同步还是sql同步，读取同一条主键的修改记录的顺序都很重要，而多线程是乱序取的，因此需要设置kafka source并发度为1。

但后来发现指定了消费者组的情况下，同一个key的信息都会由同一个消费者消费，也就是被同一个线程接收走，因此kafka source并发度可以超过1。

## todos
1. Serializable是否能再优化，让线程切换资源传递少点？