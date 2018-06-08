package com.iflytek.edcc

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2017/10/19.
  */

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2017/10/19
  * Time: 13:52
  * Description
  */

object SparkHiveSqlTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName)
    //设置数据自动覆盖
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    //SparkContext是Spark的入口，负责连接Spark集群，创建RDD，累积量和广播量等。从本质上来说，SparkContext是Spark的对外接口，负责向调用这提供Spark的各种功能。它的作用是一个容器。
    val sc = new SparkContext(conf)

    //SQLContext是Spark SQL进行结构化数据处理的入口，可以通过它进行DataFrame的创建及SQL的执行
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    //spark sql执行引擎，集成hive数据，读取在classpath的 hive-site.xml配置文件配置Hive
    val hiveContext = new HiveContext(sc)
    import hiveContext._

    //1,使用use确认库名，再查询表
    hiveContext.sql("use zx_base")
    hiveContext.sql("select count(1) from dim_school")

    //2，使用库名.表名
    hiveContext.sql("select count(1) from zx_dev.util_date_relation")
    sc.stop()

  }

}
