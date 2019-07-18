package com.iflytek.edcc

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/06/08.
  * Time: 13:52
  * Description
  */

object TestSpark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    //本地模式提交运行
//        conf.setMaster("local")
    conf.setAppName(this.getClass.getName)

    //SparkContext是Spark的入口，负责连接Spark集群，创建RDD，累积量和广播量等。从本质上来说，SparkContext是Spark的对外接口，负责向调用这提供Spark的各种功能。它的作用是一个容器。
    val sc = new SparkContext(conf)

    //SQLContext是Spark SQL进行结构化数据处理的入口，可以通过它进行DataFrame的创建及SQL的执行
//    val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._

    //spark sql执行引擎，集成hive数据，读取在classpath的 hive-site.xml配置文件配置Hive
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    hiveContext.sql("select * from edu_jyyun_model_dev.dim_grade")

    sc.stop();

  }

}