package com.iflytek.edcc

import org.apache.spark.sql.SparkSession

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/06/08.
  * Time: 13:52
  * Description
  */

object TestSpark {

  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("spark-hive").enableHiveSupport().getOrCreate();
      spark.sql("select * from edu_jyyun_model.dim_date").show()
      spark.catalog.listDatabases.show(false)
  }

}