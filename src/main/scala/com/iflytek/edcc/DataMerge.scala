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

object DataMerge {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    //本地模式提交运行
    //    conf.setMaster("local")
    conf.setAppName(this.getClass.getName)

    //SparkContext是Spark的入口，负责连接Spark集群，创建RDD，累积量和广播量等。从本质上来说，SparkContext是Spark的对外接口，负责向调用这提供Spark的各种功能。它的作用是一个容器。
    val sc = new SparkContext(conf)

    //SQLContext是Spark SQL进行结构化数据处理的入口，可以通过它进行DataFrame的创建及SQL的执行
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //spark sql执行引擎，集成hive数据，读取在classpath的 hive-site.xml配置文件配置Hive
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    //日期分区参数
    if(args.length==1){
      val pdate = args(0)
      //模拟数据
      //      val pdate = "2018-04-30"

      val schoolList = "'2244000014000000019','4444000020000002112','4444000020000001425'"
      val schoolId = "2244000014000000019"
      val schoolName = "合肥市第八中学"

      //获取到海边合并数据所需的配置表数据
      val dataMergeConfig = hiveContext.sql("select hive_table_name,data_merge_type,data_merge_dimension,data_merge_measure,data_merge_partition,del_flag from edu_edcc.data_merge_config")
      //          val dataMergeConfig = sqlContext.createDataFrame(Seq(
      //                                        DataMergeConfig("edu_edcc.temp_merge_sh_zx_ques_natural_gp","sum","province_id@city_id@district_id@school_id@school_name@app_id@app_name","ques_total_num","pdate"),
      //                                        DataMergeConfig("edu_edcc.temp_merge_sh_zx_ques_natural_gp","sum","province_id@city_id@district_id@school_id@school_name@app_id@app_name","ques_use_num","pdate")
      //                                       ))
      //配置信息只读取一次后进行缓存
      //      dataMergeConfig.cache()

      //获取合并规则配置信息
      val dataMergeConfigData = dataMergeConfig
        .map(item => {

          //模拟数据
          //          val hiveTableName = item.getAs[String]("hiveTableName")
          //          val dataMergeType = item.getAs[String]("dataMergeType")
          //          val dataMergeDimension = item.getAs[String]("dataMergeDimension")
          //          val dataMergeMeasure = item.getAs[String]("dataMergeMeasure")
          //          val dataMergePartition = item.getAs[String]("dataMergePartition")

          val hiveTableName = item.getAs[String]("hive_table_name")
          val dataMergeType = item.getAs[String]("data_merge_type")
          val dataMergeDimension = item.getAs[String]("data_merge_dimension")
          val dataMergeMeasure = item.getAs[String]("data_merge_measure")
          val dataMergePartition = item.getAs[String]("data_merge_partition")

          val array = dataMergeMeasure.split("@")
          val tempArray = ArrayBuffer[String]()
          for(x<-array){
            tempArray+=(dataMergeType+"("+x+")")
          }
          (hiveTableName,List((dataMergeDimension,dataMergeMeasure,tempArray.mkString(","),dataMergePartition)))
        })
        .reduceByKey((x,y)=>{
          x.++(y)
        }).collect()

      //模拟数据
      //    val rdd1 = sc.makeRDD(List((1,"1001",20,10,0.25,"2018-06-11"),(2,"1001",10,10,0.40,"2018-06-11"),(3,"1001",30,10,0.50,"2018-06-11"),(4,"1001",20,10,0.15,"2018-06-11"))).cache()
      //    val rdd2 = sc.makeRDD(List((1,"1001",0.25,"2018-06-11"),(2,"1001",0.45,"2018-06-11"))).cache()
      //    val dataframe1 = rdd1.map(x=>{
      //      (x._1,x._2,x._3,x._4,x._5,x._6)
      //    }).toDF("school_id","app_id","user_total_num","user_valid_num","user_active_rate","pdate")
      //    dataframe1.registerTempTable("test1")
      //    dataframe1.show()
      //    val dataframe2 = rdd2.map(x=>{
      //      (x._1,x._2,x._3,x._4)
      //    }).toDF("school_id","app_id","user_active_rate","pdate")
      //    dataframe2.registerTempTable("test2")
      //    dataframe2.show()

      //获取hive表下对应学校的数据
      dataMergeConfigData.map(x=>{
        val tableName = x._1
        val data = x._2
        println("hive表名：",tableName)
        var dimensionSelect = ""
        var dimensionGroupBy = ""
        var measure = ""
        var measureSpecial = ""
        var measureArray = ArrayBuffer[String]()
        var measureSpecialArray = ArrayBuffer[String]()
        var partition = ""

        for(item<-data){
          //合肥八中学校id合并,既是把其他2所学校id对应记录修改掉
          dimensionGroupBy = item._1.replaceAll("@",",")
          dimensionSelect = item._1.replaceAll("@",",")
            .replaceAll("school_id",s"'${schoolId}' as school_id")
            .replaceAll("school_name",s"'${schoolName}' as school_name")
          measureArray += item._2.replaceAll("@",",")
          measureSpecialArray += item._3
          partition = item._4
        }
        measure = measureArray.mkString(",")
        measureSpecial = measureSpecialArray.mkString(",")

        //单独处理合肥八中的数据
        //        val sql1 = s"select ${dimensionSelect},${measureSpecial} from ${tableName} where school_id in(${schoolList}) and ${partition} = '${pdate}' group by ${dimensionGroupBy}"
        //        println("执行sql : "+sql1)
        //        val dataframe1 = sqlContext.sql(sql1)
        //        dataframe1.show()
        //
        //        val sql2 = s"select ${dimensionSelect},${measure} from ${tableName} where school_id not in(${schoolList}) and ${partition} = '${pdate}'"
        //        println("执行sql : "+sql2)
        //        val dataframe2 = sqlContext.sql(sql2)
        //        dataframe2.show()

        //执行配置设置
        val preSql = "set hive.exec.dynamic.partition.mode=nonstrict"
        println("执行sql : "+preSql)
        hiveContext.sql(preSql)

        //执行语句
        val sql =
        s"insert overwrite table ${tableName} partition(${partition})" +
          s" select" +
          s" ${dimensionGroupBy},${measureSpecial},${partition}" +
          s" from " +
          s" (" +
          s" select" +
          s" ${dimensionSelect},${measure},${partition}" +
          s" from ${tableName}" +
          s" where school_id in(${schoolList})" +
          s" and ${partition} = '${pdate}'" +
          s" ) as temp" +
          s" group by ${dimensionGroupBy},${partition}" +
          s" union all" +
          s" select" +
          s" ${dimensionGroupBy},${measure},${partition}" +
          s" from ${tableName}" +
          s" where school_id not in(${schoolList})" +
          s" and ${partition} = '${pdate}'"
        println("执行sql : "+sql)
        hiveContext.sql(sql)

      })
    } else {
      println("请传入参数")
    }
    sc.stop()

  }

}

/**
  *  合并数据配置类
  */
case class DataMergeConfig(hiveTableName : String,
                           dataMergeType : String,
                           dataMergeDimension : String,
                           dataMergeMeasure : String,
                           dataMergePartition : String){

}