package com.it.spark.sql.data

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
 * @author ZuYingFang
 * @time 2022-03-29 12:17
 * @description
 */
object Spark02_Hive_LoadAndSave {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        // 使用SparkSQL连接外置的Hive
        // 1 拷贝hive-site.xml文件到resource文件夹中
        // 2 启用Hive的支持 enableHiveSupport()
        // 3 增加对应的依赖关系（包含mysql驱动）
        spark.sql("show tables").show

        // 这样就连接到了Hive数据库了，只需要写spark.sql("")，在里面写sql语句就行

        spark.close()

    }

}
