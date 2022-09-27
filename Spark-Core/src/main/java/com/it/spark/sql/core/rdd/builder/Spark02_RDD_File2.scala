package com.it.spark.sql.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-22 17:40
 * @description
 */
object Spark02_RDD_File2 {

    def main(args: Array[String]): Unit = {

        // 准备环境
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("File")
        val sc = new SparkContext(conf)


        // 创建RDD————从文件中创建RDD，将文件中的数据作为处理的数据源
        // textFile方法：以行为单位读取数据(Hello World Hello Spark)
        // wholeTextFiles方法：以文件为单位读取数据(file:/D:/IntelliJ IDEA/BigData/06-Spark/datas/1.txt,Hello World Hello Spark)
        val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas")
        rdd.collect().foreach(println)


        // 关闭环境
        sc.stop()


    }

}
