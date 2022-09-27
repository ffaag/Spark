package com.it.spark.sql.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-22 17:40
 * @description
 */
object Spark01_RDD_Memory_Par2 {

    def main(args: Array[String]): Unit = {

        // 准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)


        // 创建RDD————从内存中创建RDD，将内存中集合的数据作为处理的数据源
        // 指定分区数，底层计算
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 3)

        // 等同于rdd.collect……，这里是将处理完成的数据保存成分区文件
        rdd.saveAsTextFile("output")

        // 关闭环境
        sc.stop()


    }

}
