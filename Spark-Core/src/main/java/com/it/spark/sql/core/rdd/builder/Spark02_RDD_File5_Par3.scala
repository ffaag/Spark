package com.it.spark.sql.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-22 17:40
 * @description
 */
object Spark02_RDD_File5_Par3 {

    def main(args: Array[String]): Unit = {

        // 准备环境
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("File")
        val sc = new SparkContext(conf)


        // 创建RDD————从文件中创建RDD，将文件中的数据作为处理的数据源
        /**
         * 14 / 2 = 7 byte
         * 14 / 7 = 2 个数据分区
         * 1234567@@    [0, 7]  0 1 2  3  4  5  6 7    读取1234567@@
         * 89@@         [7, 14] 7 8 9 10 11 12 13 14   读取890
         * 0
         * 如果数据源为多个文件，计算分区时以单个文件为单位
         */
        val rdd: RDD[String] = sc.textFile("datas/word.txt", 2)
        rdd.saveAsTextFile("output")

        // 关闭环境
        sc.stop()


    }

}
