package com.it.spark.sql.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-22 17:40
 * @description
 */
object Spark02_RDD_File3_Par1 {

    def main(args: Array[String]): Unit = {

        // 准备环境
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("File")
        val sc = new SparkContext(conf)


        // 创建RDD————从文件中创建RDD，将文件中的数据作为处理的数据源
        /**
         * 设定分区数
         * 可以设定最小分区数，不设定使用默认的最小分区数minPartitions
         * def defaultMinPartitions: Int = math.min(defaultParallelism, 2)，这里defaultParallelism就是运行环境的核数
         * 你设定的只是最小的，不一定就是你设定的那个，底层使用hadoop的读取方式，如7个字节，指定2个分区
         * totalSize = 7
         * goalSize = 7/2=3(byte)
         * 7/3=2..1 根据hadoop 1.1倍原则，需要一个新的分区，因此总共三个分区
         */
        val rdd: RDD[String] = sc.textFile("datas/1.txt", 2)
        rdd.saveAsTextFile("output")

        // 关闭环境
        sc.stop()


    }

}
