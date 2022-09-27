package com.it.spark.sql.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-22 17:40
 * @description
 */
object Spark02_RDD_File4_Par2 {

    def main(args: Array[String]): Unit = {

        // 准备环境
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("File")
        val sc = new SparkContext(conf)


        // 创建RDD————从文件中创建RDD，将文件中的数据作为处理的数据源
        /**
         * 设定好分区后，数据分区的数据如何分配
         * 1 数据以行为单位进行读取 spark采用hadoop的方式读取，一行一行读，和字节数无关
         * 2 数据读取时以偏移量为单位
         * 3 数据分区的偏移量范围的计算，以上节7个字节为例
         * 1@@  => 012
         * 2@@  => 345
         * 3    => 6
         * 0 => [0, 3]   读 0 1 2 3 得到 1@@2，因为按行读，所以 2 后面的@@也读到，因此第一个数据文件中读到的是 1@@2@@
         * 1 => [3, 6]   读 3 4 5 6 因为 3 4 5 已经被读过，不会再读，只读 6 ，得到 3 ，因此第二个数据文件读到的是 3
         * 2 => [6, 7]   读 6 7 ，6读过，因此第三个数据文件为空
         * 【】   【】   【】
         */
        val rdd: RDD[String] = sc.textFile("datas/1.txt", 2)
        rdd.saveAsTextFile("output")


        // 关闭环境
        sc.stop()


    }

}
