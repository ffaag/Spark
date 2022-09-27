package com.it.spark.sql.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-22 17:40
 * @description
 */
object Spark01_RDD_Memory_Par1 {

    def main(args: Array[String]): Unit = {

        // 准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        sparkConf.set("spark.default.parallelism", "5")
        val sc = new SparkContext(sparkConf)


        // 创建RDD————从内存中创建RDD，将内存中集合的数据作为处理的数据源
        // RDD的并行度：先进行分区，得到子task，然后发给executor。分区数和并行度是有区别，可以不同的

        /**
         * 第二个参数为分区的数量，可以不传递，会使用默认值
         * val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)1
         * 底层代码为：scheduler.conf.getInt("spark.default.parallelism", totalCores)
         * 即默认情况下从配置对象中获取配置参数spark.default.parallelism，获取不到则使用totalCores，取值为运行环境的最大可用核数
         * 可在自己定义的配置里面自行设置sparkConf.set("spark.default.parallelism", "5")
         */
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // 等同于rdd.collect……，这里是将处理完成的数据保存成分区文件
        rdd.saveAsTextFile("output")

        // 关闭环境
        sc.stop()


    }

}
