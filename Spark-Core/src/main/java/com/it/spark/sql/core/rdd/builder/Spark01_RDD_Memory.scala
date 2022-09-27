package com.it.spark.sql.core.rdd.builder

import org.apache.spark.SparkContext.jarOfObject
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-22 17:40
 * @description
 */
object Spark01_RDD_Memory {

    def main(args: Array[String]): Unit = {

        // 准备环境

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)


        // 创建RDD————从内存中创建RDD，将内存中集合的数据作为处理的数据源
        val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)

        // parallelize——并行，两个方法是一样的，下面那个方法更容易记住，makeRDD底层就调用的parallelize
//        val rdd1: RDD[Int] = sc.parallelize(seq)
//        rdd1.collect().foreach(println)

        val rdd2: RDD[Int] = sc.makeRDD(seq)

        rdd2.collect().foreach(println)


        // 关闭环境
        sc.stop()


    }

}
