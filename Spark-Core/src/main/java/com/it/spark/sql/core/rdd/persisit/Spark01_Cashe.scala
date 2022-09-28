package com.it.spark.sql.core.rdd.persisit

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * @author ZuYingFang
 * @time 2022-03-27 12:07
 * @description
 */
object Spark01_Cache {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        val list = List("Hello Scala", "Hello Spark")

        val rdd1: RDD[String] = sc.makeRDD(list)
        val rdd2: RDD[(String, Int)] = rdd1.flatMap(_.split(" ")).map((_, 1))
        val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
        rdd3.collect().foreach(println)

        println("----------------------------------------")

        val rdd11: RDD[String] = sc.makeRDD(list)
        val rdd21: RDD[(String, Int)] = rdd11.flatMap(_.split(" ")).map((_, 1))
        val rdd31: RDD[(String, Iterable[Int])] = rdd21.groupByKey()
        rdd31.collect().foreach(println)

        println("----------------------------------------")

        // 看着像是重用的rdd21，但是注意rdd中是不存储数据的，如果重复使用，只能是重头 执行逻辑操作来获取数据，也就是对象可重用，数据无法重用
        val rdd41: RDD[(String, Iterable[Int])] = rdd21.groupByKey()
        rdd41.collect().foreach(println)

        println("----------------------------------------")

        /**
         * 办法：持久化操作，即RDD通过Cache或者Persist方法将前面的计算结果缓存，默认情况下会把数据以缓存在JVM的堆内存中。
         * 但是并不是这两个方法被调用时立即缓存，而是触发后面的action算子时（不执行那没数据哇），该RDD将会被缓存在计算节点的内存中，并供后面重用。
         */
        rdd21.cache()   // 放在内存中，可能不安全
        rdd2.persist(StorageLevel.DISK_ONLY)   // 不写参数默认放在内存中 persist(StorageLevel.MEMORY_ONLY)
        val rdd51: RDD[(String, Iterable[Int])] = rdd21.groupByKey()
        rdd51.collect().foreach(println)


        sc.stop()
    }

}
