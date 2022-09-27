package com.it.spark.sql.core.rdd.persisit

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * @author ZuYingFang
 * @time 2022-03-27 12:07
 * @description
 */
object Spark02_CheckPoint {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)
        sc.setCheckpointDir("checkpoint")

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


        // checkpoint需要落盘，需要指定检查点保存的路径
        rdd21.checkpoint()
        val rdd51: RDD[(String, Iterable[Int])] = rdd21.groupByKey()
        rdd51.collect().foreach(println)

        /**
         * cache：将数据临时存储在内存中进行重用，在内存中不太安全，会在血缘关系中添加新的依赖，一旦出现问题可重头读取数据
         * persist：将数据临时存储在磁盘文件中进行重用，涉及到磁盘IO，性能较低，数据安全
         *          作业执行完毕临时数据就会丢失，下一个作业用不了了
         * checkPoint：将数据长久保存在磁盘文件中进行重用，涉及到磁盘IO，性能较低，数据安全
         *             为了保证数据安全，一般情况下会独立产生一个作业执行
         *             为了提高效率，一般情况下会和cache联合使用，这样checkpoint的job只需从Cache缓存中读取数据即可，否则需要再从头计算一次RDD。
         *             执行过程中会切断血缘关系，重新建立新的血缘关系，就等同于改变数据源
         */
        rdd2.cache()
        rdd2.checkpoint()


        sc.stop()
    }

}
