package com.it.spark.sql.core.rdd.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-26 21:00
 * @description OneToOneDependency：新的RDD的一个分区的数据依赖于旧的RDD一个分区的数据 也叫窄依赖
 *              ShuffleDependency：新的RDD的一个分区的数据依赖于旧的RDD多个分区的数据，如groupBy 也叫宽依赖
 *
 *      RDD任务切分中间分为：Application、Job、Stage和Task
                ⚫ Application：初始化一个SparkContext即生成一个Application；
                ⚫ Job：一个Action算子就会生成一个Job；
                ⚫ Stage：Stage等于宽依赖(ShuffleDependency)的个数加1；
                ⚫ Task：一个Stage阶段中，最后一个RDD的分区个数就是Task的个数。
        注意：Application->Job->Stage->Task每一层都是1对n的关系。
 */
object Spark02_RDD_Dep {


    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.textFile("datas/word.txt")
        println(lines.dependencies)   // 打印这个rdd的依赖关系
        println("***************************")

        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.dependencies)   // 打印这个rdd的依赖关系
        println("***************************")

        val wordToOne: RDD[(String, Int)] = words.map({
            word => (word, 1)
        })
        println(wordToOne.dependencies)   // 打印这个rdd的依赖关系
        println("***************************")

        val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey((x, y) => x + y)
        println(wordToCount.dependencies)   // 打印这个rdd的依赖关系
        println("***************************")


        wordToCount.collect().foreach(println)


        sc.stop()
    }




}
