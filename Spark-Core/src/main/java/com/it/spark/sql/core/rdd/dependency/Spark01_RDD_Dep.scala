package com.it.spark.sql.core.rdd.dependency

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author ZuYingFang
 * @time 2022-03-26 21:00
 * @description
 */
object Spark01_RDD_Dep {


    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.textFile("datas/word.txt")
        println(lines.toDebugString)   // 打印这个rdd的血缘关系
        println("***************************")

        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.toDebugString)   // 打印这个rdd的血缘关系
        println("***************************")

        val wordToOne: RDD[(String, Int)] = words.map({
            word => (word, 1)
        })
        println(wordToOne.toDebugString)   // 打印这个rdd的血缘关系
        println("***************************")

        val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey((x, y) => x + y)
        println(wordToCount.toDebugString)   // 打印这个rdd的血缘关系
        println("***************************")


        wordToCount.collect().foreach(println)


        sc.stop()
    }




}
