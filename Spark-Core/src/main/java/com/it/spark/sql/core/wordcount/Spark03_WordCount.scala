package com.it.spark.sql.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-20 12:54
 * @description
 */
object Spark03_WordCount {
    def main(args: Array[String]): Unit = {
        // 建立和Spark框架的连接
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)


        // 执行业务操作
        val lines: RDD[String] = sc.textFile("datas/word.txt")

        val words: RDD[String] = lines.flatMap(_.split(" "))

        val wordToOne: RDD[(String, Int)] = words.map({
            word => (word, 1)
        })


        // Spark框架提供了更多的功能，可以将分组和聚合使用一个方法来实现
        // reduceByKey：相同的key的数据，可以对value进行reduce聚合
        val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey((x, y) => x + y)

        val array: Array[(String, Int)] = wordToCount.collect()
        array.foreach(println)


        // 关闭连接
        sc.stop()
    }


}
