package com.it.spark.sql.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-20 12:54
 * @description
 */
object Spark02_WordCount {
    def main(args: Array[String]): Unit = {
        // 建立和Spark框架的连接
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)


        // 执行业务操作
        val lines: RDD[String] = sc.textFile("datas")

        val words: RDD[String] = lines.flatMap(_.split(" "))

        val wordToOne: RDD[(String, Int)] = words.map({
            word => (word, 1)
        })

        val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
            t => t._1
        )


        val wordToCount: RDD[(String, Int)] = wordGroup.map {
            case (word, list) => {
                list.reduce(
                    (t1, t2) => {
                        (t1._1, t1._2 + t2._2)
                    }
                )
            }
        }

        val array: Array[(String, Int)] = wordToCount.collect()
        array.foreach(println)

        // 关闭连接
        sc.stop()
    }


}
