package com.it.spark.sql.core.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author ZuYingFang
 * @time 2022-03-20 12:54
 * @description
 */
object Spark01_WordCount {
    def main(args: Array[String]): Unit = {
        // 建立和Spark框架的连接
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)


        // 执行业务操作
        // 1 读取文件，获取一行一行的数据
        val lines: RDD[String] = sc.textFile("datas")

        // 2 将数据进行拆分成单词
        val words: RDD[String] = lines.flatMap(_.split(" "))

        // 3 将数据根据单词进行分组，便于统计(hello, hello, hello)
        val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

        // 4 对分组后的数据进行转换，得到每个单词的数量(hello, 3)
        val wordToCount: RDD[(String, Int)] = wordGroup.map {
            case (word, list) => {
                (word, list.size)
            }
        }

        // 5 将转换结果采集到控制台打印出来，执行算子，生成task，得到真正的数据
        val array: Array[(String, Int)] = wordToCount.collect()
        array.foreach(println)

        // 关闭连接
        sc.stop()
    }


}
