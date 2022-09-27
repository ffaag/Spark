package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark06_groupBy_Test2 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子-groupBy：将数据进行分区，注意，同一组数据一定在一个分区内，一个分区内不一定只有一个组
        val rdd: RDD[String] = sc.textFile("datas/apache.log")

        val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(line => (line.split(" ")(3).split(":")(1), 1)).groupBy(_._1)
        val groupByRDD: RDD[(String, Int)] = timeRDD.map {
            case (hour, iter) => { // 偏函数，所以map用的大括号
                (hour, iter.size)
            }
        }

        groupByRDD.collect().foreach(println)


        sc.stop()

    }

}
