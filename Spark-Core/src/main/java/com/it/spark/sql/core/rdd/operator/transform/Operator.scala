package com.it.spark.sql.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-26 15:32
 * @description
 */
object Operator {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("datas/agent.log", 1)

        val rdd1: RDD[Array[String]] = rdd.map(_.split(" "))

        val mapRDD: RDD[((String, String), Int)] = rdd1.map(line => ((line(1), line(4)), 1))  // ((省份，广告), 1)

        val rdd2: RDD[(String, (String, Int))] = mapRDD.reduceByKey(_ + _).map((tuple) => (tuple._1._1, (tuple._1._2, tuple._2)))   // (省份, (广告, sum))

        val rdd3: RDD[(String, Iterable[(String, Int)])] = rdd2.groupByKey(1)


        val rdd4: RDD[(String, List[(String, Int)])] = rdd3.sortByKey().mapValues(iter => iter.toList.sortWith(_._2 > _._2).take(3))


        rdd4.saveAsTextFile("output")

        sc.stop()
    }

}
