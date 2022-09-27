package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark03_mapPartitionsWithIndex_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


        val rddMap: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, iter) => {
            iter.map((index, _))
        })

        rddMap.collect().foreach(println)


        sc.stop()

    }

}
