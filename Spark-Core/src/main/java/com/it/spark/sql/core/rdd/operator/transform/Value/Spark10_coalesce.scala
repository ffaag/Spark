package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark10_coalesce {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子-coalesce：在大数据集过滤后会产生很多小任务集，使用此方法收缩并合并分区，减少分区个数
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3) // 1 2    3 4    5 6

        // 默认情况下不会将分区的数据打乱重新组合，一个分区的数据还是在同一个分区，第二个参数是是否shuffle，true表示打乱
        //        val coalesceRDD: RDD[Int] = rdd.coalesce(2)// 1 2      3 4 5 6
        val coalesceRDD: RDD[Int] = rdd.coalesce(2, true) // 打乱了


        coalesceRDD.saveAsTextFile("output")


    }

}
