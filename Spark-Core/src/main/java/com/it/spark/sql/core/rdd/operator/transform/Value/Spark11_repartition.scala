package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark11_repartition {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子-repartition：底层执行coalesce(也可变大变小，变大必须shuffle)，且shuffle为true，可以将分区边大或者变小均可
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2) // 1 2    3 4    5 6


        val repartitionRDD: RDD[Int] = rdd.repartition(3)


        repartitionRDD.saveAsTextFile("output")


    }

}
