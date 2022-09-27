package com.it.spark.sql.core.rdd.operator.transform.Key_Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 15:01
 * @description reduceByKey分区内和分区间计算规则是相同的
 */
object Spark09_leftOuterJoin {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // RDD算子：leftOuterJoin————左外连接，主表必有，从表数据可能有可能没有，没有为None
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("4", 4), ("4", 90), ("a", 6),("c",9)))
        val rdd2: RDD[(String, String)] = sc.makeRDD(List(("a", "b"), ("a", "c"), ("a", "d"), ("4", "l")))
        val leftOuterJoinRDD: RDD[(String, (Int, Option[String]))] = rdd1.leftOuterJoin(rdd2)
        val rightOuterJoinRDD: RDD[(String, (Option[Int], String))] = rdd1.rightOuterJoin(rdd2)
        leftOuterJoinRDD.collect().foreach(println)
        sc.stop()

    }

}
