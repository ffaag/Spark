package com.it.spark.sql.core.rdd.operator.transform.Key_Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 15:01
 * @description reduceByKey分区内和分区间计算规则是相同的
 */
object Spark08_join {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // RDD算子：join————对一个(K, V)和(K, W)的两个RDD，返回(K, (V, W))的RDD，以第一个rdd为主，一次只会join一个
        // 没有匹配上的不显示
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("4", 4), ("4", 90), ("a", 6)))
        val rdd2: RDD[(String, String)] = sc.makeRDD(List(("a", "b"), ("a", "c"), ("a", "d"), ("4", "l")))
        val joinRDD: RDD[(String, (Int, String))] = rdd1.join(rdd2)
        joinRDD.collect().foreach(println)
        sc.stop()

        /**
         * (a,(1,b))
         * (a,(1,c))
         * (a,(1,d))
         * (a,(6,b))
         * (a,(6,c))
         * (a,(6,d))
         * (4,(4,l))
         * (4,(90,l))
         */
    }

}
