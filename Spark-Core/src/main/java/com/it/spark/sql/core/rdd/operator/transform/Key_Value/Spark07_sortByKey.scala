package com.it.spark.sql.core.rdd.operator.transform.Key_Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 15:01
 * @description
 */
object Spark07_sortByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // RDD算子：sortByKey————Key必须显示Ordered特质，按照key进行排序
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("4", 4), ("4", 90), ("a", 6)), 2)
        val sortByKeyRDD: RDD[(String, Int)] = rdd.sortByKey(true)
        sortByKeyRDD.collect().foreach(println)
        sc.stop()
    }

}
