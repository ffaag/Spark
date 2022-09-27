package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark12_sortBy {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子-sortBy：排序前通过函数进行处理，按照处理结果进行排序，默认升序，不改变分区数量，中间存在shuffle
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("2", 1), ("12", 8), ("11", 2), ("1", 4), ("8", 9)), 2)

        val sortByRDD: RDD[(String, Int)] = rdd.sortBy(_._1.toInt, false)

        sortByRDD.saveAsTextFile("output")


    }

}
