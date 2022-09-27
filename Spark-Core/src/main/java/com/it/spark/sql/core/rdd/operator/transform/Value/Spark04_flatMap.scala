package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark04_flatMap {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子
        val rdd = sc.makeRDD(List(List(1, 2), 3, List(3, 4)))

        /**
         * flatMap，先map然后将数据扁平化
         */

        val mapRDD = rdd.flatMap(
            data => {
                data match {
                    case list: List[_] => list
                    case num => List(num)
                }
            }
        )
        mapRDD.collect().foreach(println)

        sc.stop()

    }

}
