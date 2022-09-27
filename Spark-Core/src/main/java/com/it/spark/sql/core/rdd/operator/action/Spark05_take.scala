package com.it.spark.sql.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-26 16:20
 * @description 行动算子：触发作业执行的方法，底层代码调用的是环境对象的runJob方法，创建ActiveJob，并提交执行
 */
object Spark05_take {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // take：返回RDD的前几个元素组成的数组
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        val ints: Array[Int] = rdd.take(2)

        println(ints.mkString(", "))

        sc.stop()

    }


}
