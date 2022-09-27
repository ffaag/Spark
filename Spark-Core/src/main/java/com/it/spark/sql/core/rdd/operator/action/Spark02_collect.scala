package com.it.spark.sql.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-26 16:20
 * @description 行动算子：触发作业执行的方法，底层代码调用的是环境对象的runJob方法，创建ActiveJob，并提交执行
 */
object Spark02_collect {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // collect：以数组的形式返回数据集中的所有元素
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        val ints: Array[Int] = rdd.collect()

        println(ints.mkString(", "))

        sc.stop()

    }


}
