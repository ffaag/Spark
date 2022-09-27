package com.it.spark.sql.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author ZuYingFang
 * @time 2022-03-26 16:20
 * @description 行动算子：触发作业执行的方法，底层代码调用的是环境对象的runJob方法，创建ActiveJob，并提交执行
 */
object Spark01_reduce {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // reduce：聚合RDD所有元素，现聚合分区内，再聚合分区间数据，直接出结果，不需要collect
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        val i: Int = rdd.reduce(_ + _)
        println(i)


        sc.stop()

    }


}
