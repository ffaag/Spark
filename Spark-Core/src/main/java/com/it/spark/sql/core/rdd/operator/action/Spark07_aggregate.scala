package com.it.spark.sql.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-26 16:20
 * @description 行动算子：触发作业执行的方法，底层代码调用的是环境对象的runJob方法，创建ActiveJob，并提交执行
 */
object Spark07_aggregate {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // aggregate：分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

        val i: Int = rdd.aggregate(10)(_ + _, _ + _)

        println(i)

        sc.stop()

    }


}
