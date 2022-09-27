package com.it.spark.sql.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-26 16:20
 * @description 从计算的角度, 算子以外的代码都是在Driver端执行, 算子里面的代码都是在Executor端执行
 * 如：collect().foreach(println)，这里这个foreach就是在driver端执行的，而println在executor执行
 * rdd.foreach()，这里的foreach在executor执行
 */
object Spark11_foreach {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // foreach：分布式遍历RDD中的每一个元素，调用指定函数
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("a", 4), ("b", 5), ("c", 6)))

        // foreach是没有顺序的，不按顺序来做，按照executor端内存数据打印
        // 如果有collect的，那就是driver端内存集合进行，按顺序执行的
        rdd.foreach(tuple => println(tuple._1 + "---------->>>>>>>>>" + tuple._2))

        sc.stop()

    }


}
