package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark05_glom_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子-glom：将同一个分区的数据直接转换成相同类型的内存数组进行处理，分区不变
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        val glomRDD: RDD[Array[Int]] = rdd.glom()
        val maxRDD: RDD[Int] = glomRDD.map(_.max)

        println(maxRDD.collect().sum)

        sc.stop()

    }

}
