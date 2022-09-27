package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object  Spark06_groupBy_Test1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子-groupBy：将数据进行分区，注意，同一组数据一定在一个分区内，一个分区内不一定只有一个组
        val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "scala", "hadoop"), 2)
        val groupByRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

        groupByRDD.collect().foreach(println)

        sc.stop()

    }

}
