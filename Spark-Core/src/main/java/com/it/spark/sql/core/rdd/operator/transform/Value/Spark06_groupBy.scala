package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark06_groupBy {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子-groupBy：将数据进行分区，注意，同一组数据一定在一个分区内，一个分区内不一定只有一个组
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        // groupBy，根据函数来分组，函数返回值组为这一组的key，相同key的数据在同一组中
        val groupByRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)

        groupByRDD.collect().foreach(println)

        sc.stop()

    }

}
