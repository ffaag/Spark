package com.it.spark.sql.core.rdd.operator.transform.Key_Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 15:01
 * @description reduceByKey 和 groupByKey都会存在shuffle，经过磁盘效率很低
 *              但是reduceByKey支持分区内预聚合，即先在自己的分区内进行聚合，然后在不同分区间聚合，可以有效减少shuffle时落盘的数量，提升shuffle的性能
 */
object Spark02_groupByKey {


    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // RDD算子：groupByKey————将数据按照相同的Key对Value进行分组，相同Key数据在一个元组，第一个元素为key，第二个元素为value的集合

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 4), ("c", 90), ("a", 6), ("b", 175)), 2)
        val reduceByKeyRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey(2)
        reduceByKeyRDD.saveAsTextFile("output")

        sc.stop()
    }

}
