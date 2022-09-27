package com.it.spark.sql.core.rdd.operator.transform.Key_Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 15:01
 * @description
 */
object Spark01_partitionBy {


    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
          val sc = new SparkContext(sparkConf)

        // RDD算子：partitionBy————将数据按照指定的Partitioner重新进行分区，Spark默认使用HashPartitioner
        // 可以改变分区数量，HashPartitioner底层规则为 key.hashCode % numPartitions
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
        val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
        val partitionByRDD: RDD[(Int, Int)] = mapRDD.partitionBy(new HashPartitioner(4))
        partitionByRDD.saveAsTextFile("output")

        sc.stop()
    }

}
