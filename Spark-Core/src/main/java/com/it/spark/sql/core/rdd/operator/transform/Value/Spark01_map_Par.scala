package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark01_map_Par {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子  map
        /**
         * 1 rdd计算一个分区内的数据是一个一个执行的，即分区内数据执行是有序
         * 2 不同分区间的数据执行是并发的，与分区数和并行数有关
         */
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        val mapRDD: RDD[Int] = rdd.map(num => {
            println(">>>>>>>>>" + num)
            num
        })

        val mapRDD1: RDD[Int] = mapRDD.map(num => {
            println("###########" + num)
            num
        })

        mapRDD1.collect()


        sc.stop()

    }

}
