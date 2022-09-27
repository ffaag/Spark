package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark02_mapPartitions_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子  map
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        /**
         * mapPartitions，以分区为单位，把分区数据全部拿到了才开始进行逻辑处理
         * 里面的匿名函数也是一样，输入是一个迭代器，输出也是一个迭代器
         * 缺点是必须将整个分区数据加载到内存中，处理完也不会释放掉，因为存在对象的引用，因此容易出现内存溢出
         */
        val mapRDD: RDD[Int] = rdd.mapPartitions(
            iter => {
                List(iter.max).iterator
            }
        )
        mapRDD.collect().foreach(println)

        sc.stop()

    }

}
