package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark03_mapPartitionsWithIndex {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        /**
         * mapPartitionsWithIndex，以分区为单位，把分区数据全部拿到了才开始进行逻辑处理
         * 里面的匿名函数,输入是一个 int 和迭代器，输出是一个迭代器，int是分区的索引
         * 缺点是必须将整个分区数据加载到内存中，处理完也不会释放掉，因为存在对象的引用，因此容易出现内存溢出
         */

        rdd.mapPartitionsWithIndex((index, iter) => {
            if (index == 1) { // 判断分区索引是不是1，分区索引从0开始
                iter
            } else {
                Nil.iterator
            }
        })

        sc.stop()

    }

}
