package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark08_sample {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子-sample：按照指定规则(函数返回true/flase)从数据集中抽取数据，不改变原数据
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

        /**
         * sample三个参数
         * withReplacement: Boolean 是否可放回抽取，true表示放回抽取，false不放回时的状态
         * fraction: Double  每条数据被抽取的概率 [0, 1] 0全不取，1全取
         *                   基准值的概念，算到的概率大于他就出来，小于就不出来
         * seed: Long = Utils.random.nextLong  随机数种子，不传递时使用的是当前的系统时间，这样不会重复
         */
//        val sampleRDD: RDD[Int] = rdd.sample(false, 0.4)
//
//        sampleRDD.collect().foreach(println)


        /**
         * sample三个参数  true放回时的状态
         * withReplacement: Boolean 是否可放回抽取，true表示放回抽取
         * fraction: Double  表示数据源中每条数据被抽取的可能次数
         * seed: Long = Utils.random.nextLong  随机数种子，不传递时使用的是当前的系统时间，这样不会重复
         */
        val sampleRDD: RDD[Int] = rdd.sample(true, 2)

        sampleRDD.collect().foreach(println)

    }

}
