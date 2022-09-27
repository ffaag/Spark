package com.it.spark.sql.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-27 14:45
 * @description
 */
object Spark01_Acc {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)


        // 分区内计算，分区间也要计算
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        val i: Int = rdd.reduce(_ + _)

        // 这个foreach是每个分区都会执行，所以结果可能不是10
        // 结果是零，是因为sum在driver中，foreach里面执行的语句在executor中，改变的只是executor中的sum，driver中的sum没有变化还是0
        var sum = 0
        rdd.foreach( num => sum += num)
        println(sum)


        // 解决办法，使用累加器，累加器用来把Executor端变量信息聚合到Driver端。
        // 在Driver程序中定义的变量，在Executor端的每个Task都会得到这个变量的一份新的副本，
        // 每个task更新这些副本的值后，传回Driver端进行merge。


        sc.stop()


    }

}
