package com.it.spark.sql.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-27 16:02
 * @description Top10热门品类
 */
object Spark03_req1 {

    def main(args: Array[String]): Unit = {

        /**
         * 问题：1 存在大量的shuffle操作(reduceByKey)
         * 解决办法：1 (品类ID, 点击数量) => (品类ID, (1, 0, 0))
         * (品类ID, 下单数量) => (品类ID, (0, 1, 0))
         * (品类ID, 支付数量) => (品类ID, (0, 0, 1))
         * 然后统计进行reduceByKey
         */


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Hot10")
        val sc = new SparkContext(sparkConf)

        // 1 读取原始数据
        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

        val rdd2: RDD[(String, (Int, Int, Int))] = rdd.flatMap(action => {
            val datas: Array[String] = action.split("_")
            if (datas(6) != "-1")
                List((datas(6), (1, 0, 0)))
            else if (datas(8) != "null")
                datas(8).split(",").map((_, (0, 1, 0)))
            else if (datas(10) != "null")
                datas(10).split(",").map((_, (0, 0, 1)))
            else
                Nil
        })


        // 5 将品类进行排序，取前十名，元组排序可以得到这个效果，因此可以得到这样一个数据结构(品类ID, (点击数量, 下单数量, 支付数量))
        val rdd3: RDD[(String, (Int, Int, Int))] = rdd2.reduceByKey((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3))

        val result: Array[(String, (Int, Int, Int))] = rdd3.sortBy(_._2, false).take(10)


        // 6 将结果打印到控制台
        result.foreach(println)


        sc.stop()

    }

}
