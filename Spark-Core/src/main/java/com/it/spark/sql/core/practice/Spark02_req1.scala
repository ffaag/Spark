package com.it.spark.sql.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-27 16:02
 * @description  Top10热门品类
 */
object Spark02_req1 {

    def main(args: Array[String]): Unit = {

        /**
         * 问题：1 rdd被重复使用
         *      2 cogroup有可能存在shuffle，性能较低
         * 解决办法：1 rdd.cache()放到缓存中
         *          2 (品类ID, 点击数量) => (品类ID, (点击数量, 0, 0))
         *            (品类ID, 下单数量) => (品类ID, (0, 下单数量, 0))
         *            (品类ID, 支付数量) => (品类ID, (0, 0, 支付数量))
         *           然后可以两个进行聚合，这就没问题了
         */


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Hot10")
        val sc = new SparkContext(sparkConf)

        // 1 读取原始数据
        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        rdd.cache()


        // 2 统计品类点击数量：(品类ID, (点击数量, 0, 0))，为了效率可以先把那么没有点击的过滤掉
        val rdd2: RDD[(String, (Int, Int, Int))] = rdd.filter(_.split("_")(6) != "-1")
                                                   .map(line => (line.split("_")(6), 1))
                                                   .reduceByKey(_ + _).map(s => (s._1, (s._2, 0, 0)))


        // 3 统计品类下单数量：(品类ID, (0, 下单数量, 0))
        val rdd3: RDD[String] = rdd.filter(_.split("_")(8) != "null")
                                   .flatMap(line => line.split("_")(8).split(","))
        val rdd4: RDD[(String, (Int, Int, Int))] = rdd3.map((_, 1)).reduceByKey(_ + _).map(s => (s._1, (0, s._2, 0)))


        // 4 统计品类支付数量：(品类ID, (0, 0,支付数量))
        val rdd5: RDD[String] = rdd.filter(_.split("_")(10) != "null")
                                   .flatMap(line => line.split("_")(10).split(","))
        val rdd6: RDD[(String, (Int, Int, Int))] = rdd5.map((_, 1)).reduceByKey(_ + _).map(s => (s._1, (0, 0, s._2)))


        // 5 将品类进行排序，取前十名，元组排序可以得到这个效果，因此可以得到这样一个数据结构(品类ID, (点击数量, 下单数量, 支付数量))
        val rdd7: RDD[(String, (Int, Int, Int))] = rdd2.union(rdd4).union(rdd6)

        val rdd8: RDD[(String, (Int, Int, Int))] = rdd7.reduceByKey((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3))

        val result: Array[(String, (Int, Int, Int))] = rdd8.sortBy(_._2, false).take(10)


        // 6 将结果打印到控制台
        result.foreach(println)



        sc.stop()

    }

}
