package com.it.spark.sql.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-27 16:02
 * @description  Top10热门品类
 */
object Spark01_req1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Hot10")
        val sc = new SparkContext(sparkConf)

        // 1 读取原始数据
        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")


        // 2 统计品类点击数量：(品类ID, 点击数量)，为了效率可以先把那么没有点击的过滤掉
        val rdd2: RDD[(String, Int)] = rdd.filter(_.split("_")(6) != "-1")
                                          .map(line => (line.split("_")(6), 1))
                                          .reduceByKey(_ + _)


        // 3 统计品类下单数量：(品类ID, 下单数量)
        val rdd3: RDD[String] = rdd.filter(_.split("_")(8) != "null")
                                   .flatMap(line => line.split("_")(8).split(","))
        val rdd4: RDD[(String, Int)] = rdd3.map((_, 1)).reduceByKey(_ + _)


        // 4 统计品类支付数量：(品类ID, 支付数量)
        val rdd5: RDD[String] = rdd.filter(_.split("_")(10) != "null")
                                   .flatMap(line => line.split("_")(10).split(","))
        val rdd6: RDD[(String, Int)] = rdd5.map((_, 1)).reduceByKey(_ + _)


        // 5 将品类进行排序，取前十名，元组排序可以得到这个效果，因此可以得到这样一个数据结构(品类ID, (点击数量, 下单数量, 支付数量))
        val rdd7: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = rdd2.cogroup(rdd4, rdd6)
        val rdd8: RDD[(String, (Int, Int, Int))] = rdd7.mapValues {
            case (clickIter, orderIter, payIter) => {
                var clickCnt = 0
                if (clickIter.iterator.hasNext)
                    clickCnt = clickIter.iterator.next()

                var orderCnt = 0
                if (orderIter.iterator.hasNext)
                    orderCnt = orderIter.iterator.next()

                var payCnt = 0
                if (payIter.iterator.hasNext)
                    payCnt = payIter.iterator.next()

                (clickCnt, orderCnt, payCnt)
            }
        }
        val result: Array[(String, (Int, Int, Int))] = rdd8.sortBy(_._2, false).take(10)


        // 6 将结果打印到控制台
        result.foreach(println)



        sc.stop()

    }

}
