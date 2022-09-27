package com.it.spark.sql.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-27 16:02
 * @description Top10热门品类
 */
object Spark06_req3 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Hot10")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

        val rdd2: RDD[UserVisitAction] = rdd.map(line => {
            val data: Array[String] = line.split("_")
            UserVisitAction(data(0), data(1).toLong, data(2), data(3).toLong, data(4), data(5), data(6).toLong, data(7).toLong, data(8), data(9),
                data(10), data(11), data(12).toLong)
        })
        rdd2.cache()

        // 先计算分母
        val map: Map[Long, Long] = rdd2.map(user => (user.page_id, 1L)).reduceByKey(_ + _).collect().toMap


        // 计算分子，先对sessionID进行分组，然后根据时间来排序，得到访问的顺序，注意了，排序是对里面的value排序，因此要用mapValues
        val rdd3: RDD[(String, List[((Long, Long), Int)])] = rdd2.groupBy(_.session_id).mapValues(iter => {
            val list: List[Long] = iter.toList.sortBy(_.action_time).map(_.page_id) // 同一个sessionId的按时间访问页面的序列
            val tuples: List[(Long, Long)] = list.zip(list.tail) // 滑窗     拉链一样可以实现
            tuples.map((_, 1))
        })
        val rdd4: RDD[((Long, Long), Int)] = rdd3.map((_._2)).flatMap(list => list).reduceByKey(_ + _)
        rdd4.foreach {
            case ((pageid1, pageid2), sum) => {
                println(s"页面${pageid1}--->>>${pageid2}的页面跳转率为："  + (sum.toDouble / map.getOrElse(pageid1, 0L)))
            }
        }


        sc.stop()

    }

    //用户访问动作表
    case class UserVisitAction(
                                date: String, //用户点击行为的日期
                                user_id: Long, // 用 户 的 ID
                                session_id: String, //Session 的 ID
                                page_id: Long, // 某 个 页 面 的 ID
                                action_time: String, //动作的时间点
                                search_keyword: String, //用户搜索的关键词
                                click_category_id: Long, // 某 一 个 商 品 品 类 的 ID
                                click_product_id: Long, // 某 一 个 商 品 的 ID
                                order_category_ids: String, //一次订单中所有品类的 ID 集合
                                order_product_ids: String, //一次订单中所有商品的 ID 集合
                                pay_category_ids: String, //一次支付中所有品类的 ID 集合
                                pay_product_ids: String, //一次支付中所有商品的 ID 集合
                                city_id: Long) //城市 id
}
