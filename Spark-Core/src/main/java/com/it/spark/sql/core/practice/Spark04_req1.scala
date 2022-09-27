//package com.it.spark.core.practice
//
//import com.it.spark.core.practice.Spark04_req1.HotCategory
//import org.apache.spark.rdd.RDD
//import org.apache.spark.util.AccumulatorV2
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable
//
///**
// * @author ZuYingFang
// * @time 2022-03-27 16:02
// * @description Top10热门品类
// */
//object Spark04_req1 {
//
//    def main(args: Array[String]): Unit = {
//
//        /**
//         * 问题： 还是存在一个reduceByKey，因此还是存在shuffle操作
//         * 解决办法： 累加器，挨个遍历，不存在shuffle
//         */
//
//
//        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Hot10")
//        val sc = new SparkContext(sparkConf)
//
//        val accumulator = new HotCategoryAccumulator()
//        sc.register(accumulator,"hotCategoryAccumulator")
//
//        // 1 读取原始数据
//        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
//
//        rdd.flatMap(action => {
//            val datas: Array[String] = action.split("_")
//            if (datas(6) != "-1")
//                accumulator.add(datas(6), "click")
//            else if (datas(8) != "null")
//                datas(8).split(",").foreach(accumulator.add(_, "order"))
//            else if (datas(10) != "null")
//                datas(10).split(",").foreach(accumulator.add(_, "pay"))
//        })
//
//
//        // 5 将品类进行排序，取前十名，元组排序可以得到这个效果，因此可以得到这样一个数据结构(品类ID, (点击数量, 下单数量, 支付数量))
//        val result: mutable.Map[String, HotCategory] = accumulator.value
//
//        val categories: mutable.Iterable[HotCategory] = result.map(_._2)
//
//        categories.toList.sortWith((left, right) =>{
//
//            if(left.clickCnt > right.clickCnt) {
//                true
//            } else if(left.clickCnt == right.clickCnt){
//                if(left.orderCnt > right.orderCnt) {
//                    true
//                }  else if (left.orderCnt == right.orderCnt)
//
//
//        })
//
//        // 6 将结果打印到控制台
//        result.foreach(println)
//
//
//        sc.stop()
//
//    }
//
//    case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)
//
//    /**
//     * 输入应该为(品类ID， 行为类型)
//     * 输出类型为mutable.Map(String, HotCategory)
//     */
//    class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
//
//        private val hcMap = mutable.Map[String, HotCategory]()
//
//        override def isZero: Boolean = hcMap.isEmpty
//
//        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAccumulator()
//
//        override def reset(): Unit = hcMap.clear()
//
//        override def add(v: (String, String)): Unit = {
//            val cid: String = v._1
//            val actionType: String = v._2
//            val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
//            if (actionType == "click")
//                category.clickCnt += 1
//            else if(actionType == "order")
//                category.orderCnt += 1
//            else if(actionType == "pay")
//                category.payCnt += 1
//            hcMap.update(cid, category)
//
//        }
//
//        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
//            val map1: mutable.Map[String, HotCategory] = this.hcMap
//            val map2: mutable.Map[String, HotCategory] = other.value
//
//            map2.foreach{
//                case (cid, hc) =>{
//                    val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
//                    category.clickCnt += hc.clickCnt
//                    category.orderCnt += hc.orderCnt
//                    category.payCnt += hc.payCnt
//                    map1.update(cid, category)
//                }
//            }
//        }
//
//        override def value: mutable.Map[String, HotCategory] = hcMap
//    }
//
//}
