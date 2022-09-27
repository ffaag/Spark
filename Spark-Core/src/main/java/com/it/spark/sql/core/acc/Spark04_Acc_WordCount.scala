package com.it.spark.sql.core.acc

import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author ZuYingFang
 * @time 2022-03-20 12:54
 * @description
 */
object Spark04_Acc_WordCount {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)


        val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hello"))

        // 创建累加器对象
        var wcAcc = new MyAccumulator()

        // 向Spark进行注册
        sc.register(wcAcc, "wordCountAcc")

        rdd.foreach(word =>wcAcc.add(word))

        println(wcAcc.value)

        sc.stop()
    }

    /**
     * 自己定义一个累加器以实现Map数据类型的功能
     * 1 继承AccumulatorV2，定义泛型[IN, OUT]
     *          IN：累加器输入的数据类型
     *          OUT：累加器返回的数据类型
     * 2 重写方法
     */
    class  MyAccumulator extends AccumulatorV2 [String, mutable.Map[String, Long]]{

        private var wcMap = mutable.Map[String, Long]()


        // 判断是否为初始状态
        override def isZero: Boolean = wcMap.isEmpty


        // 复制累加器
        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator()


        // 重置累加器
        override def reset(): Unit = wcMap.clear()


        // 获取累加器需要计算的值
        override def add(word: String): Unit = {
            val newCnt: Long = wcMap.getOrElse(word, 0L) + 1L
            wcMap.update(word, newCnt)
        }


        // Driver合并多个分区内的累加器，即合并当前的和传进来的，并且更i性能当前的这个
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            val map1: mutable.Map[String, Long] = this.wcMap
            val map2: mutable.Map[String, Long] = other.value

            map2.foreach{
                case (word, count) =>{
                    val newCount: Long = map1.getOrElse(word, 0L) + count
                    map1.update(word, newCount)
                }
            }

        }

        // 返回结果
        override def value: mutable.Map[String, Long] = wcMap
    }

}
