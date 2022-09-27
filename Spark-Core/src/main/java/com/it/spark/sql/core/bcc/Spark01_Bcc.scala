package com.it.spark.sql.core.bcc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author ZuYingFang
 * @time 2022-03-27 14:45
 * @description  广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。
 *               Spark中的广播变量就可以将闭包的数据保存到Executor的内存中
 *               Spark中的广播变量不能够更改 ： 分布式共享只读变量
 */
object Spark01_Bcc {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)


        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
//        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
        val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))


        // join类似于笛卡尔积，会导致数据量几何增长，并且影响shuffle的性能，不推荐使用
//        val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//        joinRDD.collect().foreach(println)   // (a,(1,4))  (b,(2,5))  (c,(3,6))

        rdd1.map{
            case (w, c)=>{
                val l: Int = map.getOrElse(w, 0)
                (w, (c, l))
            }
        }.collect().foreach(println)

        // 结果就是每个task都会保存一份map，当这个executor中有多个task时就保存了很多重复数据
        // 解决办法就是


        sc.stop()


    }

}
