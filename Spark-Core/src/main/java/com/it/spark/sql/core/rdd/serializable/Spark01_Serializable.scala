package com.it.spark.sql.core.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-26 20:31
 * @description
 */
object Spark01_Serializable {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "xiaofang"))

        val search = new Search("h")
        search.getMatch1(rdd).collect().foreach(println)
        sc.stop()

    }

    // 查询对象
    class Search(query: String) extends Serializable {
        def isMatch(s: String): Boolean = {
            s.contains(query)
        }

        // 函数序列化案例
        def getMatch1(rdd: RDD[String]): RDD[String] = {
            rdd.filter(isMatch)
        }


        // 属性序列化案例
        def getMatch2(rdd: RDD[String]): RDD[String] = {
            rdd.filter(x => x.contains(query))
        }
    }


}
