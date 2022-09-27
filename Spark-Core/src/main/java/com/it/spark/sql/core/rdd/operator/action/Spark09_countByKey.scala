package com.it.spark.sql.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-26 16:20
 * @description 行动算子：触发作业执行的方法，底层代码调用的是环境对象的runJob方法，创建ActiveJob，并提交执行
 */
object Spark09_countByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // countByKey：相同key的数据进行计数
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("a", 4), ("b", 5), ("c", 6)))

        val stringToLong: collection.Map[String, Long] = rdd.countByKey()

        println(stringToLong.mkString(", "))

        sc.stop()

    }


}
