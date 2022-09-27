package com.it.spark.sql.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-27 12:57
 * @description
 */
object Spark01_save {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1),
            ("b", 2),
            ("c", 3)
        ))

        rdd.saveAsTextFile("output1")
        rdd.saveAsObjectFile("output2")
        rdd.saveAsSequenceFile("output3")

        sc.stop()

    }

}
