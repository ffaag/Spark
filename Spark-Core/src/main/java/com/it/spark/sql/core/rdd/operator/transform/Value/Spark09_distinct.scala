package com.it.spark.sql.core.rdd.operator.transform.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-23 16:31
 * @description
 */
object Spark09_distinct {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)


        // RDD算子-distinct：去重
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4, 5))
        // 底层逻辑case _ => map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)  numPartitions==null
        val distinctRDD: RDD[Int] = rdd.distinct()


        distinctRDD.collect().foreach(println)


    }

}
