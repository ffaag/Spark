package com.it.spark.sql.core.rdd.operator.transform.Key_Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 15:01
 * @description reduceByKey分区内和分区间计算规则是相同的
 */
object Spark04_aggregateByKey_Test {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)


        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("4", 4), ("4", 90), ("a", 6)), 2)

        // aggregateByKey最终返回值结果应该与初始值的类型保持一致
//        val value: RDD[(String, String)] = rdd.aggregateByKey("")(_ + _, _ + _)

        // 获取相同key的数据的平均值，这里v记录的是相同key的value值，t记录的就是(总数，次数)
        val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))((t, v) => {
            (t._1 + v, t._2 + 1)
        }, (t1, t2) => {
            (t1._1 + t2._1, t1._2 + t2._2)
        })

        val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
            case (num, int) => {
                num / int
            }
        }


        sc.stop()
    }

}
