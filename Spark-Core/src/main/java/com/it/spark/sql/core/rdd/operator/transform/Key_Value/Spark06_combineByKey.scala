package com.it.spark.sql.core.rdd.operator.transform.Key_Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-24 15:01
 * @description reduceByKey分区内和分区间计算规则是相同的
 */
object Spark06_combineByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // RDD算子：combineByKey————方法需要三个参数，允许用户返回值与输入不一致
        // 第一个参数，将相同key的第一个数据进行结构的转换
        // 第二个参数，分区内计算规则，第三个参数，分区间计算规则
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("4", 4), ("4", 90), ("a", 6)), 2)
        val newRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
            v=>(v,1),
            (t, v) =>  (t._1 + v, t._2 + 1),
            (t1, t2) => {
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
