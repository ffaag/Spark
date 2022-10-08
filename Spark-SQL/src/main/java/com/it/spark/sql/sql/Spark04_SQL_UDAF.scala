package com.it.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, expressions, functions}

/**
 * @author ZuYingFang
 * @time 2022-03-29 12:17
 * @description 强类型，不用指定位置，而是可以直接使用属性，方便很多
 */
object Spark04_SQL_UDAF {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


        val df: DataFrame = spark.read.json("datas/user.json")
        df.createOrReplaceTempView("user")

        // 自定义函数
        spark.udf.register("ageAVG", functions.udaf(new MyAvgUDAF())) // 聚合函数自己自行定义类

        spark.sql("select ageAVG(age) from user").show()


        spark.close()

    }

    /**
     * 自定义聚合函数类：计算年龄的平均值
     * org.apache.spark.sql.expressions.Aggregator
     * IN：输入的数据类型——Long
     * BUF：缓冲区数据类型——Buff，自定义的样例类
     * OUT：输出的数据类型——Long
     */

    case class Buff(var total:Long, var count:Long)


    class MyAvgUDAF extends Aggregator[Long, Buff, Long] {

        // 缓冲区初始值
        override def zero: Buff = Buff(0L, 0L)   //

        // 根据输入的数据更新缓冲区的数据
        override def reduce(b: Buff, a: Long): Buff = {
            b.total += a
            b.count += 1
            b
        }

        // 合并
        override def merge(b1: Buff, b2: Buff): Buff = {
            b1.count += b2.count
            b1.total += b2.total
            b1
        }

        // 计算结果
        override def finish(reduction: Buff): Long = reduction.total / reduction.count

        // 缓冲区编码操作
        override def bufferEncoder: Encoder[Buff] = Encoders.product

        // 输出的编码操作
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }

}
