package com.it.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ZuYingFang
 * @time 2022-04-01 18:14
 * @description 自定义数据源来采集
 */
object Spark06_Transform {

    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))


        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        // transform方法可以将底层的RDD获取到后进行操作，其实就是对DStream中的RDD应用转换
        // 可以写代码  Driver端 执行一次
        val value: DStream[String] = lines.transform(
            rdd => {
                // 可以写代码 Driver端 一个采集周期一个RDD，RDD源源不断过来，周期执行
                rdd.map(
                    str => {
                        // 可以写代码 Executor端 多遍
                        str
                    }
                )
            }
        )

        // 可以写代码 Driver端 执行一次
        lines.map(
            data => {
                // 可以写代码 Executor端 多遍
                data
            }
        )





        ssc.start()
        ssc.awaitTermination()
    }

}
