package com.it.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ZuYingFang
 * @time 2022-04-01 18:14
 * @description 自定义数据源来采集
 */
object Spark08_WindowOperation {

    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")


        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        val value: DStream[(String, Int)] = lines.map((_, 1))
        // 9s的窗口，3s的滑动，明显有6s重复了，第一个函数对新增加的3s进行操作，第二个函数对失去的3个进行操作
        // 这中间的6s就要进行保存，因此要设置缓冲区
        val value1: DStream[(String, Int)] = value.reduceByKeyAndWindow(
            _ + _, _ - _, Seconds(9), Seconds(3)
        )
        value1.print()


        ssc.start()
        ssc.awaitTermination()
    }

}
