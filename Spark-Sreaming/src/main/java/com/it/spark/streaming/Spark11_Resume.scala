package com.it.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @author ZuYingFang
 * @time 2022-04-01 18:14
 * @description 关闭后重新启动，需要恢复之前的数据
 */
object Spark11_Resume {

    def main(args: Array[String]): Unit = {

        val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => {
            val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
            val ssc = new StreamingContext(sparkConf, Seconds(3))


            val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
            ssc
        })
        ssc.checkpoint("cp")   // 将数据保存到缓存点中


        ssc.start()
        ssc.awaitTermination() // 阻塞main线程，因此需要创建一个新的线程去关闭采集器
    }

}
