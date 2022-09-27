package com.it.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @author ZuYingFang
 * @time 2022-04-01 18:14
 * @description 自定义数据源来采集
 */
object Spark10_Close {

    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))


        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)


        ssc.start()


        // 优雅的关闭，计算节点不再接收新的数据，而是将现有的数据处理完毕后关闭
        new Thread(new Runnable {
            override def run(): Unit = {
                while (true) {
                    if (true) { // 什么时候关闭我们的采集器
                        // 获取SparkStreaming的状态
                        val state: StreamingContextState = ssc.getState()
                        if (state == StreamingContextState.ACTIVE) {
                            ssc.stop(true, true)
                        }
                    }
                    Thread.sleep(5000)
                }
            }
        }).start()


        ssc.awaitTermination() // 阻塞main线程，因此需要创建一个新的线程去关闭采集器

    }

}
