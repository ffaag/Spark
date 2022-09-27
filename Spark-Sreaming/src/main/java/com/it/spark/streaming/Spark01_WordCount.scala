package com.it.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ZuYingFang
 * @time 2022-04-01 18:14
 * @description
 */
object Spark01_WordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))



        // 获取到端口发送过来的数据，一行一行的数据
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val words: DStream[String] = lines.flatMap(_.split(" "))
        val wordToOne: DStream[(String, Int)] = words.map((_, 1))
        val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)
        wordToCount.print()


        // 关闭环境   由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭ssc.stop()
        // main方法执行完。应用程序会自动关闭，因此不能让main执行完毕
        // 1 启动采集器
        ssc.start()
        // 2 等待采集器关闭
        ssc.awaitTermination()

    }

}
