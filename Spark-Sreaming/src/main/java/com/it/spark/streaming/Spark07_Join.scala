package com.it.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ZuYingFang
 * @time 2022-04-01 18:14
 * @description 自定义数据源来采集
 */
object Spark07_Join {

    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(5))


        val lines1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val lines2: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)

        val map1: DStream[(String, Int)] = lines1.map((_, 1))
        val map2: DStream[(String, Int)] = lines2.map((_, 1))

        val joinDS: DStream[(String, (Int, Int))] = map1.join(map2)

        joinDS.print()

        ssc.start()
        ssc.awaitTermination()
    }

}
