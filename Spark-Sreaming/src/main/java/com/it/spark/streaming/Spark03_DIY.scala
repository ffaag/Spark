package com.it.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

/**
 * @author ZuYingFang
 * @time 2022-04-01 18:14
 * @description  自定义数据源来采集
 */
object Spark03_DIY {

    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
        messageDS.print()

        ssc.start()
        ssc.awaitTermination()
    }

    /**
     * 定义泛型，传递参数，重写方法
     */
    class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){

        private var flag = true;

        override def onStart(): Unit = {
            new Thread(new Runnable {
                override def run(): Unit = {
                    while (flag){
                        val message: String = "采集到的数据为" + new Random().nextInt(10).toString
                        store(message)
                        Thread.sleep(500)
                    }
                }
            }).start()
        }

        override def onStop(): Unit = {
            flag = false
        }
    }



}
