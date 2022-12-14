package com.it.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author ZuYingFang
 * @time 2022-04-01 18:14
 * @description
 */
object Spark02_Queue {

    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))


        val rddQueue = new mutable.Queue[RDD[Int]]()
        val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, false)
        val mapStream: DStream[(Int, Int)] = inputStream.map((_, 1))
        val reduceStream: DStream[(Int, Int)] = mapStream.reduceByKey(_ + _)
        reduceStream.print()


        ssc.start()
        for (i <- 1 to 5){
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }
        ssc.awaitTermination()


    }

}
