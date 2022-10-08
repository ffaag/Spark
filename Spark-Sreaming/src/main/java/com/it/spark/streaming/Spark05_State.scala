package com.it.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ZuYingFang
 * @time 2022-04-01 18:14
 * @description
 */
object Spark05_State {

    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")  // 设置有状态操作时，需要设定缓冲区，buffer数据存在这
        import StreamingContext._

        // 无状态操作，只对当前采集周期内的数据进行处理，下一个周期的数据分到下个周期的结果去
        // 但是某些场合下需要进行将各个周期的数据进行汇总
//        val datas: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
//        val wordToCount: DStream[(String, Int)] = datas.map((_, 1)).reduceByKey(_ + _)
//        wordToCount.print()

        val datas: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        // updateStateByKey根据key对数据进行更新，第一个seq为相同key的value值，第二个opt为缓冲区为相同key的value值
        val wordToOne: DStream[(String, Int)] = datas.map((_, 1))
        val state: DStream[(String, Int)] = wordToOne.updateStateByKey(
            (seq: Seq[Int], buffer: Option[Int]) => {
                val newCount: Int = buffer.getOrElse(0) + seq.sum
                Option(newCount)
            }
        )
        state.print()

        ssc.start()
        ssc.awaitTermination()
    }

}
