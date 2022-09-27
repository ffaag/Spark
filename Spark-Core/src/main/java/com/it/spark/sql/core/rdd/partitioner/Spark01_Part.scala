package com.it.spark.sql.core.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-27 12:46
 * @description
 */
object Spark01_Part {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        /**
         * Spark目前支持Hash分区和Range分区，和用户自定义分区。Hash分区为当前的默认分区。
         * 分区器直接决定了RDD中分区的个数、RDD中每条数据经过Shuffle后进入哪个分区，进而决定了Reduce的个数。
         * ➢ 只有Key-Value类型的RDD才有分区器，非Key-Value类型的RDD分区的值是None
         * ➢ 每个RDD的分区ID范围：0 ~ (numPartitions - 1)，决定这个值是属于那个分区的。
         * 1) Hash分区：对于给定的key，计算其hashCode,并除以分区个数取余
         * 2) Range分区：将一定范围内的数据映射到一个分区中，尽量保证每个分区数据均匀，而且分区间有序
         */

        val rdd: RDD[(String, String)] = sc.makeRDD(List(
            ("nba", "*********"),
            ("cba", "******"),
            ("wnba", "********"),
            ("nba", "*******")
        ), 3) // 想要的结果是nba放一号分区，其他分别放二号，三号分区，就得自定义分区

        val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)  // 使用自己的分区器分区
        partRDD.saveAsTextFile("output")

        sc.stop()

    }


    // 自定义分区器
    class MyPartitioner extends Partitioner {

        override def numPartitions: Int = 3


        // 根据数据的key返回数据所在的分区索引，从0开始
        override def getPartition(key: Any): Int = {
            key match {
                case "nba" => 0
                case "wnba" => 1
                case _ => 2
            }
        }
    }

}
