package com.it.spark.sql.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-20 12:54
 * @description
 */
object Spark04_WordCount {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)



        sc.stop()
    }


    def wordCount1(sc :SparkContext ):Unit={

        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val mapRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val rdd2: RDD[(String, Iterable[String])] = mapRDD.groupBy(word => word)
        val rdd3: RDD[(String, Int)] = rdd2.mapValues(iter => iter.size)
    }


    def wordCount2(sc :SparkContext ):Unit={

        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val mapRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val rdd1: RDD[(String, Int)] = mapRDD.map((_, 1))
        val rdd2: RDD[(String, Iterable[Int])] = rdd1.groupByKey(1)
        val rdd3: RDD[(String, Int)] = rdd2.mapValues(iter => iter.size)
    }


    def wordCount3(sc :SparkContext ):Unit={

        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val mapRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val rdd1: RDD[(String, Int)] = mapRDD.map((_, 1))
        val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)
    }

    def wordCount4(sc :SparkContext ):Unit={

        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val mapRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val rdd1: RDD[(String, Int)] = mapRDD.map((_, 1))
        val stringToLong: collection.Map[String, Long] = rdd1.countByKey()
    }

    def wordCount5(sc :SparkContext ):Unit={

        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val mapRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val stringToLong: collection.Map[String, Long] = mapRDD.countByValue()
    }

    def wordCount6(sc :SparkContext ):Unit={

        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val mapRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val rdd1: RDD[(String, Int)] = mapRDD.map((_, 1))
    }



}
