package com.it.spark.sql.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-22 17:40
 * @description
 */
object Spark02_RDD_File1 {

    def main(args: Array[String]): Unit = {

        // 准备环境
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("File")
        val sc = new SparkContext(conf)


        // 创建RDD————从文件中创建RDD，将文件中的数据作为处理的数据源
        // 1 path中的文件路径：默认以当前环境的根路径(项目)为基准，可以写绝对路径，也可以写相对路径
        val rdd: RDD[String] = sc.textFile("datas/1.txt")
        rdd.collect().foreach(println)

        // 2 path同时可以指向目录
        val rdd2: RDD[String] = sc.textFile("datas")
        rdd2.collect().foreach(println)

        // 3 path可以使用通配符*，*代表零个或多个字符
        val rdd3: RDD[String] = sc.textFile("datas/1*.txt")
        rdd3.collect().foreach(println)

        // 4 path可以分布式存储系统路径：HDFS
        val rdd4: RDD[String] = sc.textFile("hdfs://hadoop102:8020/test.txt")

        // 关闭环境
        sc.stop()


    }

}
