package com.it.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ZuYingFang
 * @time 2022-03-29 12:17
 * @description
 */
object Spark02_SQL_UDF {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


        val df: DataFrame = spark.read.json("datas/user.json")
        df.createOrReplaceTempView("user")

        // 自定义函数
        spark.udf.register("prefixName",  (name :String) => {"Name: " + name})  // 这里的函数体不能省

        spark.sql("select age, prefixName(username) from user").show()


        spark.close()

    }

}
