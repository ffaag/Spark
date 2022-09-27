package com.it.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author ZuYingFang
 * @time 2022-03-29 11:21
 * @description
 */
object Spark01_SQL_Basic {


    def main(args: Array[String]): Unit = {

        // 创建运行环境
        // 相互转换：RDD=>DataFrame=>DataSet 转换需要引入隐式转换规则，否则无法转换  spark不是包名，是上下文环境对象名
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._


        // 执行逻辑
        // 1 创建DataFrame
        val df: DataFrame = spark.read.json("datas/user.json") // 路径和textFile一样，根路径开始
        df.show()

        // 2 SQL 风格语法
        df.createOrReplaceTempView("user")
        spark.sql("select * from user").show()

        // 3 DSL风格
        df.select("username", "age").show()
        df.select('username, 'age + 1).show()

        // 4 相互转换
        val rdd1: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 28), (3, "wangwu", 20)))
        // （1）RDD => DataFrame
        val df1: DataFrame = rdd1.toDF("id", "name", "age")
        df1.show()
        // （2）DataFrame => DataSet
        val ds1: Dataset[User] = df1.as[User]
        ds1.show()
        // （3）DataSet => DataFrame
        val df2: DataFrame = ds1.toDF()
        df2.show()
        // （4）DataFrame => RDD
        val rdd2: RDD[Row] = df2.rdd
        rdd2.foreach(a => println(a.getString(1))) // 得到的RDD里面是一行一行的数据，获取数据类似jdbc里面那样，索引从0开始
        // （5）RDD => DataSet
        val ds2: Dataset[User] = rdd1.map { case (id, name, age) => User(id, name, age) }.toDS() // 先将rdd转换成bean类型的数据
        ds2.show()
        // （6）DataSet => RDD
        val rdd3: RDD[User] = ds2.rdd
        rdd3.foreach(user => println(user.toString))


        // 关闭环境
        spark.close()
    }

    case class User(id: Int, name: String, age: Int)


}
