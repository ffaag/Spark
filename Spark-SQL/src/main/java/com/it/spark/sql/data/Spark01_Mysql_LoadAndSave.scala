package com.it.spark.sql.data

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
 * @author ZuYingFang
 * @time 2022-03-29 12:17
 * @description
 */
object Spark01_Mysql_LoadAndSave {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        // 读取mysql的数据
        val df: DataFrame = spark.read
          .format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/spark-sql")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "fangzu")
          .option("dbtable", "user")
          .load()
//        df.show()




        // 保存数据到Mysql
        df.write
          .format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/spark-sql")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "fangzu")
          .option("dbtable", "user1")
          .mode(SaveMode.Append)
          .save()

        spark.close()

    }

}
