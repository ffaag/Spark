package com.it.spark.sql.practice

import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.sql._

/**
 * @author ZuYingFang
 * @time 2022-03-29 12:17
 * @description 早期的UDAF强类型聚合函数使用DSL语法操作
 */
object Spark01_Test {

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "xiaofang")
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()



        spark.sql("use xiaofang")

        // 准备数据
        spark.sql(
            """
              |CREATE TABLE `user_visit_action`(
              |`date` string,
              |`user_id` bigint,
              |`session_id` string,
              |`page_id` bigint,
              |`action_time` string,
              |`search_keyword` string,
              |`click_category_id` bigint,
              |`click_product_id` bigint,
              |`order_category_ids` string,
              |`order_product_ids` string,
              |`pay_category_ids` string,
              |`pay_product_ids` string,
              |`city_id` bigint)
              |row format delimited fields terminated by '\t'
              |""".stripMargin)

        spark.sql(
            """
              |load data local inpath 'data/user_visit_action.txt' into table xiaofang.user_visit_action
              |""".stripMargin)

        spark.sql(
            """
              |CREATE TABLE `product_info`(
              |`product_id` bigint,
              |`product_name` string,
              |`extend_info` string)
              |row format delimited fields terminated by '\t'
              |""".stripMargin)

        spark.sql(
            """
              |load data local inpath 'data/product_info.txt' into table xiaofang.product_info
              |""".stripMargin)

        spark.sql(
            """
              |CREATE TABLE `city_info`(
              |`city_id` bigint,
              |`city_name` string,
              |`area` string)
              |row format delimited fields terminated by '\t'
              |""".stripMargin)

        spark.sql(
            """
              |load data local inpath 'data/city_info.txt' into table city_info
              |""".stripMargin)

        // 这里的路径也是以工程的根路径为基准

        spark.close()

    }

}
