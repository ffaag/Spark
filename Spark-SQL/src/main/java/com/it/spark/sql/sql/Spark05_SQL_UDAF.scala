package com.it.spark.sql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
 * @author ZuYingFang
 * @time 2022-03-29 12:17
 * @description 早期的UDAF强类型聚合函数使用DSL语法操作
 */
object Spark05_SQL_UDAF {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val df: DataFrame = spark.read.json("datas/user.json")

        /**
         * 早期版本中，spark不能在sql中使用强类型的UDAF操作
         * SQL / DSL
         * 早期的UDAF强类型聚合函数使用DSL语法操作
         */
        val ds: Dataset[User] = df.as[User]

        // 将UDAF函数转换为查询的列对象
        val udafCol: TypedColumn[User, Long] = new MyAvgUDAF().toColumn
        ds.select(udafCol).show()

        spark.close()

    }

    /**
     * 自定义聚合函数类：计算年龄的平均值
     * org.apache.spark.sql.expressions.Aggregator
     * IN：输入的数据类型——User
     * BUF：缓冲区数据类型——Buff
     * OUT：输出的数据类型——Long
     */

    case class User(username:String, age:Long)
    case class Buff(var total:Long, var count:Long)

    class MyAvgUDAF extends Aggregator[User, Buff, Long] {

        // 缓冲区初始值
        override def zero: Buff = Buff(0L, 0L)

        // 根据输入的数据更新缓冲区的数据
        override def reduce(b: Buff, a: User): Buff = {
            b.total += a.age
            b.count += 1
            b
        }

        // 合并
        override def merge(b1: Buff, b2: Buff): Buff = {
            b1.count += b2.count
            b1.total += b2.total
            b1
        }

        // 计算结果
        override def finish(reduction: Buff): Long = reduction.total / reduction.count

        // 缓冲区编码操作
        override def bufferEncoder: Encoder[Buff] = Encoders.product

        // 输出的编码操作
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }

}
