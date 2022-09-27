package com.it.spark.sql.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-26 16:20
 * @description 从计算的角度, 算子以外的代码都是在Driver端执行, 算子里面的代码都是在Executor端执行
 * 如：collect().foreach(println)，这里这个foreach就是在driver端执行的，而println在executor执行
 * rdd.foreach()，这里的foreach在executor执行
 */
object Spark11_foreach_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
        val sc = new SparkContext(sparkConf)

        // foreach：分布式遍历RDD中的每一个元素，调用指定函数
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        val user = new User()

        // 这个foreach在driver端执行，里面的println则在executor端执行
        // RDD算子中传递的函数是会包含闭包操作的，会进行检测传进来的数据是否实现序列化，没有实现就不会执行
        rdd.foreach((num =>{println("age= " + (user.age + num))}))

        sc.stop()

    }

//    class User extends Serializable {
//        var age = 30
//    }

    // 自动添加序列化特质，并且自动加apply和unApply
    case class User() {
        var age = 30
    }

}
