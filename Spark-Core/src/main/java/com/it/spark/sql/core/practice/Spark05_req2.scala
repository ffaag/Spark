package com.it.spark.sql.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-27 16:02
 * @description Top10热门品类
 */
object Spark05_req2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Hot10")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

        val top10: Array[String] = top10Category(rdd) // 获得前十的品类ID

        // 过滤原始数据，保留点击和前十品类ID
        val rdd2: RDD[String] = rdd.filter(line => top10.contains(line.split("_")(6)))

        // 由于一个session可能对应多个品类，因此我们这样写
        val rdd3: RDD[((String, String), Int)] = rdd2.map(line => {
            val datas: Array[String] = line.split("_")
            ((datas(6), datas(2)), 1) // ((品类ID, sessionID), 1)
        }).reduceByKey(_ + _)

        val rdd4: RDD[(String, (String, Int))] = rdd3.map {
            case ((cid, sid), sum) => {
                (cid, (sid, sum))
            }
        }

        // 相同品类放一起并取前十
        val rdd5: RDD[(String, List[(String, Int)])] = rdd4.groupByKey().mapValues(_.toList.sortWith(_._2 > _._2).take(10))


        rdd5.collect().foreach(println)

        sc.stop()

    }

    // 获取前十的品类
    def top10Category(rdd: RDD[String]): Array[String] = {
        val rdd2: RDD[(String, (Int, Int, Int))] = rdd.flatMap(action => {
            val datas: Array[String] = action.split("_")
            if (datas(6) != "-1")
                List((datas(6), (1, 0, 0)))
            else if (datas(8) != "null")
                datas(8).split(",").map((_, (0, 1, 0)))
            else if (datas(10) != "null")
                datas(10).split(",").map((_, (0, 0, 1)))
            else
                Nil
        })

        val rdd3: RDD[(String, (Int, Int, Int))] = rdd2.reduceByKey((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3))

        val result: Array[String] = rdd3.sortBy(_._2, false).take(10).map(_._1)

        return result

    }

}
