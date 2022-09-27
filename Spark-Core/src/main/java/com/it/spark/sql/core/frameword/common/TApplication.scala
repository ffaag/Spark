package com.it.spark.sql.core.frameword.common

import com.it.spark.sql.core.frameword.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZuYingFang
 * @time 2022-03-28 12:49
 * @description
 */
trait TApplication {

    def start(master:String = "local[*]", app:String = "Application")(op : => Unit): Unit ={
        val sparkConf = new SparkConf().setMaster(master).setAppName(app)
        val sc = new SparkContext(sparkConf)
        EnvUtil.put(sc)

        try{
            op
        }catch {
            case ex => println(ex.getMessage)
        }

        sc.stop()
        EnvUtil.clear()
    }

}
