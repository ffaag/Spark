package com.it.spark.sql.core.frameword.util

import org.apache.spark.SparkContext

/**
 * @author ZuYingFang
 * @time 2022-03-28 14:23
 * @description
 */
object EnvUtil {

    private val scLocal = new ThreadLocal[SparkContext]()  // 创建一个ThreadLocal，往里面放数据，整个线程都可以共享这个数据

    def put(sc:SparkContext): Unit ={
        scLocal.set(sc)
    }

    def get(): SparkContext ={
        scLocal.get()
    }

    def clear(): Unit ={
        scLocal.remove()
    }

}
