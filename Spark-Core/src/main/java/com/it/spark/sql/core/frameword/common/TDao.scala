package com.it.spark.sql.core.frameword.common

import com.it.spark.sql.core.frameword.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * @author ZuYingFang
 * @time 2022-03-28 14:19
 * @description
 */
trait TDao {

    def readFile(path: String): RDD[String] ={
        EnvUtil.get().textFile(path)
    }

}
