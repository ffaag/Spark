package com.it.spark.sql.core.frameword.controller

import com.it.spark.sql.core.frameword.common.TController
import com.it.spark.sql.core.frameword.service.WordCountService

/**
 * @author ZuYingFang
 * @time 2022-03-28 12:35
 * @description
 */
class WordCountController extends TController{

    private val wordCountService = new WordCountService()

    override def dispatch(): Unit ={
        val dataAnalysis: Array[(String, Int)] = wordCountService.dataAnalysis
        dataAnalysis.foreach(println)
    }

}
