package com.it.spark.sql.core.frameword.application

import com.it.spark.sql.core.frameword.common.TApplication
import com.it.spark.sql.core.frameword.controller.WordCountController

/**
 * @author ZuYingFang
 * @time 2022-03-28 12:35
 * @description
 */
object WordCountApplication extends App with TApplication {

    start(){
        val wordCountController = new WordCountController()
        wordCountController.dispatch()
    }

}
