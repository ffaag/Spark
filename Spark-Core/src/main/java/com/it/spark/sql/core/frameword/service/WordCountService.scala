package com.it.spark.sql.core.frameword.service

import com.it.spark.sql.core.frameword.common.TService
import com.it.spark.sql.core.frameword.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author ZuYingFang
 * @time 2022-03-28 12:35
 * @description
 */
class WordCountService extends TService{

    private val wordCountDao = new WordCountDao()

    override def dataAnalysis: Array[(String, Int)] ={

        val lines: RDD[String] = wordCountDao.readFile("datas/word.txt")

        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne: RDD[(String, Int)] = words.map({ word => (word, 1)})
        val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey((x, y) => x + y)
        wordToCount.collect()

    }

}
