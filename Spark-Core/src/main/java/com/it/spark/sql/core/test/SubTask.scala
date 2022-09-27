package com.it.spark.sql.core.test

/**
 * @author ZuYingFang
 * @time 2022-03-22 12:15
 * @description
 */
class SubTask extends Serializable {

    var data:List[Int] = _

    var logic: Int => Int = _

    // 计算
    def compute(): List[Int] = {
        data.map(logic)
    }
}
