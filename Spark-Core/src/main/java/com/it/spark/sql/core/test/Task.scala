package com.it.spark.sql.core.test

/**
 * @author ZuYingFang
 * @time 2022-03-22 11:59
 * @description
 */
class Task extends Serializable {

    val data = List(1, 2, 3, 4)

    val logic: Int => Int = _ * 2

    // 计算
    def compute(): List[Int] = {
        data.map(logic)
    }

}
