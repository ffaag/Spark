package com.it.spark.sql.core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket


/**
 * @author ZuYingFang
 * @time 2022-03-22 11:48
 * @description
 */
object Driver {

    def main(args: Array[String]): Unit = {

        val task = new Task()

        // 连接服务器
        val client1 = new Socket("localhost", 9999)

        val out1: OutputStream = client1.getOutputStream
        val objectOutputStream1 = new ObjectOutputStream(out1)

        val subTask1 = new SubTask()
        subTask1.logic = task.logic
        subTask1.data = task.data.take(2)


        objectOutputStream1.writeObject(subTask1)
        objectOutputStream1.flush()
        objectOutputStream1.close()
        client1.close()
        println("客户端9999数据发送完毕")



        val client2 = new Socket("localhost", 8888)
        val out2: OutputStream = client2.getOutputStream
        val objectOutputStream2 = new ObjectOutputStream(out2)


        val subTask2 = new SubTask()
        subTask2.logic = task.logic
        subTask2.data = task.data.takeRight(2)


        objectOutputStream2.writeObject(subTask2)
        objectOutputStream2.flush()
        objectOutputStream2.close()

        client2.close()


        println("客户端8888数据发送完毕")
    }

}
