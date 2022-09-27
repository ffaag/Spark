package com.it.spark.sql.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @author ZuYingFang
 * @time 2022-03-22 11:49
 * @description
 */
object Executor {

    def main(args: Array[String]): Unit = {
        // 启动服务器，接收数据
        val server = new ServerSocket(9999)
        println("服务器启动，等待接收数据")

        // 等待客户端的连接
        val client: Socket = server.accept()

        val in: InputStream = client.getInputStream
        val objectInputStream = new ObjectInputStream(in)

        val subTask: SubTask = objectInputStream.readObject().asInstanceOf[SubTask]

        val list: List[Int] = subTask.compute()

        println("计算节点[9999]计算的结果为" + list)


        objectInputStream.close()
        client.close()
        server.close()
    }
}
