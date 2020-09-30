package com.lls.sc.lang

import java.util

object Hello {

    def main(args: Array[String]): Unit = {
        hello(args)
    }

    def hello(args: Array[String]): Unit = {
        if (args.isEmpty) {
            println("args is empty ${args.isEmpty}")
        }
        println("hello world!")

        val hashmap = new util.HashMap[Int, String]()
        val message = "Hello world!"
    }

    def searchFrom(i: Int, args: Array[String]): Int =
        if (i >= args.length) -1 // 不要越过最后一个参数
        else if (args(i).startsWith("-")) searchFrom(i + 1, args) // 跳过选项 else if (args(i).endsWith(".scala")) i // 找到!
        else searchFrom(i + 1, args) // 继续找

    def max(x: Int, y: Int): Int = {
        if (x > y) x else y
    }

}
