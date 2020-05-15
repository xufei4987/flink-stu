package com.youxu.networkFlowAnalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream = env.readTextFile(getClass.getResource("/UserBehavior.csv").getPath)
      .map(data => {
      val datas = data.split(",")
      UserBehavior(datas(0).trim.toLong, datas(1).trim.toLong, datas(2).trim.toInt, datas(3).trim, datas(4).trim.toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .map(data => ("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print("pv count")

    env.execute("pv count job")
  }
}
