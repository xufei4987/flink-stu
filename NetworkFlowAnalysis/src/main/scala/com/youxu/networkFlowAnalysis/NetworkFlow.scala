package com.youxu.networkFlowAnalysis

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//窗口聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("E:\\IdeaProjects\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(data => {
        val datas = data.split(" ")
        val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timeStamp = format.parse(datas(3)).getTime
        ApacheLogEvent(datas(0).trim, datas(1).trim, timeStamp, datas(5).trim, datas(6).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new AggCount(), new WindowResult())
    val processStream = dataStream.keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
    processStream.print()

    env.execute("network flow job")
  }
}

class AggCount() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotUrls(topN: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor("url-state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    //把每一条数据存入状态列表
    urlState.add(value)
    //注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val urlViewCounts = new ListBuffer[UrlViewCount]
    val ite = urlState.get().iterator()
    while (ite.hasNext) {
      val urlViewCount = ite.next()
      urlViewCounts += urlViewCount
    }
    urlState.clear()
    val topNUrlViews = urlViewCounts.sortWith(_.count > _.count).take(topN)
    //格式化输出结果
    val result: StringBuilder = new StringBuilder
    result.append("时间：").append(new Date(timestamp - 1)).append("\n")
    for (i <- topNUrlViews.indices) {
      result.append("NO").append(i + 1).append(":").append("url=").append(topNUrlViews(i).url)
        .append(" 访问量=").append(topNUrlViews(i).count).append("\n")
    }
    result.append("==========================")
    out.collect(result.toString())
    Thread.sleep(1000)
  }
}
