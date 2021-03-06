package com.youxu.marketAnalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//输入的广告点击事件样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

//按照省份统计的输出结果样例类
case class CountByProvince(windowEnd: String, province: String, count: Long)

//输出的黑名单报警信息
case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdStatisticsByGeo {
  //定义侧输出流的tag
  val blackListOutputTag:OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/AdClickLog.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val datas = data.split(",")
        AdClickEvent(datas(0).trim.toLong, datas(1).trim.toLong, datas(2).trim, datas(3).trim, datas(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //自定义processFunction过滤大量刷点击的行为
    val filterBlackListStream = dataStream
      .keyBy(data => (data.userId,data.adId))
      .process( new FilterBlackListUser(100) )

      val adCountStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new adCountAgg(), new AdCountResult())

    adCountStream.print("AdStatisticsByGeo")
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blackList")
    env.execute("AdStatisticsByGeo job")

  }
}

class adCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
  }
}

class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent]{
  //定义状态，保存当前用户对当前广告的点击量
  lazy val countState: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("count-state",classOf[Long]) )
  //保存是否发送过黑名单的状态
  lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState( new ValueStateDescriptor[Boolean]("issent-state",classOf[Boolean]) )
  //保存定时器触发的时间戳
  lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state",classOf[Long]))
  override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
    //取出count状态
    val curCount = countState.value()
    //如果是第一次处理，注册定时器  每天 00:00触发
    if(curCount == 0){
      val ts = (ctx.timerService().currentProcessingTime()/(1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
      resetTimer.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }
    //判断计数是否达到上限，如果达到则加入黑名单
    if(curCount > maxCount){
      //没有发送过黑名单
      if(!isSentBlackList.value()){
        isSentBlackList.update(true)
        //测输出流输出
        ctx.output(new OutputTag[BlackListWarning]("blackList"), BlackListWarning(value.userId,value.adId,s"click over $maxCount times today."))
      }
      return
    }
    countState.update(curCount + 1)
    //主流输出
    out.collect( value )
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
    //定时器触发 清空状态
    if(timestamp == resetTimer.value()){
      isSentBlackList.clear()
      countState.clear()
      resetTimer.clear()
    }
  }
}
