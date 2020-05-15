package com.youxu.marketAnalysis

import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import java.util.{Random, UUID}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

case class MarketingViewCount( windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long )

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource( new SimulatedEventSource() )
      .assignAscendingTimestamps(_.timestamp)
      .filter( _.behavior != "UNINSTALL" )
      .map( data => ((data.channel,data.behavior), 1L) )
      .keyBy(_._1)
      .timeWindow( Time.hours(1), Time.seconds(10) )
      .process( new MarketingCountByChannel() )

    dataStream.print("AppMarketingByChannel")

    env.execute("AppMarketingByChannel job")
  }
}

case class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior]{

  var running = true

  val behaviorTypes = Seq("CLICK","DOWNLOAD","INSTALL","UNINSTALL")

  val channelSets = Seq("wechat","weibo","appstore","huaweistore")

  val random = new Random()

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxEle = Long.MaxValue
    var count = 0L
    //随机生成数据
    while(running && count < maxEle){
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes(random.nextInt(behaviorTypes.size))
      val channel = channelSets(random.nextInt(channelSets.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id,behavior,channel,ts))

      count = count + 1
      TimeUnit.MILLISECONDS.sleep(10L)

    }
  }

  override def cancel(): Unit = running = false
}

class MarketingCountByChannel() extends ProcessWindowFunction[((String,String),Long),MarketingViewCount,(String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(context.window.getStart).toString
    val endTs = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size
    out.collect(MarketingViewCount(startTs,endTs,channel,behavior,count))
  }
}
