package com.youxu.networkFlowAnalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloom {
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
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      //触发器，一般默认窗口关闭时触发；若是到了窗口关闭时触发等于说把整个窗口的数据(大批量)都缓存到了内存(此时内存是放不下那个大的数据的)中，也就没有放到redis的必要了，
      //应该改为：来一次数据就触发一次
      .trigger(new MyTrigger())
      .process(new UvCountWithBloomProcessFunction())

    dataStream.print("bloom uv")

    env.execute("bloom uv job")
  }
}

//自定义窗口触发器            -----类型[二元组 是map之后的数据类型, ]
class MyTrigger() extends Trigger[ (String, Long), TimeWindow ]{
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  //因为没有注册定时器，所以不用去清除
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //每来一条数据就，就直接触发窗口操作，并清空所有状态, 所以将count值存到redis
    TriggerResult.FIRE_AND_PURGE
  }
}

class UvCountWithBloomProcessFunction() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  lazy val jedis = new Jedis("192.168.15.101", 6379)
  lazy val bloom = new Bloom(1 << 29)

  //利用redis的位图来进行uv统计
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //位图的存储方式，以窗口的时间戳作为key,一个key对应一个位图，key是windowEnd,value是bitmap
    val storeKey = context.window.getEnd.toString
    var count = 0L
    //把每个窗口的uv count值也存入名为count的redis表, 存放内容为( windowEnd -> uvCount )，所以要先从redis中读取
    if (jedis.hget("count",storeKey) != null){
      count = jedis.hget("count",storeKey).toLong
    }
    //用布隆过滤器判断当前用户是否存在,MyTrigger中设置的是一条一条的处理，所以elements中只有一条,布隆过滤器中为String，所以转换.toString
    val userId = elements.last._2.toString
    //对userId进行hash计算，根据hash值去位图中找到对应的count, 61为随机值
    val offset = bloom.hash(userId, 61)
    //定义一个标识位，判断redis位图中有没有这一位, .getbit获取存储某一个值的某一位
    val isExist = jedis.getbit(storeKey, offset)
    if (! isExist){
      //如果不存在，位图对应位置置位1，count进行 count + 1
      jedis.setbit(storeKey,offset,true)
      jedis.hset("count", storeKey, (count +1).toString)
      out.collect(UvCount(storeKey.toLong, count + 1))
    }else {
      out.collect(UvCount(storeKey.toLong, count))
    }
  }
}

class Bloom(size: Long) {
  //1 << 27 就等于 2的27次方 1024 * 1024 * 2^4 * 2^3  1B等于8bit 在位图中可以代表8个数  所以16M内存可以表示1 << 27个数
  private val cap = if (size < 0) 1 << 27 else size

  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }

}