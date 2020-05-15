package com.youxu.hotItemsAnalysis

import java.util.{Date, Properties}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义窗口聚合结果的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //创建执行环节
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.16.26.16:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //读取数据
    //    val dataStream = env.readTextFile("E:\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("hotItems", new SimpleStringSchema(), properties))
      .map(data => {
        val datas = data.split(",")
        UserBehavior(datas(0).trim.toLong, datas(1).trim.toLong, datas(2).trim.toInt, datas(3).trim, datas(4).trim.toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000L)

    //处理数据
    val processStream = dataStream.filter(_.behavior.equals("pv"))
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd) //按照窗口分组
      .process(new TopNHotItems(3))

    //sink:控制台输出
    processStream.print()

    env.execute("hot items job")
  }
}

//自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义预聚合函数计算平均数
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp, acc._2 + 1)

  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1 + acc1._1, acc._2 + acc1._2)
}

//自定义窗口函数计算输出ItemViewCount
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

//自定义处理函数
class TopNHotItems(topN: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //把每一条数据存入状态列表
    itemState.add(i)
    //注册一个定时器
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  //定时器触发时，对所有的数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //将所有的state中的数据取出，放到一个List Buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]
    val itemViewCounts = itemState.get()
    itemViewCounts.forEach(item => {
      allItems += item
    })
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topN)
    //清空状态
    itemState.clear()
    //将排名结果格式化输出
    val builder = new StringBuilder()
    builder.append("time：").append(new Date(timestamp - 1)).append("\n")

    for (i <- sortedItems.indices) {
      val currentItem = sortedItems(i)
      builder.append("No").append(i + 1).append(":")
        .append("商品ID=").append(currentItem.itemId)
        .append("浏览量=").append(currentItem.count)
        .append("\n")
    }
    builder.append("=======================")

    out.collect(builder.toString())


  }
}
