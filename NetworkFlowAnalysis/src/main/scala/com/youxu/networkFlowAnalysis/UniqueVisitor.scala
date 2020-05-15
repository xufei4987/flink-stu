package com.youxu.networkFlowAnalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class UvCount(windowEnd: Long, uvCount: Long)

/**
 * uv count> UvCount(1511661600000,28196)
 * uv count> UvCount(1511665200000,32160)
 * uv count> UvCount(1511668800000,32233)
 * uv count> UvCount(1511672400000,30615)
 * uv count> UvCount(1511676000000,32747)
 * uv count> UvCount(1511679600000,33898)
 * uv count> UvCount(1511683200000,34631)
 * uv count> UvCount(1511686800000,34746)
 * uv count> UvCount(1511690400000,32356)
 * uv count> UvCount(1511694000000,13)
 * end time :3714
 */
object UniqueVisitor {
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
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())

    dataStream.print("uv count")
    val startTime = System.currentTimeMillis()
    env.execute("uv count job")
    print("cost time :" + (System.currentTimeMillis() - startTime))
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    var idSet = Set[Long]()
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }
    out.collect(new UvCount(window.getEnd, idSet.size))
  }
}
