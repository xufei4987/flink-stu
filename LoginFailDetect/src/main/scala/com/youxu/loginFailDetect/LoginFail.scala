package com.youxu.loginFailDetect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class LoginEvent(userId:Long,ip:String,eventType:String,eventTime:Long)

case class Warning(userId:Long,firstFailTime:Long,lastFailTime:Long,warningMsg:String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val datas = data.split(",")
        LoginEvent(datas(0).trim.toLong, datas(1).trim, datas(2).trim, datas(3).trim.toLong)
      })
      //乱序数据需要设置waterMark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime*1000L
      })

    val warningStream = dataStream.keyBy(_.userId)
      .process( new LoginWarning(2) )
    warningStream.print("LoginFail")
    env.execute("LoginFail job")
  }
}

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{
  //定义状态，保存2秒内的所有失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state",classOf[LoginEvent]))
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    val loginFailList = loginFailState.get()

    if(value.eventType == "fail"){
      //第一次记录
      if(!loginFailList.iterator().hasNext){
        ctx.timerService().registerEventTimeTimer(value.eventTime*1000L + 2000L)
      }
      loginFailState.add(value)
    }else{
      loginFailState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    val loginFailList = loginFailState.get()
    val buffer = new ListBuffer[LoginEvent]
    loginFailList.forEach(data =>{
      buffer += data
    })
    if(buffer.size >= maxFailTimes){
      out.collect(Warning(buffer.head.userId,buffer.head.eventTime,buffer.last.eventTime,s"login fail in 2 second for ${buffer.length} times."))
    }
    loginFailState.clear()
  }
}
