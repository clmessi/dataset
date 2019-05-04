package structuredStreaming
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._


/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network.
  *
  * Usage: MapGroupsWithState <hostname> <port>
  * <hostname> and <port> describe the TCP server that Structured Streaming
  * would connect to receive data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  * `$ nc -lk 9999`
  * and then run the example
  * `$ bin/run-example sql.streaming.StructuredSessionization
  * localhost 9999`
  */
object StructuredSessionization {

  def main(args: Array[String]): Unit = {
//    if (args.length < 2) {
//      System.err.println("Usage: StructuredSessionization <hostname> <port>")
//      System.exit(1)
//    }

    val host = "192.168.11.24"
    val port = 9999

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("StructuredSessionization")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    // 将行分割成单词，将单词当作事件的 sessionId
    val events = lines
      .as[(String, Timestamp)]
      .flatMap { case (line, timestamp) => // 模式匹配
        line.split(" ").map(word => Event(sessionId = word, timestamp))
      }

    // Sessionize the events. 追踪事件的数量, session 会话的开始和结束时间戳，并报告会话更新.
    val sessionUpdates = events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

      case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>

        // If timed out, then remove session and send final update
        if (state.hasTimedOut) {
          val finalUpdate =
            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
          state.remove()
          finalUpdate
        } else {
          // Update start and end timestamps in session
          val timestamps = events.map(_.timestamp.getTime).toSeq
          val updatedSession = if (state.exists) {
            val oldSession = state.get
            SessionInfo(
              oldSession.numEvents + timestamps.size,
              oldSession.startTimestampMs,
              math.max(oldSession.endTimestampMs, timestamps.max))
          } else {
            SessionInfo(timestamps.size, timestamps.min, timestamps.max)
          }
          state.update(updatedSession)

          // Set timeout such that the session will be expired if no data received for 10 seconds
          state.setTimeoutDuration("10 seconds")
          SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
        }
    }

    // Start running the query that prints the session updates to the console
    val query = sessionUpdates
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
/** 用户自定义数据类型，表示输入事件 */
case class Event(sessionId: String, timestamp: Timestamp)

/**
  * 用户定义数据类型，用于存储 session 信息， 作为 mapGroupsWithState 中的 state
  *
  * @param numEvents        session 中收到的 event 总数
  * @param startTimestampMs 会话开始时，接收到的第一个   event 的 timestamp
  * @param endTimestampMs   会话超时前，接收到的最后一个 event 的 timestamp
  */
case class SessionInfo(
                        numEvents: Int,
                        startTimestampMs: Long,
                        endTimestampMs: Long) {

  /** 第一个和最后一个 event 之间， session 会话的持续时间 */
  def durationMs: Long = endTimestampMs - startTimestampMs
}

/**
  * 用户定义数据类型， 表示由 mapGroupsWithState 返回的 update 信息
  *
  * @param id          session 会话的 Id
  * @param durationMs  会话活跃时长，即, 从第一个 event 到它超时
  * @param numEvents   session 会话活跃期间接收到的 events 数量
  * @param expired     session 是活跃还是超时
  */
case class SessionUpdate(
                          id: String,
                          durationMs: Long,
                          numEvents: Int,
                          expired: Boolean)
