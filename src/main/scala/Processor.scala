import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime

/**
  * Created by Ben on 4/28/2017.
  */
object Processor extends App {

  override def main(args: Array[String]): Unit = {
    val logPath = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz"
    val sessionWindow = 60 * 15 * 1000 // max session idle time (millis)
    val numTopUsers = 10
    val conf = new SparkConf().setAppName("weblog processor")

    val outputs = processLogs(logPath, numTopUsers, sessionWindow, conf)
  }

  def processLogs(logFilePath: String, NTopUsers: Int, sessionWindow: Long, conf: SparkConf) : Outputs = {

    val sc = new SparkContext("local", "Simple App", "C;/Users/Ben/IdeaProjects/weblog")
    val sqlContext = new SQLContext(sc)

    // Parse only required fields
    val parsedLog = sc.textFile(logFilePath).map { line =>
      val logFormat = 	"""(.+)\s.+\s([\d\.]+)\:(\d+)\s.+\s.+\s.+\s.+\s\d+\s\d+\s\d+\s\d+\s\"(.+)\s(.+)\s(.+)\"\s\"(.*)\".*""".r
      val logFormat(ts, ip, port, reqType, path, ver, ua) = line
      AwsLog(DateTime parse ts getMillis, ip, port.toInt, reqType, path, ua)
    } cache()

    // Create a mapping of user -> hit timestamp. Users are identified using ip and ua
    val hits = parsedLog.map(hit => (hit.ip ++ hit.userAgent, hit.timestamp))
    // Group hits by ip+ua and for each hit generate an entry with all hits from this user
    val userHits = parsedLog.groupBy(l => l.ip ++ l.userAgent).mapValues(_.toList).map(g => (g._1,g._2.map(l => Hit(l.url, l.timestamp))))

    // join individual hits with all hits of a user. Filter to a set of hits belonging to the session of the hit and distinct the set of hits in the session to remove duplicates
    val sessions = hits.join(userHits).map(s => (s._1,filterSessionHits(s._2._1, s._2._2, sessionWindow))).distinct()
    val userSessions = sessions.groupBy(s => s._1).cache()

    // Count number of hits per session
    val sessionHits = userSessions.mapValues(allUserSessions => allUserSessions.map(s => s._2.times.size))
    // Count hits tp unique urls per session
    val sessionUniqueHits = userSessions.mapValues(allUserSessions => allUserSessions.map(s => s._2.urls.size))
    // mapping of user to a list of session lengths
    val sessionLengths = userSessions.mapValues(allUserSessions => allUserSessions.map(s => s._2.length))
    // average length of all sessions
    val avgSession = sessionLengths.flatMap(s => s._2).mean()
    // for every user select longest session, then sort descending and take top N users
    val mostEngaged = sessionLengths.map(u => (u._1, u._2.max)).sortBy(u => Long.MaxValue - u._2).take(NTopUsers)

    Outputs(sessionHits.collect(), avgSession, sessionUniqueHits.collect(), mostEngaged)
  }

  /**
    * Filters a list of hits based on timestamp to those hits that are in session with a provided timestamp
    * @param lineTs
    *         reference timestamp
    * @param otherHits
    *         list of hits that will be checked against lineTs
    * @return
    *         Session object containing the session length, a set of session timestamps and a list of paths
    */
  def filterSessionHits(lineTs: Long, otherHits: List[Hit], sessionWindow: Long): Session = {
    val sortedHits = otherHits.sortBy(h => h.ts)
    val curHitIdx = sortedHits.indexWhere(h => h.ts == lineTs)

    // Find first hit belonging to session
    var lowerIdx = curHitIdx
    var refTs = lineTs
    while (lowerIdx > 0 && Math.abs(refTs - sortedHits(lowerIdx - 1).ts) < sessionWindow ) {
      lowerIdx -= 1
      refTs = sortedHits(lowerIdx).ts
    }

    // Find last hit belonging to session
    var upperIdx = curHitIdx
    refTs = lineTs
    while ( upperIdx+1 < sortedHits.length && Math.abs(refTs - sortedHits(upperIdx + 1).ts) < sessionWindow ) {
      upperIdx += 1
      refTs = sortedHits(upperIdx).ts
    }

    upperIdx = upperIdx - lowerIdx + 1
    val sessionHits = sortedHits.splitAt(lowerIdx)._2.splitAt(upperIdx)._1
    val times = sessionHits.map(h => h.ts).toSet
    val paths = sessionHits.map(h => h.url).toSet
    Session(times.max - times.min, times, paths)
  }

  case class AwsLog(timestamp: Long,
                    // elb: String,
                    ip: String,
                    port: Int,
                    // backendIp: String,
                    // backendPort: Int,
                    // requestProcessingSecs: Double,
                    // backendProcessingSecs: Double,
                    // responseProcessingSecs: Double,
                    // elbStatusCode: Int,
                    // backendStatusCode: Int,
                    // receivedBytes: Long,
                    // sentBytes: Long,
                    requestType: String,
                    url: String,
                    userAgent: String
                    // sslCipher: String,
                    // sslProtocol: String
                   )

  case class UserSessions(uid: String, sessions: List[Session])
  case class Hit(url: String, ts: Long)
  case class Session(length: Long, times: Set[Long], urls: Set[String])
  case class Outputs(sessionHits: Array[(String, Iterable[Int])], avgSessionLength: Double, hitsPerSession: Array[(String, Iterable[Int])], mostEngaged: Array[(String, Long)])
}

