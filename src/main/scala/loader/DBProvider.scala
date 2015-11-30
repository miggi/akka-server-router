package loader

import java.sql.Connection

import loader.SqlStatements._
import org.h2.jdbcx.JdbcConnectionPool
import org.h2.tools.Server

import scala.collection.mutable.ArrayBuffer

trait DBProvider {

  val dbUrl = "jdbc:h2:~/test"
  val user = "sa"
  val password = ""

  def startH2() = {
    val tcp = Server.createTcpServer().start
    val web = Server.createWebServer().start

    println(s"Started H2 Server TCP = ${tcp.getURL}")
    println(s"Started H2 Server WEB = ${web.getURL}")

    val conn = connection()
    List(dropWorkersSql, createWorkersSql, dropDelaySql, createDelaySql, initDelaySql)
      .foreach((sql: String) => conn.createStatement().execute(sql))

    conn.close()
  }

  def loadDelay(connection: Connection) = {
    val delayRs = connection
      .createStatement()
      .executeQuery(selectDelay)
    delayRs.next()
    delayRs.getInt("delay")
  }

  def loadWorkers(): List[String] = {
    val conn = connection()
    var workers = ArrayBuffer.empty[String]

    try {
      val rs = conn
        .createStatement()
        .executeQuery(selectWorkers)

      while (rs.next()) {
        val  host = rs.getString("host")
        workers += host
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      conn.close()
    }
    workers.toList
  }

  def connection() = {
    JdbcConnectionPool
      .create(dbUrl, user, password)
      .getConnection
  }

}
