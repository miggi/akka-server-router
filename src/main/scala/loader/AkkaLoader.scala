package loader

import java.util.concurrent.{TimeUnit, Executors}

import akka.actor._
import akka.routing.RoundRobinGroup
import org.apache.commons.codec.binary.Base64

import scala.util.Random

object AkkaLoader extends App with DBProvider {

  startH2()

  implicit val system = ActorSystem("AkkaLoader")
  val loader = system.actorOf(Props[Loader], name = "Loader")
  val refreshWorkersSeconds = 5

  val refreshCmd = "REFRESH"
  val startCmd   = "START"

  val scheduler = Executors
    .newSingleThreadScheduledExecutor()
    .scheduleAtFixedRate(
      new RefreshTask(loader), refreshWorkersSeconds, refreshWorkersSeconds, TimeUnit.SECONDS)

  val subscriber = system.actorOf(Props[Subscriber], name = "Subscriber")
  var frameCounter = 0

  loader ! refreshCmd

  while (true) {
    val delay = loadDelay(conn)
    loader ! startCmd

    Thread.sleep(delay)
  }
}

class Loader extends Actor with DBProvider {

  import loader.SqlStatements._

  var remoteRouter: ActorRef = null

  def generateHash() = {
    val buffer = new Array[Byte](1024) //TODO: parametrize in DB
    Random.nextBytes(buffer)
    Base64.encodeBase64String(buffer)
  }

  def refreshRRGroup() = {
    val workers = loadWorkers()
    val routees = for (host <- workers) yield s"akka.tcp://Workers@$host:5555/user/RemoteWorker"

    if (routees.nonEmpty) {
      println("Routees to refresh= " + routees)
      val routeesGroup = new RoundRobinGroup(routees).props()

      remoteRouter = context.actorOf(routeesGroup)
      println("Updated Router: " + remoteRouter.path)
      conn
        .createStatement()
        .executeUpdate(cleanWorkersSql)

    } else if (remoteRouter != null) {
      context.stop(remoteRouter)
      remoteRouter = null
    }
  }

  def receive = {
    case startCmd => {
      if (remoteRouter != null) remoteRouter ! generateHash()
    }
    case refreshCmd => refreshRRGroup()
    case msg: String => println(s"AKKA Loader: '$msg'")
  }
}

class Subscriber extends Actor with DBProvider {

  import loader.SqlStatements._

  private def subscribe(host: String) = {
    println(s"Got subscription! Registering = $host")

    try {
      register(host)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def receive = {
    case msg: String => subscribe(msg)
  }

  private def register(host: String) = {
    val conn = connection()
    val rsExisting = conn
      .createStatement()
      .executeQuery(s"$selectWorkers WHERE host = '$host'")

    if (!rsExisting.first()) {
      conn
        .createStatement()
        .executeUpdate(s"INSERT INTO public.workers (host) VALUES ('$host');")
    }
  }

}

class RefreshTask(actor: ActorRef) extends Runnable {
  override def run(): Unit = {
    println("Sheduled refresh ...")
    actor ! AkkaLoader.refreshCmd
  }
}


