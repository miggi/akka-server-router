package loader

import java.util.concurrent.{ThreadLocalRandom, TimeUnit, Executors}

import akka.actor._
import akka.routing.RoundRobinGroup
import org.apache.commons.codec.binary.Base64

import scala.util.Random

object AkkaLoader extends App with DBProvider {

  startH2()

  implicit val system = ActorSystem("AkkaLoader")
  val loader = system.actorOf(Props[Loader], name = "Loader")

  val refreshWorkersSecs = 5
  val refreshCmd = "REFRESH"

  val scheduler = Executors
    .newSingleThreadScheduledExecutor()
    .scheduleAtFixedRate(
      new RefreshTask(loader), refreshWorkersSecs, refreshWorkersSecs, TimeUnit.SECONDS)

  val subscriber = system.actorOf(Props[Subscriber], name = "Subscriber")

  loader ! refreshCmd

  while (true) {
    val delay = loadDelay(conn)
    loader ! "START"
    Thread.sleep(delay)
  }
}

class Loader extends Actor with DBProvider {

  import loader.SqlStatements._

  var remoteRouter: ActorRef = null

  def generateRandomHash() = {
    val minBound = 1024
    val maxBound = 2048
    val randomized = ThreadLocalRandom.current().nextDouble(minBound, maxBound)

    val bufferSize =  randomized.toInt
    println (s"Propagating HASH with buffer size = $bufferSize")

    val buffer = new Array[Byte](bufferSize)

    Random.nextBytes(buffer)
    Base64.encodeBase64String(buffer)
  }

  def refreshRoundRobinGroup() = {
    val workers = loadWorkers()
    val routees = for (host <- workers) yield s"akka.tcp://Workers@$host:5555/user/RemoteWorker"

    if (routees.nonEmpty) {
      println("Routees to refresh = " + routees)
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
    case "START" =>
      if (remoteRouter != null) remoteRouter ! generateRandomHash()

    case "REFRESH" => refreshRoundRobinGroup()
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
    println("Scheduled refresh ...")
    actor ! "REFRESH"
  }
}


