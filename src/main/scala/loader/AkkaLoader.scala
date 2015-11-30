package loader

import akka.actor._
import akka.routing.RoundRobinGroup
import org.apache.commons.codec.binary.Base64

import scala.util.Random

object AkkaLoader extends App with DBProvider {

  startH2()

  implicit val system = ActorSystem("AkkaLoader")

  val loader = system.actorOf(Props[Loader], name = "Loader")
  val subscriber = system.actorOf(Props[Subscriber], name = "Subscriber")
  val conn = connection()
  var frameCounter = 0

  loader ! "REFRESH"

  while (true) {
    val delay = loadDelay(conn)

    loader ! "FIRE"

    refreshIfNeeded(delay)
    Thread.sleep(delay)
  }

  def refreshIfNeeded(delay: Int) = {
    frameCounter += 1

    if (frameCounter % 3 == 0) {
      loader ! "REFRESH"
    }
  }
}

class Loader extends Actor with DBProvider {

  var remoteRouter: ActorRef = null

  def generateHash() = {
    val buffer = new Array[Byte](1024) //TODO: parametrize in DB
    Random.nextBytes(buffer)
    Base64.encodeBase64String(buffer)
  }

  def refreshRRGroup() = {

    val workers = loadWorkers()
    val routees = for (host <- workers) yield s"akka.tcp://Workers@$host:5555/user/RemoteWorker"
//    val routees = for (host <- workers) yield s"akka.tcp://Workers@127.0.0.1:5555/user/RemoteWorker"

    if (routees.nonEmpty) {
      println ("Routees to refresh= " + routees)
      val routeesGroup = new RoundRobinGroup(routees).props()

      remoteRouter = context.actorOf(routeesGroup)
      println("Updated Router: " + remoteRouter.path)
    }
  }

  def receive = {
    case "FIRE" =>{
      if (remoteRouter != null)  remoteRouter ! generateHash()}

    case "REFRESH" => refreshRRGroup()

    case msg: String =>
      println(s"AKKA Loader: '$msg'")
  }
}

class Subscriber extends Actor with DBProvider {

  import loader.SqlStatements._

  private def subscribe(host: String) = {
    println(s"Got subscription! Registering = $host")

    try {
      register(host)
    } catch {
      case e: Exception =>
        e.printStackTrace()
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


