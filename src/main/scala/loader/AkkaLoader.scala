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

  while (true) {
    val delay = loadDelay(conn)

    loader ! "FIRE"

    loader ! "REFRESH"
    Thread.sleep(delay)
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

    if (routees.nonEmpty) {
      val routeesGroup = new RoundRobinGroup(routees).props()
      if(remoteRouter !=null) {
        // TODO: need suspend and refresh!
       }
      remoteRouter = context.actorOf(routeesGroup, "router")
    }
  }

  def receive = {
    case "FIRE" => if (remoteRouter != null) remoteRouter ! generateHash()
    case "REFRESH" => refreshRRGroup()

    case msg: String =>
      println(s"AKKA Loader: '$msg'")
  }
}

class Subscriber extends Actor with DBProvider {

  private def subscribe(host: String) = {
    println(s"Got subscription! Registering = $host")

    try {
      connection()
        .createStatement()
        .executeUpdate(s"INSERT INTO public.workers (host) VALUES ('$host');")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def receive = {
    case msg: String => subscribe(msg)
  }

}

