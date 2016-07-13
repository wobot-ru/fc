package ru.wobot.net

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.http.Status
import play.api.libs.json.JsValue
import play.api.libs.ws.WSResponse
import play.api.{Environment, Mode}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

/**
  * Created by kviz on 7/5/2016.
  */
object Fetcher {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  val environment = Environment(new File("."), this.getClass.getClassLoader, Mode.Prod)


  lazy val ws = {
    import com.typesafe.config.ConfigFactory
    import play.api._
    import play.api.libs.ws._
    import play.api.libs.ws.ahc.{AhcWSClient, AhcWSClientConfig}
    import play.api.libs.ws.ahc.AhcConfigBuilder
    import org.asynchttpclient.AsyncHttpClientConfig

    val configuration = Configuration.reference ++ Configuration(ConfigFactory.parseString(
      """
        |ws.followRedirects = true
      """.stripMargin))

    val parser = new WSConfigParser(configuration, environment)
    val config = new AhcWSClientConfig(wsClientConfig = parser.parse())
    val builder = new AhcConfigBuilder(config)
    val logging = new AsyncHttpClientConfig.AdditionalChannelInitializer() {
      override def initChannel(channel: io.netty.channel.Channel): Unit = {
        channel.pipeline.addFirst("log", new io.netty.handler.logging.LoggingHandler("debug"))
      }
    }
    val ahcBuilder = builder.configure()
    ahcBuilder.setHttpAdditionalChannelInitializer(logging)
    val ahcConfig = ahcBuilder.build()
    new AhcWSClient(ahcConfig)
  }


  def fetchFriendsSync(id: Long): String = {
    Source.fromURL(s"http://api.vk.com/method/friends.get?user_id=$id").mkString
    //    val l: BufferedSource = Source.fromURL("http://httpbin.org/get?test=chakram", "windows-1251")
    //    l.mkString
  }

  def close(): Unit = {
    ws.close()
    system.terminate()
  }

  def fetch(uri: String): Future[Fetch] ={
    //println(s"---fetch | ThreadId=${Thread.currentThread().getId}")
    val id: Long = uri.substring(7).replaceAll("/", "").toLong
    fetchFriends(id)
  }

  def fetchFriends(id: Long): Future[Fetch] = {
    val api: String = s"http://api.vk.com/method/friends.get?user_id=$id"
    val url: String = s"vk://id$id"
    //val async: Future[WSResponse] = fetchAsync(id)

    //async.flatMap(x=>Future.successful(x.body))
    fetchAsync(api).map(x => {
      x.status match {
        case Status.OK => {
          (x.json \ "error").asOpt[JsValue] match {
            case Some(_) => ErrorFetch(url, x.json.toString())
            case _ => SuccessFetch[Seq[Long]](url, (x.json \ "response").as[Seq[Long]])
          }
        }
        case _ => ErrorFetch(api, x.statusText)
      }

    }).recover {
      case t: Throwable => ErrorFetch(api, t.toString)
    }
  }

  def fetchAsync(url: String): Future[WSResponse] = {
    //println(s"---fetch | ThreadId=${Thread.currentThread().getId}")
    ws
      .url(url)
      .get()
  }

  trait Fetch {
    def uri: String
  }

  case class SuccessFetch[T](uri: String, data: T) extends Fetch

  case class ErrorFetch[T](uri: String, msg: String) extends Fetch
}
