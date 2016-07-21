package ru.wobot.net

import java.io.{File, IOException}
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.http.Status
import play.api.libs.json.JsValue
import play.api.libs.ws.WSResponse
import play.api.{Environment, Mode}
import ru.wobot.Profile

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source

object Fetcher {

  trait Fetch {
    def uri: String

    def crawlDate: Long
  }

  case class SuccessFetch[T](uri: String, crawlDate: Long, data: T) extends Fetch

  case class ErrorFetch[T](uri: String, crawlDate: Long, msg: String) extends Fetch

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

  def fetch(uri: String): Future[Fetch] = {
    //println(s"---fetch | ThreadId=${Thread.currentThread().getId}")
    val id: Long = uri.substring(7).replaceAll("/", "").toLong
    val profile = fetchProfile(id)
    val friends = fetchFriends(id)

    val agg = for {
      p <- profile
      f <- friends
    } yield {
      val name: String = (p \ "first_name").as[String]
      val lastName: String = (p \ "last_name").as[String]
      SuccessFetch(uri, System.nanoTime(), new Profile(id, s"$name $lastName", f, p))
    }

    agg.recover {
      case t: Throwable => ErrorFetch(uri, System.nanoTime, t.toString)
    }
  }

  def fetchFriends(id: Long): Future[Seq[Long]] = {
    val api: String = s"http://api.vk.com/method/friends.get?user_id=$id"
    val url: String = s"vk://id$id"
    //val async: Future[WSResponse] = fetchAsync(id)

    //async.flatMap(x=>Future.successful(x.body))
    fetchAsync(api).map(x => {
      x.status match {
        case Status.OK => {
          (x.json \ "error").asOpt[JsValue] match {
            case Some(x) => throw new IOException(x.toString())
            case _ => (x.json \ "response").as[Seq[Long]]
          }
        }
        case _ => throw new IOException(s"Unexpected status ${x.status} ${x.statusText}")
      }
    })
  }

  def fetchProfile(id: Long): Future[JsValue] = {
    val api: String = s"http://api.vk.com/method/users.get?user_ids=$id&fields=sex,bdate,city,country,photo_50,photo_100,photo_200_orig,photo_200,photo_400_orig,photo_max,photo_max_orig,photo_id,online,online_mobile,domain,has_mobile,contacts,connections,site,education,universities,schools,can_post,can_see_all_posts,can_see_audio,can_write_private_message,status,last_seen,relation,relatives,counters,screen_name,maiden_name,timezone,occupation,activities,interests,music,movies,tv,books,games,about,quotes,personal,friend_status,military,career"
    val url: String = s"vk://id$id"
    //val async: Future[WSResponse] = fetchAsync(id)

    //async.flatMap(x=>Future.successful(x.body))
    fetchAsync(api).map(x => {
      x.status match {
        case Status.OK => {
          (x.json \ "error").asOpt[JsValue] match {
            case Some(x) => throw new IOException(x.toString())
            case _ => (x.json \ "response") (0).get
          }
        }
        case _ => throw new IOException(s"Unexpected status ${x.status} ${x.statusText}")
      }
    })
  }

  def fetchAsync(url: String): Future[WSResponse] = {
    //println(s"---fetch | ThreadId=${Thread.currentThread().getId}")
    ws
      .url(url)
      .get()
  }

}
