package spark.deploy.client

import spark.deploy._
import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.pattern.AskTimeoutException
import spark.{SparkException, Logging}
import akka.remote.RemoteClientLifeCycleEvent
import akka.remote.RemoteClientShutdown
import spark.deploy.RegisterApplication
import spark.deploy.master.Master
import akka.remote.RemoteClientDisconnected
import akka.actor.Terminated
import akka.dispatch.Await

/**
  * The main class used to talk to a Spark deploy cluster. Takes a master URL, an app description,
  * and a listener for cluster events, and calls back the listener when various events occur.
  * 这是一个和集群交互的主要actor类，包含如下内容：master url， 应用描述，用于监听集群事件的监听器，当有不同的事件发生的时候
  * 回调监听器，被 SparkDeploySchedulerBackend的start 调用
  */
private[spark] class Client(
                             actorSystem: ActorSystem, //actor主根
                             masterUrl: String, // 指向master的url  spark://xxxx
                             appDescription: ApplicationDescription, //应用描述 应用名称，核数，每个节点内存，执行命令，以及spark的跟目录
                             listener: ClientListener) //监听器
  extends Logging {

  var actor: ActorRef = null
  var appId: String = null

  class ClientActor extends Actor with Logging {
    var master: ActorRef = null
    var masterAddress: Address = null
    var alreadyDisconnected = false // To avoid calling listener.disconnected() multiple times

    override def preStart() { // actor实例化的时候就会执行
      logInfo("Connecting to master " + masterUrl)
      try {
        master = context.actorFor(Master.toAkkaUrl(masterUrl)) //获得到master的actor通信实例
        masterAddress = master.path.address
        master ! RegisterApplication(appDescription) //通知master，现在有一个计算任务app要注册
        context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
        context.watch(master) // Doesn't work with remote actors, but useful for testing
      } catch {
        case e: Exception =>
          logError("Failed to connect to master", e)
          markDisconnected()
          context.stop(self)
      }
    }

    override def receive = {
      case RegisteredApplication(appId_) => // app实例注册成功
        appId = appId_
        listener.connected(appId)

      case ApplicationRemoved(message) =>
        logError("Master removed our application: %s; stopping client".format(message))
        markDisconnected()
        context.stop(self)

      case ExecutorAdded(id: Int, workerId: String, host: String, cores: Int, memory: Int) =>
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, host, cores))
        listener.executorAdded(fullId, workerId, host, cores, memory)

      case ExecutorUpdated(id, state, message, exitStatus) =>
        val fullId = appId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus)
        }

      case Terminated(actor_) if actor_ == master =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case RemoteClientDisconnected(transport, address) if address == masterAddress =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case RemoteClientShutdown(transport, address) if address == masterAddress =>
        logError("Connection to master failed; stopping client")
        markDisconnected()
        context.stop(self)

      case StopClient =>
        markDisconnected()
        sender ! true
        context.stop(self)
    }

    /**
      * Notify the listener that we disconnected, if we hadn't already done so before.
      */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }
  }

  def start() {
    // Just launch an actor; it will call back into the listener.
    actor = actorSystem.actorOf(Props(new ClientActor)) // 被 SparkDeploySchedulerBackend的start 调用
  }

  def stop() {
    if (actor != null) {
      try {
        val timeout = 5.seconds
        val future = actor.ask(StopClient)(timeout)
        Await.result(future, timeout)
      } catch {
        case e: AskTimeoutException => // Ignore it, maybe master went away
      }
      actor = null
    }
  }
}
