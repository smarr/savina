package edu.rice.habanero.benchmarks.concdict

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.concdict.DictionaryConfig.{DoWorkMessage, EndWorkMessage, ReadMessage, WriteMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration
import akka.actor.ActorSystem
import som.Random

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object DictionaryAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new DictionaryAkkaActorBenchmark)
  }

  private final class DictionaryAkkaActorBenchmark extends Benchmark {
    var system : ActorSystem = null
    
    def initialize(args: Array[String]) {
      DictionaryConfig.parseArgs(args)
    }

    def printArgInfo() {
      DictionaryConfig.printArgs()
    }

    def runIteration() : Future[Int] = {
      system = AkkaActorState.newActorSystem("Dictionary")
      val p = Promise[Int]
      
      val numWorkers: Int = DictionaryConfig.NUM_ENTITIES
      val numMessagesPerWorker: Int = DictionaryConfig.NUM_MSGS_PER_WORKER

      val master = system.actorOf(Props(new Master(p, numWorkers,
          numMessagesPerWorker)))
      AkkaActorState.startActor(master)

      return p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      return DictionaryConfig.verifyResult(n)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private class Master(completion: Promise[Int], numWorkers: Int, numMessagesPerWorker: Int) extends AkkaActor[AnyRef] {

    private final val workers = new Array[ActorRef](numWorkers)
    private final val dictionary = context.system.actorOf(Props(
        new Dictionary(completion)))
    private var numWorkersTerminated: Int = 0

    override def onPostStart() {
      AkkaActorState.startActor(dictionary)

      var i: Int = 0
      while (i < numWorkers) {
        workers(i) = context.system.actorOf(Props(new Worker(self, dictionary, i, numMessagesPerWorker)))
        AkkaActorState.startActor(workers(i))
        workers(i) ! new DoWorkMessage()
        i += 1
      }
    }

    override def process(msg: AnyRef) {
      if (msg.isInstanceOf[DictionaryConfig.EndWorkMessage]) {
        numWorkersTerminated += 1
        if (numWorkersTerminated == numWorkers) {
          dictionary ! new EndWorkMessage()
          exit()
        }
      }
    }
  }

  private class Worker(master: ActorRef, dictionary: ActorRef, id: Int, numMessagesPerWorker: Int) extends AkkaActor[AnyRef] {

    private final val writePercent = DictionaryConfig.WRITE_PERCENTAGE
    private var messageCount: Int = 0
    private final val random = new Random(id + numMessagesPerWorker + writePercent)

    override def process(msg: AnyRef) {
      messageCount += 1
      if (messageCount <= numMessagesPerWorker) {
        val anInt: Int = random.next(100)
        if (anInt < writePercent) {
          dictionary ! new WriteMessage(self, random.next(), random.next())
        } else {
          dictionary ! new ReadMessage(self, random.next())
        }
      } else {
        master ! new EndWorkMessage()
        exit()
      }
    }
  }

  private class Dictionary(completion: Promise[Int]) extends AkkaActor[AnyRef] {

    private[concdict] final val dataMap = DictionaryConfig.createDataMap(
        DictionaryConfig.DATA_LIMIT)

    override def process(msg: AnyRef) {
      msg match {
        case writeMessage: DictionaryConfig.WriteMessage =>
          val key = writeMessage.key
          val value = writeMessage.value
          dataMap.put(key, value)
          val sender = writeMessage.sender.asInstanceOf[ActorRef]
          sender ! new DictionaryConfig.ResultMessage(self, value)
        case readMessage: DictionaryConfig.ReadMessage =>
          val value = dataMap.get(readMessage.key)
          val sender = readMessage.sender.asInstanceOf[ActorRef]
          sender ! new DictionaryConfig.ResultMessage(self, value)
        case _: DictionaryConfig.EndWorkMessage =>
          completion.success(dataMap.size)
          exit()
        case _ =>
          System.err.println("Unsupported message: " + msg)
      }
    }
  }
}
