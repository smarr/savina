package edu.rice.habanero.benchmarks.big

import som.Random

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.big.BigConfig.{ExitMessage, Message, PingMessage, PongMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BigJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BigJetlangActorBenchmark)
  }

  private final class BigJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      BigConfig.parseArgs(args)
    }

    def printArgInfo() {
      BigConfig.printArgs()
    }

    def runIteration() : Future[Integer] = {
      val p = Promise[Integer]

      val sinkActor = new SinkActor(p, BigConfig.W)
      sinkActor.start()

      val bigActors = Array.tabulate[JetlangActor[AnyRef]](BigConfig.W)(i => {
        val loopActor = new BigActor(i, BigConfig.N, sinkActor)
        loopActor.start()
        loopActor
      })

      val neighborMessage = new NeighborMessage(bigActors)
      sinkActor.send(neighborMessage)
      bigActors.foreach(loopActor => {
        loopActor.send(neighborMessage)
      })

      bigActors.foreach(loopActor => {
        loopActor.send(new PongMessage(-1))
      })

      return p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      return Await.result(f, Duration.Inf) == BigConfig.W
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      JetlangActorState.awaitTermination()
      
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private case class NeighborMessage(neighbors: Array[JetlangActor[AnyRef]]) extends Message

  private class BigActor(id: Int, numMessages: Int, sinkActor: JetlangActor[AnyRef]) extends JetlangActor[AnyRef] {

    private var numPings = 0
    private var expPinger = -1
    private val random = new Random(id)
    private var neighbors: Array[JetlangActor[AnyRef]] = null

    private val myPingMessage = new PingMessage(id)
    private val myPongMessage = new PongMessage(id)

    override def process(msg: AnyRef) {
      msg match {
        case pm: PingMessage =>

          val sender = neighbors(pm.sender)
          sender.send(myPongMessage)

        case pm: PongMessage =>

          if (pm.sender != expPinger) {
            println("ERROR: Expected: " + expPinger + ", but received ping from " + pm.sender)
          }
          if (numPings == numMessages) {
            sinkActor.send(ExitMessage.ONLY)
          } else {
            sendPing()
            numPings += 1
          }

        case em: ExitMessage =>
          exit()

        case nm: NeighborMessage =>

          neighbors = nm.neighbors
      }
    }

    private def sendPing(): Unit = {
      val target = random.next(neighbors.size)
      val targetActor = neighbors(target)

      expPinger = target
      targetActor.send(myPingMessage)
    }
  }

  private class SinkActor(completion: Promise[Integer], numWorkers: Int) extends JetlangActor[AnyRef] {

    private var numMessages = 0
    private var neighbors: Array[JetlangActor[AnyRef]] = null

    override def process(msg: AnyRef) {
      msg match {
        case em: ExitMessage =>

          numMessages += 1
          if (numMessages == numWorkers) {
            neighbors.foreach(loopWorker => loopWorker.send(ExitMessage.ONLY))
            completion.success(numMessages)
            exit()
          }

        case nm: NeighborMessage =>
          neighbors = nm.neighbors
      }
    }
  }

}
