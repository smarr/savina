package edu.rice.habanero.benchmarks.logmap

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.logmap.LogisticMapConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.Promise

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object LogisticMapAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new LogisticMapAkkaActorBenchmark)
  }

  private final class LogisticMapAkkaActorBenchmark extends Benchmark {
    var system: ActorSystem = null
    def initialize(args: Array[String]) {
      LogisticMapConfig.parseArgs(args)
    }

    def printArgInfo() {
      LogisticMapConfig.printArgs()
    }

    def runIteration() : Future[Double] = {
      system = AkkaActorState.newActorSystem("LogisticMap")
      val p = Promise[Double]

      val master = system.actorOf(Props(new Master(p)))
      AkkaActorState.startActor(master)
      master ! StartMessage.ONLY

      p.future
    }

    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val r = Await.result(f, Duration.Inf)
      return LogisticMapConfig.verify(r);
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private class Master(completion: Promise[Double]) extends AkkaActor[AnyRef] {

    private final val numComputers: Int = LogisticMapConfig.numSeries
    private final val computers = Array.tabulate[ActorRef](numComputers)(i => {
      val rate = LogisticMapConfig.startRate + (i * LogisticMapConfig.increment)
      context.system.actorOf(Props(new RateComputer(rate)))
    })

    private final val numWorkers: Int = LogisticMapConfig.numSeries
    private final val workers = Array.tabulate[ActorRef](numWorkers)(i => {
      val rateComputer = computers(i % numComputers)
      val startTerm = i * LogisticMapConfig.increment
      context.system.actorOf(Props(new SeriesWorker(i, self, rateComputer, startTerm)))
    })

    private var numWorkRequested: Int = 0
    private var numWorkReceived: Int = 0
    private var termsSum: Double = 0

    protected override def onPostStart() {
      computers.foreach(loopComputer => {
        AkkaActorState.startActor(loopComputer)
      })
      workers.foreach(loopWorker => {
        AkkaActorState.startActor(loopWorker)
      })
    }

    override def process(theMsg: AnyRef) {
      theMsg match {
        case _: StartMessage =>

          var i: Int = 0
          while (i < LogisticMapConfig.numTerms) {
            // request each worker to compute the next term
            workers.foreach(loopWorker => {
              loopWorker ! NextTermMessage.ONLY
            })
            i += 1
          }

          // workers should stop after all items have been computed
          workers.foreach(loopWorker => {
            loopWorker ! GetTermMessage.ONLY
            numWorkRequested += 1
          })

        case rm: ResultMessage =>

          termsSum += rm.term
          numWorkReceived += 1

          if (numWorkRequested == numWorkReceived) {
            completion.success(termsSum)

            computers.foreach(loopComputer => {
              loopComputer ! StopMessage.ONLY
            })
            workers.foreach(loopWorker => {
              loopWorker ! StopMessage.ONLY
            })
            exit()
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class SeriesWorker(id: Int, master: ActorRef, computer: ActorRef, startTerm: Double) extends AkkaActor[AnyRef] {

    private var curTerm = startTerm
    private var waitingForReply = false
    private var waitingNextTerm = 0
    private var waitingGetTerm  = false

    override def process(theMsg: AnyRef) {
      theMsg match {
        case computeMessage: NextTermMessage =>
          if (waitingForReply) {
            waitingNextTerm += 1
          } else {
            val sender = self
            val newMessage = new ComputeMessage(sender, curTerm)
            computer ! newMessage
            waitingForReply = true
          }

        case rm: ResultMessage =>
          if (waitingNextTerm > 0) {
            waitingNextTerm -= 1
            
            val sender = self
            val newMessage = new ComputeMessage(sender, rm.term)
            computer ! newMessage 
          } else {
            curTerm = rm.term
            waitingForReply = false
            if (waitingGetTerm) {
              master ! new ResultMessage(rm.term)
            }
          }

        case _: GetTermMessage =>
          if (waitingForReply) {
            waitingGetTerm = true
          } else {
            master ! new ResultMessage(curTerm)
          }

        case _: StopMessage =>
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class RateComputer(rate: Double) extends AkkaActor[AnyRef] {

    override def process(theMsg: AnyRef) {
      theMsg match {
        case computeMessage: ComputeMessage =>
          val result = computeNextTerm(computeMessage.term, rate)
          val resultMessage = new ResultMessage(result)
          sender() ! resultMessage

        case _: StopMessage =>
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
