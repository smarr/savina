package edu.rice.habanero.benchmarks.logmap

import edu.rice.habanero.actors.{ScalazActor, ScalazActorState, ScalazPool}
import edu.rice.habanero.benchmarks.logmap.LogisticMapConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.concurrent.Promise
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object LogisticMapScalazActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new LogisticMapScalazActorBenchmark)
  }

  private final class LogisticMapScalazActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      LogisticMapConfig.parseArgs(args)
    }

    def printArgInfo() {
      LogisticMapConfig.printArgs()
    }

    def runIteration() : Future[Double] = {
      val p = Promise[Double]

      val master = new Master(p)
      master.start()
      master.send(StartMessage.ONLY)

      p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val r = Await.result(f, Duration.Inf)
      return LogisticMapConfig.verify(r);
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      ScalazActorState.awaitTermination()
      
      if (lastIteration) {
        ScalazPool.shutdown()
      }
    }
  }

  private class Master(completion: Promise[Double]) extends ScalazActor[AnyRef] {

    private final val self = this

    private final val numComputers: Int = LogisticMapConfig.numSeries
    private final val computers = Array.tabulate[RateComputer](numComputers)(i => {
      val rate = LogisticMapConfig.startRate + (i * LogisticMapConfig.increment)
      new RateComputer(rate)
    })

    private final val numWorkers: Int = LogisticMapConfig.numSeries
    private final val workers = Array.tabulate[SeriesWorker](numWorkers)(i => {
      val rateComputer = computers(i % numComputers)
      val startTerm = i * LogisticMapConfig.increment
      new SeriesWorker(i, self, rateComputer, startTerm)
    })

    private var numWorkRequested: Int = 0
    private var numWorkReceived: Int = 0
    private var termsSum: Double = 0

    protected override def onPostStart() {
      computers.foreach(loopComputer => {
        loopComputer.start()
      })
      workers.foreach(loopWorker => {
        loopWorker.start()
      })
    }

    override def process(theMsg: AnyRef) {
      theMsg match {
        case _: StartMessage =>

          var i: Int = 0
          while (i < LogisticMapConfig.numTerms) {
            // request each worker to compute the next term
            workers.foreach(loopWorker => {
              loopWorker.send(NextTermMessage.ONLY)
            })
            i += 1
          }

          // workers should stop after all items have been computed
          workers.foreach(loopWorker => {
            loopWorker.send(GetTermMessage.ONLY)
            numWorkRequested += 1
          })

        case rm: ResultMessage =>

          termsSum += rm.term
          numWorkReceived += 1

          if (numWorkRequested == numWorkReceived) {
            completion.success(termsSum)

            computers.foreach(loopComputer => {
              loopComputer.send(StopMessage.ONLY)
            })
            workers.foreach(loopWorker => {
              loopWorker.send(StopMessage.ONLY)
            })
            exit()
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class SeriesWorker(id: Int, master: Master, computer: RateComputer, startTerm: Double) extends ScalazActor[AnyRef] {

    private final val self = this
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
            computer.send(newMessage)
            waitingForReply = true
          }

        case rm: ResultMessage =>
          if (waitingNextTerm > 0) {
            waitingNextTerm -= 1
            
            val sender = self
            val newMessage = new ComputeMessage(sender, rm.term)
            computer.send(newMessage) 
          } else {
            curTerm = rm.term
            waitingForReply = false
            if (waitingGetTerm) {
              master.send(new ResultMessage(rm.term))
            }
          }

        case _: GetTermMessage =>
          if (waitingForReply) {
            waitingGetTerm = true
          } else {
            master.send(new ResultMessage(curTerm))
          }

        case _: StopMessage =>
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class RateComputer(rate: Double) extends ScalazActor[AnyRef] {

    override def process(theMsg: AnyRef) {
      theMsg match {
        case computeMessage: ComputeMessage =>
          val result = computeNextTerm(computeMessage.term, rate)
          val resultMessage = new ResultMessage(result)
          val sender = computeMessage.sender.asInstanceOf[SeriesWorker]
          sender.send(resultMessage)

        case _: StopMessage =>
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
