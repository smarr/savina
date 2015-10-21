package edu.rice.habanero.benchmarks.trapezoid

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.trapezoid.TrapezoidalConfig.{ResultMessage, WorkMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration
import akka.actor.ActorSystem

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object TrapezoidalAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new TrapezoidalAkkaActorBenchmark)
  }

  private final class TrapezoidalAkkaActorBenchmark extends Benchmark {
    var system: ActorSystem = null
    
    def initialize(args: Array[String]) {
      TrapezoidalConfig.parseArgs(args)
    }

    def printArgInfo() {
      TrapezoidalConfig.printArgs()
    }

    def runIteration() : Future[Double] = {
      system = AkkaActorState.newActorSystem("Trapezoidal")
      val p = Promise[Double]

      val numWorkers: Int = TrapezoidalConfig.W
      val precision: Double = (TrapezoidalConfig.R - TrapezoidalConfig.L) / TrapezoidalConfig.N

      val master = system.actorOf(Props(new Master(p, numWorkers)))
      AkkaActorState.startActor(master)
      
      master ! new WorkMessage(TrapezoidalConfig.L, TrapezoidalConfig.R, precision)

      return p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      return TrapezoidalConfig.verifyResult(n)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private class Master(completion: Promise[Double], numWorkers: Int) extends AkkaActor[AnyRef] {

    private final val workers = Array.tabulate[ActorRef](numWorkers)(i =>
      context.system.actorOf(Props(new Worker(self, i))))
    private var numTermsReceived: Int = 0
    private var resultArea: Double = 0.0

    override def onPostStart() {
      workers.foreach(loopWorker => {
        AkkaActorState.startActor(loopWorker)
      })
    }

    override def process(msg: AnyRef) {
      msg match {

        case rm: ResultMessage =>
          numTermsReceived += 1
          resultArea += rm.result

          if (numTermsReceived == numWorkers) {
            completion.success(resultArea)
            exit()
          }

        case wm: WorkMessage =>
          val workerRange: Double = (wm.r - wm.l) / numWorkers
          for ((loopWorker, i) <- workers.view.zipWithIndex) {

            val wl = (workerRange * i) + wm.l
            val wr = wl + workerRange

            loopWorker ! new WorkMessage(wl, wr, wm.h)
          }

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class Worker(master: ActorRef, val id: Int) extends AkkaActor[AnyRef] {

    override def process(msg: AnyRef) {
      msg match {
        case wm: WorkMessage =>
          val n = ((wm.r - wm.l) / wm.h).asInstanceOf[Int]
          var accumArea = 0.0

          var i = 0
          while (i < n) {
            val lx = (i * wm.h) + wm.l
            val rx = lx + wm.h

            val ly = TrapezoidalConfig.fx(lx)
            val ry = TrapezoidalConfig.fx(rx)

            val area = 0.5 * (ly + ry) * wm.h
            accumArea += area

            i += 1
          }
          master ! new ResultMessage(accumArea, id)
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }
}
