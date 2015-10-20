package edu.rice.habanero.benchmarks.threadring

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.threadring.ThreadRingConfig.{DataMessage, ExitMessage, PingMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThreadRingJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThreadRingJetlangActorBenchmark)
  }

  private final class ThreadRingJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ThreadRingConfig.parseArgs(args)
    }

    def printArgInfo() {
      ThreadRingConfig.printArgs()
    }

    def runIteration() : Future[Int] = {
      import ExecutionContext.Implicits.global
      val promise = Promise[Int]

      val numActorsInRing = ThreadRingConfig.N
      val ringActors = Array.tabulate[JetlangActor[AnyRef]](numActorsInRing)(i => {
        val loopActor = new ThreadRingActor(i, numActorsInRing, promise)
        loopActor.start()
        loopActor
      })

      for ((loopActor, i) <- ringActors.view.zipWithIndex) {
        val nextActor = ringActors((i + 1) % numActorsInRing)
        loopActor.send(new DataMessage(nextActor))
      }

      ringActors(0).send(new PingMessage(ThreadRingConfig.R))

      return promise.future
    }

    override def runAndVerify(): Boolean = {
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      return n == 0
    }
    
    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      JetlangActorState.awaitTermination()
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private class ThreadRingActor(id: Int, numActorsInRing: Int,
      completion: Promise[Int]) extends JetlangActor[AnyRef] {

    private var nextActor: JetlangActor[AnyRef] = null

    override def process(msg: AnyRef) {

      msg match {

        case pm: PingMessage =>
          if (pm.hasNext) {
            nextActor.send(pm.next())
          } else {
            nextActor.send(new ExitMessage(numActorsInRing - 1))
          }

        case em: ExitMessage =>
          if (em.hasNext) {
            nextActor.send(em.next())
          } else {
            completion.success(id)
          }
          exit()

        case dm: DataMessage =>
          nextActor = dm.data.asInstanceOf[JetlangActor[AnyRef]]
      }
    }
  }
}
