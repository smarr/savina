package edu.rice.habanero.benchmarks.philosopher

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import edu.rice.habanero.actors.{ScalazActor, ScalazActorState, ScalazPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.Promise
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.Await

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object PhilosopherScalazActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PhilosopherScalazActorBenchmark)
  }

  private final class PhilosopherScalazActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PhilosopherConfig.parseArgs(args)
    }

    def printArgInfo() {
      PhilosopherConfig.printArgs()
    }

    def runIteration() : Future[Integer] = {
      val p = Promise[Integer]

      val arbitrator = new ArbitratorActor(p, PhilosopherConfig.N)
      arbitrator.start()

      val philosophers = Array.tabulate[ScalazActor[AnyRef]](PhilosopherConfig.N)(i => {
        val loopActor = new PhilosopherActor(i, PhilosopherConfig.M, arbitrator)
        loopActor.start()
        loopActor
      })

      philosophers.foreach(loopActor => {
        loopActor.send(StartMessage())
      })

      p.future
    }

    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val r = Await.result(f, Duration.Inf)
      return r == 0
    }
    
    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      ScalazActorState.awaitTermination()
      
      if (lastIteration) {
        ScalazPool.shutdown()
      }
    }
  }


  case class StartMessage()

  case class ExitMessage()

  case class HungryMessage(philosopher: ScalazActor[AnyRef], leftForkId: Int)

  case class DoneMessage(leftForkId: Int)

  case class EatMessage()

  case class DeniedMessage()


  private class PhilosopherActor(id: Int, rounds: Int, arbitrator: ArbitratorActor) extends ScalazActor[AnyRef] {

    private val self = this
    private var localCounter = 0L
    private var roundsSoFar = 0

    private val myHungryMessage = HungryMessage(self, id)
    private val myDoneMessage = DoneMessage(id)

    override def process(msg: AnyRef) {
      msg match {
        case dm: DeniedMessage =>
          localCounter += 1
          arbitrator.send(myHungryMessage)

        case em: EatMessage =>
          roundsSoFar += 1
          if (localCounter > 0) {
            localCounter = 0L
          }

          arbitrator.send(myDoneMessage)
          if (roundsSoFar < rounds) {
            self.send(StartMessage())
          } else {
            arbitrator.send(ExitMessage())
            exit()
          }

        case sm: StartMessage =>
          arbitrator.send(myHungryMessage)
      }
    }
  }

  private class ArbitratorActor(completion: Promise[Integer], numForks: Int) extends ScalazActor[AnyRef] {

    private val forks = Array.fill[Boolean](numForks)(false)
    private var numExitedPhilosophers = 0

    override def process(msg: AnyRef) {
      msg match {
        case hm: HungryMessage =>
          val rightForkId = (hm.leftForkId + 1) % numForks

          if (forks(hm.leftForkId) || forks(rightForkId)) {
            // someone else has access to the fork
            hm.philosopher.send(DeniedMessage())
          } else {
            forks(hm.leftForkId) = true
            forks(rightForkId) = true
            hm.philosopher.send(EatMessage())
          }

        case dm: DoneMessage =>
          val rightForkId = (dm.leftForkId + 1) % numForks
          
          forks(dm.leftForkId) = false
          forks(rightForkId) = false

        case em: ExitMessage =>
          numExitedPhilosophers += 1
          
          if (numForks == numExitedPhilosophers) {
            var forksTaken = 0
            forks.foreach {f =>  if (f) forksTaken += 1}
            completion.success(forksTaken)
            exit()
          }
      }
    }
  }

}
