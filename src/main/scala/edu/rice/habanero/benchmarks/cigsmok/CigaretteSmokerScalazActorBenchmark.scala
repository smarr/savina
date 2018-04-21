package edu.rice.habanero.benchmarks.cigsmok

import som.Random

import edu.rice.habanero.actors.{ScalazActor, ScalazActorState, ScalazPool}
import edu.rice.habanero.benchmarks.cigsmok.CigaretteSmokerConfig.{ExitMessage, StartMessage, StartSmoking, StartedSmoking}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.Promise
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Based on solution in <a href="http://en.wikipedia.org/wiki/Cigarette_smokers_problem">Wikipedia</a> where resources are acquired instantaneously.
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CigaretteSmokerScalazActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CigaretteSmokerScalazActorBenchmark)
  }

  private final class CigaretteSmokerScalazActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      CigaretteSmokerConfig.parseArgs(args)
    }

    def printArgInfo() {
      CigaretteSmokerConfig.printArgs()
    }

    def runIteration() : Future[Long] = {
      val p = Promise[Long]

      val arbiterActor = new ArbiterActor(p, CigaretteSmokerConfig.R, CigaretteSmokerConfig.S)
      arbiterActor.start()

      arbiterActor.send(StartMessage.ONLY)

      p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val r = Await.result(f, Duration.Inf) 
      val valid = CigaretteSmokerConfig.verify(r)
      return valid
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      ScalazActorState.awaitTermination()
      
      if (lastIteration) {
        ScalazPool.shutdown()
      }
    }
  }

  private class ArbiterActor(completion: Promise[Long], numRounds: Int, numSmokers: Int) extends ScalazActor[AnyRef] {

    private val self = this
    private val smokerActors = Array.tabulate[ScalazActor[AnyRef]](numSmokers)(i => new SmokerActor(self))
    private val random = new Random()
    private var roundsSoFar = 0
    private var smokersExited = numSmokers

    override def onPostStart() {
      smokerActors.foreach(loopActor => {
        loopActor.start()
      })
    }

    override def process(msg: AnyRef) {
      msg match {
        case sm: StartMessage =>

          // choose a random smoker to start smoking
          notifyRandomSmoker()

        case sm: StartedSmoking =>

          // resources are off the table, can place new ones on the table
          roundsSoFar += 1
          if (roundsSoFar >= numRounds) {
            // had enough, now exit
            requestSmokersToExit()
            completion.success(random.next())
            exit()
          } else {
            // choose a random smoker to start smoking
            notifyRandomSmoker()
          }
        
        case sm: ExitMessage =>
          smokersExited -= 1
          if (smokersExited == 0) {
            completion.success(random.next())
            exit()
          }
      }
    }

    private def notifyRandomSmoker() {
      // assume resources grabbed instantaneously
      val newSmokerIndex = Math.abs(random.next()) % numSmokers
      val busyWaitPeriod = random.next(1000) + 10
      smokerActors(newSmokerIndex).send(new StartSmoking(busyWaitPeriod))
    }

    private def requestSmokersToExit() {
      smokerActors.foreach(loopActor => {
        loopActor.send(ExitMessage.ONLY)
      })
    }
  }

  private class SmokerActor(arbiterActor: ArbiterActor) extends ScalazActor[AnyRef] {
    private val random = new Random()
    
    override def process(msg: AnyRef) {
      msg match {
        case sm: StartSmoking =>

          // notify arbiter that started smoking
          arbiterActor.send(StartedSmoking.ONLY)
          // now smoke cigarette
          CigaretteSmokerConfig.busyWait(random, sm.busyWaitPeriod)

        case em: ExitMessage =>
          arbiterActor.send(ExitMessage.ONLY)
          exit()
      }
    }
  }

}
