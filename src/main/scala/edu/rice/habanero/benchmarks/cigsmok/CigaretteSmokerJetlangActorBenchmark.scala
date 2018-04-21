package edu.rice.habanero.benchmarks.cigsmok

import som.Random

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.cigsmok.CigaretteSmokerConfig.{ExitMessage, StartMessage, StartSmoking, StartedSmoking}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.concurrent.Await

/**
 * Based on solution in <a href="http://en.wikipedia.org/wiki/Cigarette_smokers_problem">Wikipedia</a> where resources are acquired instantaneously.
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CigaretteSmokerJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CigaretteSmokerJetlangActorBenchmark)
  }

  private final class CigaretteSmokerJetlangActorBenchmark extends Benchmark {
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
      JetlangActorState.awaitTermination()
      
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private class ArbiterActor(completion: Promise[Long], numRounds: Int, numSmokers: Int) extends JetlangActor[AnyRef] {

    private val self = this
    private val smokerActors = Array.tabulate[JetlangActor[AnyRef]](numSmokers)(i => new SmokerActor(self))
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

  private class SmokerActor(arbiterActor: ArbiterActor) extends JetlangActor[AnyRef] {
    private val random = new Random()
    
    override def process(msg: AnyRef) {
      msg match {
        case sm: StartSmoking =>

          // notify arbiter that started smoking
          arbiterActor.send(StartedSmoking.ONLY)
          // now smoke cigarette
          CigaretteSmokerConfig.busyWait(random, sm.busyWaitPeriod)

        case em: ExitMessage =>
          arbiterActor ! ExitMessage.ONLY
          exit()
      }
    }
  }

}
