package edu.rice.habanero.benchmarks.cigsmok

import som.Random

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.cigsmok.CigaretteSmokerConfig.{ExitMessage, StartMessage, StartSmoking, StartedSmoking}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.Promise
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Based on solution in <a href="http://en.wikipedia.org/wiki/Cigarette_smokers_problem">Wikipedia</a> where resources are acquired instantaneously.
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CigaretteSmokerAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CigaretteSmokerAkkaActorBenchmark)
  }

  private final class CigaretteSmokerAkkaActorBenchmark extends Benchmark {
    var system: ActorSystem = null
    
    def initialize(args: Array[String]) {
      CigaretteSmokerConfig.parseArgs(args)
    }

    def printArgInfo() {
      CigaretteSmokerConfig.printArgs()
    }

    def runIteration() : Future[Long] = {
      val p = Promise[Long]
      
      system = AkkaActorState.newActorSystem("CigaretteSmoker")

      val arbiterActor = system.actorOf(Props(new ArbiterActor(p, CigaretteSmokerConfig.R, CigaretteSmokerConfig.S)))
      AkkaActorState.startActor(arbiterActor)

      arbiterActor ! StartMessage.ONLY

      p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val r = Await.result(f, Duration.Inf) 
      val valid = CigaretteSmokerConfig.verify(r)
      return valid
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private class ArbiterActor(completion: Promise[Long], numRounds: Int, numSmokers: Int) extends AkkaActor[AnyRef] {

    private val smokerActors = Array.tabulate[ActorRef](numSmokers)(i =>
      context.system.actorOf(Props(new SmokerActor(self))))
    private val random = new Random()
    private var roundsSoFar = 0
    private var smokersExited = numSmokers

    override def onPostStart() {
      smokerActors.foreach(loopActor => {
        AkkaActorState.startActor(loopActor)
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
      smokerActors(newSmokerIndex) ! new StartSmoking(busyWaitPeriod)
    }

    private def requestSmokersToExit() {
      smokerActors.foreach(loopActor => {
        loopActor ! ExitMessage.ONLY
      })
    }
  }

  private class SmokerActor(arbiterActor: ActorRef) extends AkkaActor[AnyRef] {
    private val random = new Random()
    
    override def process(msg: AnyRef) {
      msg match {
        case sm: StartSmoking =>

          // notify arbiter that started smoking
          arbiterActor ! StartedSmoking.ONLY
          // now smoke cigarette
          CigaretteSmokerConfig.busyWait(random, sm.busyWaitPeriod)

        case em: ExitMessage =>
          arbiterActor ! ExitMessage.ONLY
          exit()
      }
    }
  }

}
