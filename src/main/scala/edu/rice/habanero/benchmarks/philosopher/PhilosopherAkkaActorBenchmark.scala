package edu.rice.habanero.benchmarks.philosopher

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.Promise
import scala.concurrent.Future
import akka.actor.ActorSystem
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object PhilosopherAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PhilosopherAkkaActorBenchmark)
  }
  
  private final class PhilosopherAkkaActorBenchmark extends Benchmark {
    var system: ActorSystem = null
    
    def initialize(args: Array[String]) {
      PhilosopherConfig.parseArgs(args)
    }

    def printArgInfo() {
      PhilosopherConfig.printArgs()
    }

    def runIteration() : Future[Integer] = {
      system = AkkaActorState.newActorSystem("Philosopher")
      val p = Promise[Integer]

      val arbitrator = system.actorOf(Props(new ArbitratorActor(p, PhilosopherConfig.N)))
      AkkaActorState.startActor(arbitrator)

      val philosophers = Array.tabulate[ActorRef](PhilosopherConfig.N)(i => {
        val loopActor = system.actorOf(Props(new PhilosopherActor(i, PhilosopherConfig.M, arbitrator)))
        AkkaActorState.startActor(loopActor)
        loopActor
      })

      philosophers.foreach(loopActor => {
        loopActor ! StartMessage()
      })

      p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val r = Await.result(f, Duration.Inf)
      return r == 0
    }
 
    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  case class StartMessage()

  case class ExitMessage()

  case class HungryMessage(philosopher: ActorRef, leftForkId: Int)

  case class DoneMessage(leftForkId: Int)

  case class EatMessage()

  case class DeniedMessage()
  
  private class PhilosopherActor(id: Int, rounds: Int, arbitrator: ActorRef) extends AkkaActor[AnyRef] {
    private var localCounter = 0L
    private var roundsSoFar = 0

    private val myHungryMessage = HungryMessage(self, id)
    private val myDoneMessage = DoneMessage(id)

    override def process(msg: AnyRef) {
      msg match {
        case dm: DeniedMessage =>
          localCounter += 1
          arbitrator ! myHungryMessage

        case em: EatMessage =>
          roundsSoFar += 1
          if (localCounter > 0) {
            localCounter = 0L
          }

          arbitrator ! myDoneMessage
          if (roundsSoFar < rounds) {
            self ! StartMessage()
          } else {
            arbitrator ! ExitMessage()
            exit()
          }

        case sm: StartMessage =>
          arbitrator ! myHungryMessage
      }
    }
  }

  private class ArbitratorActor(completion: Promise[Integer], numForks: Int) extends AkkaActor[AnyRef] {

    private val forks = Array.fill[Boolean](numForks)(false)
    private var numExitedPhilosophers = 0

    override def process(msg: AnyRef) {
      msg match {
        case hm: HungryMessage =>
          val rightForkId = (hm.leftForkId + 1) % numForks

          if (forks(hm.leftForkId) || forks(rightForkId)) {
            // someone else has access to the fork
            hm.philosopher ! DeniedMessage()
          } else {
            forks(hm.leftForkId) = true
            forks(rightForkId) = true
            hm.philosopher ! EatMessage()
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
