package edu.rice.habanero.benchmarks.count

import akka.actor.{ActorRef, Props}
import akka.actor.ActorSystem
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CountingAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CountingAkkaActorBenchmark)
  }

  private final class CountingAkkaActorBenchmark extends Benchmark {
    var system: ActorSystem = null
    
    def initialize(args: Array[String]) {
      CountingConfig.parseArgs(args)
    }

    def printArgInfo() {
      CountingConfig.printArgs()
    }

    def runIteration() : Future[Boolean] = {
      system = AkkaActorState.newActorSystem("Counting")
      val p = Promise[Boolean]

      val counter  = system.actorOf(Props(new CountingActor()))
      val producer = system.actorOf(Props(new ProducerActor(p, counter)))
      
      AkkaActorState.startActor(counter)
      AkkaActorState.startActor(producer)

      producer ! IncrementMessage()

      return p.future
    }
    
    override def runAndVerify() : Boolean = {
      import ExecutionContext.Implicits.global
      val f = runIteration()
      return Await.result(f, Duration.Inf)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private case class IncrementMessage()

  private case class RetrieveMessage(sender: ActorRef)

  private case class ResultMessage(result: Int)

  private class ProducerActor(completion: Promise[Boolean], counter: ActorRef) extends AkkaActor[AnyRef] {

    override def process(msg: AnyRef) {
      msg match {
        case m: IncrementMessage =>
          var i = 0
          while (i < CountingConfig.N) {
            counter ! m
            i += 1
          }
          counter ! RetrieveMessage(self)

        case m: ResultMessage =>
          val result = m.result
          if (result != CountingConfig.N) {
            println("ERROR: expected: " + CountingConfig.N + ", found: " + result)
          }
          completion.success(result == CountingConfig.N)
          exit()
      }
    }
  }

  private class CountingActor extends AkkaActor[AnyRef] {

    private var count = 0

    override def process(msg: AnyRef) {
      msg match {
        case m: IncrementMessage =>
          count += 1
        case m: RetrieveMessage =>
          m.sender ! ResultMessage(count)
          exit()
      }
    }
  }

}
