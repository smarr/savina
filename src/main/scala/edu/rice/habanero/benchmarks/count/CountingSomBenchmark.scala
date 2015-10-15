package edu.rice.habanero.benchmarks.count

import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import som.actors.Promise
import som.actors.EventualMessage.DirectMessage
import som.actors.Actor
import som.actors.EventualMessage
import som.actors.Promise.Resolver
import som.actors.Promise.ResolutionAction

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CountingSomBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CountingSomActorBenchmark)
  }

  private final class CountingSomActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      CountingConfig.parseArgs(args)
    }

    def printArgInfo() {
      CountingConfig.printArgs()
    }

    def runIteration() : Promise = {
      val p = new Promise(null)

      val counter  = new CountingActor()
      val producer = new ProducerActor(new Resolver(p), counter)

      (new IncrementMessage(producer)).send()

      return p
    }
    
    override def runAndVerify() : Boolean = {
      val p = runIteration();
      
      class Complete extends ResolutionAction {
        def action(result: Object) {
          p.notify()
        }
      }
      
      p.whenResolved(new Complete)
      return p.await().asInstanceOf[Boolean];
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      Actor.awaitQuiescence()
    }
  }

  private class IncrementMessage(target: Actor) extends DirectMessage(target) { } 

  private class RetrieveMessage(target: Actor, sender: Actor) extends DirectMessage(target) {
    def getSender() : Actor = {
      return sender
    }
  } 

  private class ResultMessage(target: Actor, result: Int) extends DirectMessage(target) {
    def getResult() : Int = {
      return result
    }
  }

  private class ProducerActor(resolver: Resolver, counter: Actor) extends Actor {

    override def receive(msg: EventualMessage) {
      msg match {
        case _: IncrementMessage =>
          var i = 0
          while (i < CountingConfig.N) {
            (new IncrementMessage(counter)).send()
            i += 1
          }

          (new RetrieveMessage(counter, this)).send()

        case m: ResultMessage =>
          val result = m.getResult()
          if (result != CountingConfig.N) {
            println("ERROR: expected: " + CountingConfig.N + ", found: " + result)
          }
          resolver.resolve(result == CountingConfig.N)
      }
    }
  }

  private class CountingActor extends Actor {
    private var count = 0

    override def receive(msg: EventualMessage) {
      msg match {
        case m: IncrementMessage =>
          count += 1
        case m: RetrieveMessage =>
          (new ResultMessage(m.getSender(), count)).send()
      }
    }
  }
}
