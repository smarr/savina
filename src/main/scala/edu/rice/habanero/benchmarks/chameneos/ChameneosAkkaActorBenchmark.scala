package edu.rice.habanero.benchmarks.chameneos

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import akka.actor.ActorSystem
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ChameneosAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ChameneosAkkaActorBenchmark)
  }

  private final class ChameneosAkkaActorBenchmark extends Benchmark {
    var system : ActorSystem = null
    
    def initialize(args: Array[String]) {
      ChameneosConfig.parseArgs(args)
    }

    def printArgInfo() {
      ChameneosConfig.printArgs()
    }

    def runIteration() : Future[Int] = {
      system = AkkaActorState.newActorSystem("Chameneos")
      val p = Promise[Int]

      val mallActor = system.actorOf(Props(
        new ChameneosMallActor(p,
          ChameneosConfig.numMeetings, ChameneosConfig.numChameneos)))

      AkkaActorState.startActor(mallActor)

      return p.future
    }
    
    override def runAndVerify() : Boolean = {
      import ExecutionContext.Implicits.global
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      return n == 2 * ChameneosConfig.numMeetings
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  private class ChameneosMallActor(completion: Promise[Int], var n: Int,
      numChameneos: Int) extends AkkaActor[ChameneosHelper.Message] {

    private var waitingChameneo: ActorRef = null
    private var sumMeetings: Int = 0
    private var numFaded: Int = 0

    protected override def onPostStart() {
      startChameneos()
    }

    private def startChameneos() {
      Array.tabulate[ActorRef](numChameneos)(i => {
        val color = ChameneosHelper.Color.values()(i % 3)
        val loopChamenos = context.system.actorOf(Props(new ChameneosChameneoActor(self, color, i)))
        AkkaActorState.startActor(loopChamenos)
        loopChamenos
      })
    }

    override def process(msg: ChameneosHelper.Message) {
      msg match {
        case message: ChameneosHelper.MeetingCountMsg =>
          numFaded = numFaded + 1
          sumMeetings = sumMeetings + message.count
          if (numFaded == numChameneos) {
            completion.success(sumMeetings)
            exit()
          }
        case message: ChameneosHelper.MeetMsg =>
          if (n > 0) {
            val sender = message.sender.asInstanceOf[ActorRef]
            if (waitingChameneo == null) {
              waitingChameneo = sender
            }
            else {
              n = n - 1
              waitingChameneo ! new ChameneosHelper.MeetMsg(message.color, sender)
              waitingChameneo = null
            }
          }
          else {
            val sender = message.sender.asInstanceOf[ActorRef]
            sender ! new ChameneosHelper.ExitMsg(self)
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class ChameneosChameneoActor(mall: ActorRef, var color: ChameneosHelper.Color, id: Int)
    extends AkkaActor[ChameneosHelper.Message] {

    private var meetings: Int = 0

    protected override def onPostStart() {
      mall ! new ChameneosHelper.MeetMsg(color, self)
    }

    override def process(msg: ChameneosHelper.Message) {
      msg match {
        case message: ChameneosHelper.MeetMsg =>
          val otherColor: ChameneosHelper.Color = message.color
          val sender = message.sender.asInstanceOf[ActorRef]
          color = ChameneosHelper.complement(color, otherColor)
          meetings = meetings + 1
          sender ! new ChameneosHelper.ChangeMsg(color, self)
          mall ! new ChameneosHelper.MeetMsg(color, self)
        case message: ChameneosHelper.ChangeMsg =>
          color = message.color
          meetings = meetings + 1
          mall ! new ChameneosHelper.MeetMsg(color, self)
        case message: ChameneosHelper.ExitMsg =>
          val sender = message.sender.asInstanceOf[ActorRef]
          color = ChameneosHelper.fadedColor
          sender ! new ChameneosHelper.MeetingCountMsg(meetings, self)
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
