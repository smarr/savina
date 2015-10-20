package edu.rice.habanero.benchmarks.chameneos

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ChameneosJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ChameneosJetlangActorBenchmark)
  }

  private final class ChameneosJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ChameneosConfig.parseArgs(args)
    }

    def printArgInfo() {
      ChameneosConfig.printArgs()
    }

    def runIteration() : Future[Int] = {
      val p = Promise[Int]
      
      val mallActor = new ChameneosMallActor(p,
        ChameneosConfig.numMeetings, ChameneosConfig.numChameneos)
      mallActor.start()

      return p.future 
    }
    
    override def runAndVerify() : Boolean = {
      import ExecutionContext.Implicits.global
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      return n == 2 * ChameneosConfig.numMeetings
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      JetlangActorState.awaitTermination()
      
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private class ChameneosMallActor(completion: Promise[Int], var n: Int, numChameneos: Int) extends JetlangActor[ChameneosHelper.Message] {

    private final val self: JetlangActor[ChameneosHelper.Message] = this
    private var waitingChameneo: JetlangActor[ChameneosHelper.Message] = null
    private var sumMeetings: Int = 0
    private var numFaded: Int = 0

    protected override def onPostStart() {
      startChameneos()
    }

    private def startChameneos() {
      Array.tabulate[ChameneosChameneoActor](numChameneos)(i => {
        val color = ChameneosHelper.Color.values()(i % 3)
        val loopChamenos = new ChameneosChameneoActor(self, color, i)
        loopChamenos.start()
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
            val sender = message.sender.asInstanceOf[JetlangActor[ChameneosHelper.Message]]
            if (waitingChameneo == null) {
              waitingChameneo = sender
            }
            else {
              n = n - 1
              waitingChameneo.send(new ChameneosHelper.MeetMsg(message.color, sender))
              waitingChameneo = null
            }
          }
          else {
            val sender = message.sender.asInstanceOf[JetlangActor[ChameneosHelper.Message]]
            sender.send(new ChameneosHelper.ExitMsg(self))
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class ChameneosChameneoActor(mall: JetlangActor[ChameneosHelper.Message], var color: ChameneosHelper.Color, id: Int)
    extends JetlangActor[ChameneosHelper.Message] {

    private var meetings: Int = 0

    protected override def onPostStart() {
      mall.send(new ChameneosHelper.MeetMsg(color, this))
    }

    override def process(msg: ChameneosHelper.Message) {
      msg match {
        case message: ChameneosHelper.MeetMsg =>
          val otherColor: ChameneosHelper.Color = message.color
          val sender = message.sender.asInstanceOf[JetlangActor[ChameneosHelper.Message]]
          color = ChameneosHelper.complement(color, otherColor)
          meetings = meetings + 1
          sender.send(new ChameneosHelper.ChangeMsg(color, this))
          mall.send(new ChameneosHelper.MeetMsg(color, this))
        case message: ChameneosHelper.ChangeMsg =>
          color = message.color
          meetings = meetings + 1
          mall.send(new ChameneosHelper.MeetMsg(color, this))
        case message: ChameneosHelper.ExitMsg =>
          val sender = message.sender.asInstanceOf[JetlangActor[ChameneosHelper.Message]]
          color = ChameneosHelper.fadedColor
          sender.send(new ChameneosHelper.MeetingCountMsg(meetings, this))
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
