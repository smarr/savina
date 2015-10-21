package edu.rice.habanero.benchmarks.radixsort

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration
import som.Random

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object RadixSortJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new RadixSortJetlangActorBenchmark)
  }

  private final class RadixSortJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      RadixSortConfig.parseArgs(args)
    }

    def printArgInfo() {
      RadixSortConfig.printArgs()
    }

    def runIteration() : Future[Long] = {
      val p = Promise[Long]

      val validationActor = new ValidationActor(p, RadixSortConfig.N)
      validationActor.start()

      val sourceActor = new IntSourceActor(
    		  RadixSortConfig.N, RadixSortConfig.M, RadixSortConfig.S)
      sourceActor.start()

      var radix = RadixSortConfig.M / 2
      var nextActor: JetlangActor[AnyRef] = validationActor

      while (radix > 0) {
        val sortActor = new SortActor(RadixSortConfig.N, radix, nextActor)
        sortActor.start()

        radix /= 2
        nextActor = sortActor
      }

      sourceActor.send(new NextActorMessage(nextActor))

      return p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      return RadixSortConfig.verifyResult(n)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      JetlangActorState.awaitTermination()

      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private class NextActorMessage(actor: JetlangActor[AnyRef]) {
	  def getActor() : JetlangActor[AnyRef] = {
      return actor
    }
  }

  private class ValueMessage(value: Int) {
	def getValue() : Int = {
      return value
    }
  }

  private class IntSourceActor(numValues: Int, maxValue: Int, seed: Int)
  		extends JetlangActor[AnyRef] {

    val random = new Random(seed)

    override def process(msg: AnyRef) {

      msg match {
        case nm: NextActorMessage =>

          var i = 0
          while (i < numValues) {
            val candidate = Math.abs(random.next()) % maxValue
            val message = new ValueMessage(candidate)
            nm.getActor().send(message)

            i += 1
          }
          exit()
      }
    }
  }

  private class SortActor(numValues: Int, radix: Long, nextActor: JetlangActor[AnyRef])
  	  extends JetlangActor[AnyRef] {

    private val orderingArray = Array.ofDim[Int](numValues)
    private var valuesSoFar = 0
    private var j = 0

    override def process(msg: AnyRef): Unit = {
      msg match {
        case vm: ValueMessage =>
          valuesSoFar += 1
          val current = vm.getValue()
          if ((current & radix) == 0) {
            nextActor.send(vm)
          } else {
            orderingArray(j) = current
            j += 1
          }

          if (valuesSoFar == numValues) {

            var i = 0
            while (i < j) {
              nextActor.send(new ValueMessage(orderingArray(i)))
              i += 1
            }

            exit()
          }
      }
    }
  }

  private class ValidationActor(completion: Promise[Long], numValues: Int)
  		extends JetlangActor[AnyRef] {

    private var sumSoFar = 0L
    private var valuesSoFar = 0
    private var prevValue = 0
    private var errorValue = (-1, -1)

    override def process(msg: AnyRef) {

      msg match {
        case vm: ValueMessage =>

          valuesSoFar += 1

          if (vm.getValue() < prevValue && errorValue._1 < 0) {
            errorValue = (vm.getValue(), valuesSoFar - 1)
          }
          prevValue = vm.getValue()
          sumSoFar += prevValue

          if (valuesSoFar == numValues) {
            if (errorValue._1 >= 0) {
              println("ERROR: Value out of place: " + errorValue._1 + " at index " + errorValue._2)
            }
            completion.success(sumSoFar)
            exit()
          }
      }
    }
  }
}
