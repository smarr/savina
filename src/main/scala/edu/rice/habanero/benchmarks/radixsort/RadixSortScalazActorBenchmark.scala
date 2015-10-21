package edu.rice.habanero.benchmarks.radixsort

import edu.rice.habanero.actors.{ScalazActor, ScalazActorState, ScalazPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration
import som.Random

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object RadixSortScalazActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new RadixSortScalazActorBenchmark)
  }

  private final class RadixSortScalazActorBenchmark extends Benchmark {
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
      var nextActor: ScalazActor[AnyRef] = validationActor

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
      ScalazActorState.awaitTermination()

      if (lastIteration) {
        ScalazPool.shutdown()
      }
    }
  }

  private class NextActorMessage(actor: ScalazActor[AnyRef]) {
	def getActor() : ScalazActor[AnyRef] = {
	  return actor
	}
  }

  private class ValueMessage(value: Int) {
	def getValue() : Int = {
      return value
    }
  }

  private class IntSourceActor(numValues: Int, maxValue: Int, seed: Int)
      extends ScalazActor[AnyRef] {

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

  private class SortActor(numValues: Int, radix: Long, nextActor: ScalazActor[AnyRef])
      extends ScalazActor[AnyRef] {

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
      extends ScalazActor[AnyRef] {

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
