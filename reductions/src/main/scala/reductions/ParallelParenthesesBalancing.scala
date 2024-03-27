package reductions

import scala.annotation.*
import org.scalameter.*

object ParallelParenthesesBalancingRunner:

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns := 40,
    Key.exec.maxWarmupRuns := 80,
    Key.exec.benchRuns := 120,
    Key.verbose := false
  ) withWarmer(Warmer.Default())

  def main(args: Array[String]): Unit =
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface:

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def balanceFrom(i: Int, opened: Int, arr: Array[Char]): Boolean = {
      if (i == arr.length) opened == 0
      else arr(i) match {
        case '(' => balanceFrom(i + 1, opened + 1, arr)
        case ')' => if (opened == 0) false else balanceFrom(i + 1, opened - 1, arr)
        case other => balanceFrom(i + 1, opened, arr)
      }
    }

    balanceFrom(0, 0, chars)
  }
  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {
    def traverse(idx: Int, until: Int, closingNum: Int, openingNum: Int): (Int, Int) = {
      var closingsOverflow = 0
      var openings = 0

      var i = idx
      while (i < until) {
        chars(i) match {
          case '(' => openings = openings + 1
          case ')' =>
            if (openings > 0) openings = openings - 1
            else closingsOverflow = closingsOverflow + 1
          case other =>
        }
        i = i + 1
      }

      (closingsOverflow, openings)
    }
    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val mid = from + (until - from) / 2

        val ((x1, x2), (y1, y2)) = parallel(reduce(from, mid), reduce(mid, until))

        if (x2 > y1) (x1, x2 - y1 + y2)
        else (x1 + y1 - x2, y2)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!