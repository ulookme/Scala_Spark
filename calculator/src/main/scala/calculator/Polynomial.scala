package calculator

object Polynomial extends PolynomialInterface:
  import scala.math.sqrt

  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] =
    Signal {
      val aValue = a()
      val bValue = b()
      val cValue = c()
      bValue * bValue - 4 * aValue * cValue
    }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] =
    Signal {
      val deltaValue = delta()
      val aValue = a()
      val bValue = b()
      val cValue = c()

      if (deltaValue < 0) Set.empty[Double]
      else if (deltaValue == 0) Set(-bValue / (2 * aValue))
      else {
        val root1 = (-bValue + sqrt(deltaValue)) / (2 * aValue)
        val root2 = (-bValue - sqrt(deltaValue)) / (2 * aValue)
        Set(root1, root2)
      }
    }
