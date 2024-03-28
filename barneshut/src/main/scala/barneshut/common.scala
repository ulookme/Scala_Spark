package barneshut

import java.util.concurrent.*
import scala.{collection => coll}
import scala.util.DynamicVariable
import barneshut.conctrees.*

class Boundaries:
  var minX = Float.MaxValue

  var minY = Float.MaxValue

  var maxX = Float.MinValue

  var maxY = Float.MinValue

  def width = maxX - minX

  def height = maxY - minY

  def size = math.max(width, height)

  def centerX = minX + width / 2

  def centerY = minY + height / 2

  override def toString = s"Boundaries($minX, $minY, $maxX, $maxY)"

sealed abstract class Quad extends QuadInterface:
  def massX: Float

  def massY: Float

  def mass: Float

  def centerX: Float

  def centerY: Float

  def size: Float

  def total: Int

  def insert(b: Body): Quad

case class Empty(centerX: Float, centerY: Float, size: Float) extends Quad:
  def massX: Float = 0.0f
  def massY: Float = 0.0f
  def mass: Float = 0.0f
  def total: Int = 0
  def insert(b: Body): Quad = Leaf(centerX, centerY, size, coll.Seq(b))

case class Fork(
  nw: Quad, ne: Quad, sw: Quad, se: Quad
) extends Quad:
  val centerX: Float = (nw.centerX + ne.centerX + sw.centerX + se.centerX) / 4
  val centerY: Float = (nw.centerY + ne.centerY + sw.centerY + se.centerY) / 4
  val size: Float = nw.size * 2
  val mass: Float = nw.mass + ne.mass + sw.mass + se.mass
  val massX: Float = if (mass != 0) (nw.mass * nw.massX + ne.mass * ne.massX + sw.mass * sw.massX + se.mass * se.massX) / mass else centerX
  val massY: Float = if (mass != 0) (nw.mass * nw.massY + ne.mass * ne.massY + sw.mass * sw.massY + se.mass * se.massY) / mass else centerY
  val total: Int = nw.total + ne.total + sw.total + se.total

  def insert(b: Body): Fork =
    if (b.x <= centerX) {
      if (b.y <= centerY) Fork(nw.insert(b), ne, sw, se)
      else Fork(nw, ne, sw.insert(b), se)
    } else {
      if (b.y <= centerY) Fork(nw, ne.insert(b), sw, se)
      else Fork(nw, ne, sw, se.insert(b))
    }

case class Leaf(centerX: Float, centerY: Float, size: Float, bodies: coll.Seq[Body]) extends Quad:
  
  val (mass, massX, massY) = (
    bodies.map(_.mass).sum,
    if (bodies.nonEmpty) bodies.map(b => b.mass * b.x).sum / bodies.map(_.mass).sum else centerX,
    if (bodies.nonEmpty) bodies.map(b => b.mass * b.y).sum / bodies.map(_.mass).sum else centerY
  )
  val total: Int = bodies.size
  
  def insert(b: Body): Quad =
    if (size > minimumSize) {
      val emptyQuadrants = Fork(
        Empty(centerX - size / 4, centerY - size / 4, size / 2),
        Empty(centerX + size / 4, centerY - size / 4, size / 2),
        Empty(centerX - size / 4, centerY + size / 4, size / 2),
        Empty(centerX + size / 4, centerY + size / 4, size / 2)
      )
      (bodies :+ b).foldLeft(emptyQuadrants: Quad)((quad, body) => quad.insert(body))
    } else Leaf(centerX, centerY, size, bodies :+ b)

def minimumSize = 0.00001f

def gee: Float = 100.0f

def delta: Float = 0.01f

def theta = 0.5f

def eliminationThreshold = 0.5f

def force(m1: Float, m2: Float, dist: Float): Float = gee * m1 * m2 / (dist * dist)

def distance(x0: Float, y0: Float, x1: Float, y1: Float): Float =
  math.sqrt((x1 - x0) * (x1 - x0) + (y1 - y0) * (y1 - y0)).toFloat

class Body(val mass: Float, val x: Float, val y: Float, val xspeed: Float, val yspeed: Float):

  def updated(quad: Quad): Body =
    var netforcex = 0.0f
    var netforcey = 0.0f

    def addForce(thatMass: Float, thatMassX: Float, thatMassY: Float): Unit =
      val dx = thatMassX - x
      val dy = thatMassY - y
      val dist = math.sqrt(dx * dx + dy * dy).toFloat

      if dist > 1f then
        val force = gee * mass * thatMass / (dist * dist)
        netforcex += dx / dist * force
        netforcey += dy / dist * force

    def traverse(quad: Quad): Unit = quad match
      case Empty(_, _, _) => // No force to add
      case Leaf(_, _, _, bodies) =>
        bodies.foreach(body => if !(body.x == x && body.y == y) then addForce(body.mass, body.x, body.y))
      case Fork(nw, ne, sw, se) =>
        if (distance(x, y, quad.centerX, quad.centerY) / quad.size) < theta then
          addForce(quad.mass, quad.massX, quad.massY)
        else
          traverse(nw); traverse(ne); traverse(sw); traverse(se)

    traverse(quad)

    val nxspeed = xspeed + netforcex / mass * delta
    val nyspeed = yspeed + netforcey / mass * delta
    val nx = x + nxspeed * delta
    val ny = y + nyspeed * delta

    Body(mass, nx, ny, nxspeed, nyspeed)


val SECTOR_PRECISION = 8

class SectorMatrix(val boundaries: Boundaries, val sectorPrecision: Int) extends SectorMatrixInterface:
  val sectorSize = boundaries.size / sectorPrecision
  val matrix = new Array[ConcBuffer[Body]](sectorPrecision * sectorPrecision)
  for i <- 0 until matrix.length do matrix(i) = ConcBuffer()

  def +=(b: Body): SectorMatrix =
    val xIndex = ((b.x - boundaries.minX) / sectorSize).toInt
    val yIndex = ((b.y - boundaries.minY) / sectorSize).toInt
    val index = yIndex * sectorPrecision + xIndex
    matrix(index) += b
    this

  def apply(x: Int, y: Int) = matrix(y * sectorPrecision + x)

  def combine(that: SectorMatrix): SectorMatrix =
    for i <- matrix.indices do
      matrix(i) = matrix(i).combine(that.matrix(i))
    this

  def toQuad(parallelism: Int): Quad =
    def BALANCING_FACTOR = 4
    def quad(x: Int, y: Int, span: Int, achievedParallelism: Int): Quad =
      if span == 1 then
        val sectorSize = boundaries.size / sectorPrecision
        val centerX = boundaries.minX + x * sectorSize + sectorSize / 2
        val centerY = boundaries.minY + y * sectorSize + sectorSize / 2
        var emptyQuad: Quad = Empty(centerX, centerY, sectorSize)
        val sectorBodies = this(x, y)
        sectorBodies.foldLeft(emptyQuad)(_ insert _)
      else
        val nspan = span / 2
        val nAchievedParallelism = achievedParallelism * 4
        val (nw, ne, sw, se) =
          if parallelism > 1 && achievedParallelism < parallelism * BALANCING_FACTOR then parallel(
            quad(x, y, nspan, nAchievedParallelism),
            quad(x + nspan, y, nspan, nAchievedParallelism),
            quad(x, y + nspan, nspan, nAchievedParallelism),
            quad(x + nspan, y + nspan, nspan, nAchievedParallelism)
          ) else (
            quad(x, y, nspan, nAchievedParallelism),
            quad(x + nspan, y, nspan, nAchievedParallelism),
            quad(x, y + nspan, nspan, nAchievedParallelism),
            quad(x + nspan, y + nspan, nspan, nAchievedParallelism)
          )
        Fork(nw, ne, sw, se)

    quad(0, 0, sectorPrecision, 1)

  override def toString = s"SectorMatrix(#bodies: ${matrix.map(_.size).sum})"

class TimeStatistics:
  private val timeMap = collection.mutable.Map[String, (Double, Int)]()

  def clear() = timeMap.clear()

  def timed[T](title: String)(body: =>T): T =
    var res: T = null.asInstanceOf[T]
    val totalTime = /*measure*/
      val startTime = System.currentTimeMillis()
      res = body
      (System.currentTimeMillis() - startTime)

    timeMap.get(title) match
      case Some((total, num)) => timeMap(title) = (total + totalTime, num + 1)
      case None => timeMap(title) = (totalTime.toDouble, 1)

    println(s"$title: ${totalTime} ms; avg: ${timeMap(title)._1 / timeMap(title)._2}")
    res

  override def toString =
    timeMap map {
      case (k, (total, num)) => k + ": " + (total / num * 100).toInt / 100.0 + " ms"
    } mkString("\n")

val forkJoinPool = ForkJoinPool()

abstract class TaskScheduler:
  def schedule[T](body: => T): ForkJoinTask[T]
  def parallel[A, B](taskA: => A, taskB: => B): (A, B) =
    val right = task {
      taskB
    }
    val left = taskA
    (left, right.join())

class DefaultTaskScheduler extends TaskScheduler:
  def schedule[T](body: => T): ForkJoinTask[T] =
    val t = new RecursiveTask[T] {
      def compute = body
    }
    Thread.currentThread match
      case wt: ForkJoinWorkerThread =>
        t.fork()
      case _ =>
        forkJoinPool.execute(t)
    t

val scheduler =
  DynamicVariable[TaskScheduler](DefaultTaskScheduler())

def task[T](body: => T): ForkJoinTask[T] =
  scheduler.value.schedule(body)

def parallel[A, B](taskA: => A, taskB: => B): (A, B) =
  scheduler.value.parallel(taskA, taskB)

def parallel[A, B, C, D](taskA: => A, taskB: => B, taskC: => C, taskD: => D): (A, B, C, D) =
  val ta = task { taskA }
  val tb = task { taskB }
  val tc = task { taskC }
  val td = taskD
  (ta.join(), tb.join(), tc.join(), td)
