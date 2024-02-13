package recfun

object RecFun extends RecFunInterface:

  def main(args: Array[String]): Unit =
    println("Pascal's Triangle")
    for row <- 0 to 10 do
      for col <- 0 to row do
        print(s"${pascal(col, row)} ")
      println()


  /*var testeStrings = List(
    "if (zero? X) max (/1 X ))",
    "je TSte avec lui tout sorte de solution possible est IN(())°°°)(-°55(()))", 
    ": ->)",
    "[(())])", 
    "()"
  )
  testeStrings.foreach{ testString =>
      println(s"Balance check for '$testString': ${balance(testString.toList)}")
    }

      // Exemples pour countChange
    val testCases = List(
      (4, List(1, 2)), // Exemple 1
      (10, List(5, 2, 3)), // Exemple 2
      (300,List(5,10,20,50,100,200,500)), // Exemple 3
      (301,List(5,10,20,50,100,200,500)), // Exemple 4
      (300,List(500,5,50,100,20,200,10)) // Exemple 5
    )
  
    testCases.foreach { case (money, coins) =>
      println(s"CountChange for $money with coins ${coins.mkString(",")}: ${countChange(money, coins)}")
    }*/

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int = {
    if (c == 0 || c ==  r) 1
    else pascal(c - 1, r - 1) + pascal(c , r - 1)
  }

  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = {
    def  balanced(chars: List[Char], openCount: Int): Boolean = {
      if(chars.isEmpty) openCount == 0
      else if (chars.head == '(') balanced(chars.tail, openCount + 1)
      else if (chars.head == ')') openCount > 0 && balanced(chars.tail, openCount - 1)
      else balanced(chars.tail, openCount)
    }
    balanced(chars, 0)
  }

  /**
   * Exercise 3
   */
  def countChange(money: Int, coins: List[Int]): Int = {
    if(money == 0) 1
    else if (money < 0 || coins.isEmpty) 0
    else countChange(money - coins.head, coins) + countChange(money, coins.tail)
  }
