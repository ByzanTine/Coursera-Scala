package recfun
import common._

object Main {
  def main(args: Array[String]) {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(pascal(col, row) + " ")
      println()
    }
    println("Balance")
    println("(if (zero? x) max (/ 1 x))" + 
      balance("(if (zero? x) max (/ 1 x))".toList))
    println("())(" + balance("())(".toList));
    println("Count Change")
    println("change 4, 1,2   " + countChange(2, List(1,2)))
  }

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int =
    if (c == 0 || c == r) 1
    else pascal(c, r - 1) + pascal(c - 1, r - 1)

  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = {
    def balanceHelp(chars: List[Char], stack: Int): Boolean = {
      if (chars.isEmpty)
        stack == 0
      else if (chars.head == ')')
        if (stack == 0) false
        else balanceHelp(chars.tail, stack - 1)
      else if (chars.head == '(') balanceHelp(chars.tail, stack + 1)
      else balanceHelp(chars.tail, stack)

    }
    balanceHelp(chars, 0)
  }

  /**
   * Exercise 3
   */
  def countChange(money: Int, coins: List[Int]): Int = {
    if (money == 0) 1
    else if (money < 0 || coins.isEmpty) 0
    else //money > 0
        countChange(money - coins.head, coins) + 
          countChange(money, coins.tail)
  }
  
}
