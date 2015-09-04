package calculator

sealed abstract class Expr
final case class Literal(v: Double) extends Expr
final case class Ref(name: String) extends Expr
final case class Plus(a: Expr, b: Expr) extends Expr
final case class Minus(a: Expr, b: Expr) extends Expr
final case class Times(a: Expr, b: Expr) extends Expr
final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {
  def computeValues(
      namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {
    namedExpressions.mapValues { x:Signal[Expr] => Signal(eval(x(), namedExpressions)) }
  }

  def eval(expr: Expr, references: Map[String, Signal[Expr]]): Double = {
    expr match {
      case l:Literal => l.v
      case r:Ref => eval(getReferenceExpr(r.name, references), references - r.name)
      case p:Plus => eval(p.a, references) + eval(p.b, references)
      case m:Minus => eval(m.a, references) - eval(m.b, references)
      case t:Times => eval(t.a, references) * eval(t.b, references)
      case d:Divide => eval(d.a, references) / eval(d.b, references)
    }
  }

  /** Get the Expr for a referenced variables.
   *  If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String,
      references: Map[String, Signal[Expr]]) = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
