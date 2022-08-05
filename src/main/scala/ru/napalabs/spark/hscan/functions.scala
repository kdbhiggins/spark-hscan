package ru.napalabs.spark.hscan

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import ru.napalabs.spark.hscan.funcs.HyperscanLike
import ru.napalabs.spark.hscan.funcs.HyperscanMatches
import org.apache.spark.unsafe.types.UTF8String

object functions {
  private def withExpr(expr: Expression): Column = new Column(expr)

  /**
   * Hyperscan regex like. Hyperscan regexps differs from java regexps,
   * see <a href="https://intel.github.io/hyperscan/dev-reference/compilation.html#semantics">hyperscan documentation</a>
   * @return boolean column with match result
   */
  def hlike(e: Column, patterns: Array[String]): Column = withExpr{
    HyperscanLike(e.expr, Literal(patterns))
  }

  /**
   * Hyperscan regex like. Hyperscan regexps differs from java regexps,
   * see <a href="https://intel.github.io/hyperscan/dev-reference/compilation.html#semantics">hyperscan documentation</a>
   * @return boolean column with match result
   */
  def hlike(e: Column, patterns: Column): Column = withExpr{
    HyperscanLike(e.expr, patterns.expr)
  }

  /**
   * Hyperscan regex like. Hyperscan regexps differs from java regexps,
   * see <a href="https://intel.github.io/hyperscan/dev-reference/compilation.html#semantics">hyperscan documentation</a>
   * @return boolean column with match result
   */
  def hmatches(e: Column, patterns: Array[String]): Column = withExpr{
    HyperscanMatches(e.expr, patterns.map(UTF8String.fromString))
  }

//   /**
//    * Hyperscan regex like. Hyperscan regexps differs from java regexps,
//    * see <a href="https://intel.github.io/hyperscan/dev-reference/compilation.html#semantics">hyperscan documentation</a>
//    * @return boolean column with match result
//    */
//   def hmatches(e: Column, patterns: Column): Column = withExpr{
//     HyperscanMatches(e.expr, patterns.expr)
//   }
}
