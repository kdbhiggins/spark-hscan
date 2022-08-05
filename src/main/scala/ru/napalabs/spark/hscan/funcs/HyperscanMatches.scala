package ru.napalabs.spark.hscan.funcs

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.{UnaryExpression, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{BooleanType, DataType, DataTypes, StringType, ArrayType, IntegerType, LongType}
import org.apache.spark.unsafe.types.UTF8String
import com.gliwka.hyperscan.wrapper.{Expression => HSRegex, Database => HSDatabase, Scanner => HSScanner}
import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.InternalRow
import java.io.ByteArrayOutputStream

/**
 * Hyperscan RegEx pattern matching function
 */
@ExpressionDescription(
  usage = "_FUNC_(str, patterns) - Returns true if str matches one of pattern, " +
    "null if any arguments are null, false otherwise.",
  arguments ="""
  * str - a string expression
  * patterns - an array of strings expression.
  """
)
case class HyperscanMatches(child: Expression,  patterns: Seq[UTF8String]) extends UnaryExpression
  with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = ArrayType(LongType)

  override def inputTypes: Seq[DataType] = StringType :: Nil

  protected lazy val database = {
    HSDatabase.compile(patterns.map(p => new HSRegex(p.toString)).asJava)
  } 

  protected lazy val scanner = {
    val s = new HSScanner()
    s.allocScratch(database)
    s
  }
  
  def matches(exprValue: String): Any = {
    scanner.scan(database, exprValue)
    new GenericArrayData(scanner.getMatchedIds().asInstanceOf[Array[Any]])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val exprClass = classOf[com.gliwka.hyperscan.wrapper.Expression].getName
    val dbClass = classOf[com.gliwka.hyperscan.wrapper.Database].getName
    val scannerClass = classOf[com.gliwka.hyperscan.wrapper.Scanner].getName
    val compileException = classOf[com.gliwka.hyperscan.wrapper.CompileErrorException].getName
    val runtimeException = classOf[java.lang.RuntimeException].getName

      if (child != null) {
        val baos = new ByteArrayOutputStream
        database.save(baos)
        val byteArray = ctx.addReferenceObj("byteArray", baos.toByteArray)
        
        val databaseRef = ctx.addMutableState(dbClass, "dbHMatches",
          v => s"""
          try {
            $v = $dbClass.load(new java.io.ByteArrayInputStream($byteArray));
          } catch( java.io.IOException e) {
            throw new $runtimeException("cant compile pattern", e);
          }""")

        val scannerRef = ctx.addMutableState(scannerClass, "scannerHMatches",
          v =>
            s"""
              $v = new $scannerClass();
              $v.allocScratch($databaseRef);
            """)

        val eval = child.genCode(ctx)
        val arrayClass = classOf[GenericArrayData].getName
        ev.copy(code =
          code"""
           ${eval.code}
           boolean ${ev.isNull} = ${eval.isNull};
           ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
           if (!${ev.isNull}) {
            $scannerRef.scan($databaseRef, ${eval.value}.toString());
            ${ev.value} = new $arrayClass($scannerRef.getMatchedIds());
          }
           """)
      } else {
        ev.copy(code =
          code"""
            ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)}
           """)
      }
  }

  override protected def withNewChildInternal(newChild: Expression): HyperscanMatches =
    copy(child = newChild)


  override def eval(input: InternalRow): Any = {
    val exprValue = child.eval(input)
    if (exprValue == null) {
      null
    } else {
      matches(exprValue.toString)
    }
  }
}
