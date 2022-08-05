package ru.napalabs.spark.hscan

import org.scalatest.FunSuite
import scala.collection.mutable.WrappedArray
import functions._

class FunctionsTest extends FunSuite with SparkSetup {
  test("hlike function") {
    withSpark { (sc, spark) =>
      val rdd = sc.makeRDD(List(("Lorem ipsum"), ("dolor sit amet"), ("consectetur adipiscing elit")))
      import spark.implicits._
      val df = rdd.toDF("val")
      val actual = df.select(hlike($"val", Array("Lorem? ", "dolo[^ ]+"))).collect().map(_.getBoolean(0))
      val expected = Array(true, true, false)
      assert(actual sameElements expected)
    }
  }
   test("hmatches function") {
    withSpark { (sc, spark) =>
      val rdd = sc.makeRDD(List(("Lorem ipsum"), ("dolor sit amet"), ("consectetur adipiscing elit"), ("Lorem  dolor"), (null)))
      import spark.implicits._
      val df = rdd.toDF("val")
      val actual = df.select(hmatches($"val", Array("Lorem? ", "dolo[^ ]+"))).collect().map(_.getAs[WrappedArray[Long]](0))
      val expected = Seq(Seq(0L), Seq(1L), Seq(), Seq(0L, 1L), null)
      assert(actual sameElements expected)
    }
  }
}
