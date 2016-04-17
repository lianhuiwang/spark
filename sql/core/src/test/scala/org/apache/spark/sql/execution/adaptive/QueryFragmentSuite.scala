package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

class QueryFragmentSuite extends QueryTest with SQLTestUtils with SharedSQLContext {
  import testImplicits._

  setupTestData()

  test("adaptive query optimization: broadcast join") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION2_ENABLED.key-> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key-> "100000") {
      val numInputPartitions: Int = 10
      val df1 =sqlContext.range(0, 20000, 1, numInputPartitions)
        .selectExpr("id % 500 as key1", "id as value1")
        .groupBy("key1")
        .agg($"key1", count("value1") as "cnt1")
      val df2 =sqlContext.range(0, 20000, 1, numInputPartitions)
        .selectExpr("id % 500 as key2", "id as value2")
        .groupBy("key2")
        .agg($"key2", count("value2") as "cnt2")
      val join =
        df1.join(df2, col("key1") === col("key2"))
          .select(col("key"), col("cnt1"), col("cnt2"))

      checkAnswer(join, sqlContext.range(0, 500).selectExpr("id as key", "40 as cnt1", "40 as cnt2").collect())
    }
  }
}
