package com.org.assignment

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
object dataFrameMatchers {
  implicit class DataFrameChecks(val df: DataFrame) {
    def shouldHaveSchemaOf(expectedDataFrame: DataFrame): Unit = shouldHaveSchema(expectedDataFrame.schema)
    def shouldHaveSchema(expectedSchema: StructType): Unit = assert(df.schema == expectedSchema)
  }
}