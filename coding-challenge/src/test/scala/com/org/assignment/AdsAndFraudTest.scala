package com.org.assignment

import com.org.assignment.unsolved.AdsAndFraudsAnswers
import com.org.assignment.unsolved.{AdsAndFrauds}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import com.org.assignment.unsolved.AdsAndFrauds
import org.apache.spark.sql.internal.SQLConf
import java.io.InputStream
import java.io.FileInputStream
import java.util.Properties

class AdsAndFraudTest extends QueryTest with SharedSQLContext {
  import dataFrameMatchers._
  
  var adsInputDir : String = null
  var fraudulentInputDir : String = null

  // Read properties file for input files
  val inputFile : InputStream = new FileInputStream("application.properties")
  val properties = new Properties()
  properties.load(inputFile)
  adsInputDir = properties.getProperty("adsInputDir")
  fraudulentInputDir = properties.getProperty("fraudulentInputDir")
  
  test("Check Answer 1") {
    val adsAndFraudsDF = new AdsAndFraudsAnswers(adsInputDir, fraudulentInputDir, 1).getAnswer

    val expectedOutput = spark.read.format("csv")
      .schema(StructType(
        StructField("adId", LongType) ::
          StructField("dateCreated", StringType) ::
          Nil))
      .load("src/test/resources/expectedOutput/answer1.csv")

    checkAnswer(expectedOutput, adsAndFraudsDF)
    adsAndFraudsDF.shouldHaveSchemaOf(expectedOutput)
  }

  test("Check Answer 2"){
    val avgPriceOfAllFraudsDF = new AdsAndFraudsAnswers(adsInputDir, fraudulentInputDir, 2).getAnswer

    val expectedOutput = spark.read.format("csv")
      .schema(StructType(
        StructField("AveragePrice", DoubleType) :: Nil)
      )
      .load("src/test/resources/expectedOutput/answer2.csv")

    checkAnswer(expectedOutput, avgPriceOfAllFraudsDF)
    avgPriceOfAllFraudsDF.shouldHaveSchemaOf(expectedOutput)

  }

  test("Check Answer 3"){
    val avgPriceOfFraudAdsDatewise = new AdsAndFraudsAnswers(adsInputDir, fraudulentInputDir, 3).getAnswer

    val expectedOutput = spark.read.format("csv")
      .schema(StructType(
        StructField("dateCreated", StringType) ::
          StructField("AveragePrice", DoubleType) ::
          Nil))
      .load("src/test/resources/expectedOutput/answer3.csv")

    checkAnswer(expectedOutput, avgPriceOfFraudAdsDatewise)
    avgPriceOfFraudAdsDatewise.shouldHaveSchemaOf(expectedOutput)
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
  }
}
