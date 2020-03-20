package com.org.assignment.unsolved

import java.util.Properties
import scala.io.Source
import java.io.FileInputStream
import java.io.InputStream

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import com.org.assignment.SparkSessionBuilder

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{min, row_number, when, expr}
import org.apache.spark.sql.SparkSession

object App {
  // Environment setup
  final val properties : Properties = new Properties()

  // Read properties file for input files paths
  val inputFile : InputStream = new FileInputStream("application.properties")
  
  // Generic method to fetch datasets and get dataframes
  def getDataframeFromCsv(datasetName: String) : Dataset[Row] = {
    val delimiter = if(datasetName.equals("3a")) " " else "|"
    val path = properties.getProperty(datasetName)
    val format = "csv"
    return SparkSessionBuilder.getDataframeFromCsv(path, format, delimiter)
  }
  
  // SOLUTION TO PROBLEM 1
  def problem1 : Unit = {
    val spark = SparkSessionBuilder.build
    import spark.implicits._
    
    val rivalDf = getDataframeFromCsv("1a")
    val productDF = getDataframeFromCsv("1b")
    val sellerDf = getDataframeFromCsv("1c")
        
    val windowSpecAgg  = Window.partitionBy($"productId")

    val rivalDf1 = rivalDf.withColumn(
              "minPrice", 
              min($"price").over(windowSpecAgg)
            )
            .filter($"price" === $"minPrice")
            .withColumnRenamed("productId", "prodId")
            .select("prodId", "minPrice", "rivalName", "saleEvent")
            
    val sellerDf1 = sellerDf.select("SellerID", "netValue").withColumnRenamed("SellerID", "SID")
    
    val joinedDf = productDF.join(
                     rivalDf1,
                     $"productId" === $"prodId",
                     "left"
                   ).join(
                       sellerDf1,
                       $"SellerID" === $"SID",
                       "left"
                   ).withColumn(
                     "finalPrice",
                     when(
                         // 1.	If PC + MaxMargin  < Q then  PC + MaxMargin
                         $"procuredValue" + $"maxMargin" < $"minPrice",
                         $"procuredValue" + $"maxMargin"
                     ).when(
                         // 2.	If PC + minMargin  < Q  then Q 
                         $"procuredValue" + $"minMargin" < $"minPrice",
                         $"minPrice"
                     ).when(
                         // 3.	If PC < Q,  And competitor with cheapest price is selling it in “Special” sale category then final price is Q. 
                         $"procuredValue" < $"minPrice" && $"saleEvent" === "Special",
                         $"minPrice"
                     ).when(
                         // 4.	If Q < PC  && competitor with cheapest price is selling is in “Special” sale category && seller classification  ==  “VeryHigh” then 0.9PC
                         $"minPrice" < $"procuredValue" && $"saleEvent" === "Special" && $"netValue" === "VeryHigh",
                         $"procuredValue" * 0.9
                     ).otherwise(
                         // 5.	If none of the conditions is satisfied then PC
                         $"procuredValue"
                     )
                   )

    joinedDf.select(
        $"ProductId",
        $"finalPrice".alias("FinalPrice"),
        $"lastModified".alias("TimeStamp"),
        expr("IF(minPrice IS NULL, procuredValue, minPrice)").alias("CheapestPrice"),
        $"rivalName".alias("RivalName")
    ).show
    
  }
  
  // Solution to problem 2
  def problem2 : Unit = {
    val spark = SparkSessionBuilder.build
    import spark.implicits._
    
    // Read the parquet from Hive Steps
    val benchmark = SparkSessionBuilder.getDataframeFromParquete.withColumnRenamed("Section", "Sec")
    
    // 1.	Filter out Students who have scored lower than average marks (calculated in above step) in that section 
    val studentDf = getDataframeFromCsv("2b")
    val qtBenchmark = benchmark.filter($"Sec" === "QT")
    val raBenchmark = benchmark.filter($"Sec" === "RA")
    val vbBenchmark = benchmark.filter($"Sec" === "VB")
    
    val studentQT = studentDf.join(
                      qtBenchmark
                    ).filter(
                        $"QT" < $"avgmarks"
                    )
    val studentRA = studentDf.join(
                      raBenchmark
                    ).filter(
                        $"RA" < $"avgmarks"
                    )    
    val studentVB = studentDf.join(
                      vbBenchmark
                    ).filter(
                        $"VB" < $"avgmarks"
                    )
    studentQT.printSchema()
    // 2.	Find out wrongly answered  and un-attempted questions in that section
    val studentsConsolidated = studentQT.select(
                                "StudentId", 
                                "Sec"
                              ).union(
                                  studentRA.select(
                                      "StudentId", 
                                      "Sec"
                                  ).union(
                                      studentVB.select(
                                          "StudentId",
                                          "Sec"
                                      )
                                  )
                              ).withColumnRenamed("Section", "Sec")
    
    // Reading the student_response.txt
    val studentResponse = getDataframeFromCsv("2c")
    val quesFilterCondition = ($"Answered" === "N") || ($"Result" === "W")

    // Filtering out wrongly answered questions
    val wrongQuestions = studentResponse.filter(
                            quesFilterCondition
                          ).join(
                            studentsConsolidated,
                            $"Section" === $"Sec"
                          )
    
    
    
    // 3.	remove “Very difficult” and “Difficult” questions 
    // Read the question data
    val filterConditions = ($"DifficultyLevel" !== "Difficult") && ($"DifficultyLevel" !== "Very difficult")
    val questionsDf = getDataframeFromCsv("2a").filter(filterConditions)
    
    // 4.	find out category of these questions  
    val questionCategory = questionsDf.select("category").distinct
    
    // 5.	Now Fetch 5-“Medium” (MQ) and 5-“Easy” (EQ) questions for each category from question pool 
    val windowSpecAgg  = Window.partitionBy($"category", $"DifficultyLevel")
   
    val finalQuestions = questionsDf.withColumn(
                            "QuesNumber",
                            row_number().over(windowSpecAgg.orderBy($"CreatedDate"))
                        ).filter($"QuesNumber" <= 5)
                        .orderBy($"Category", $"DifficultyLevel", $"QuesNumber").show
    
  }
  
  def main(args: Array[String]): Unit = {
    println("Hello, World!")
    
    // Populate properties from the properties file
    properties.load(inputFile)
    
    // Solution to problem 1
    // problem1
    
    // Solution to problem 2
    problem2
  }
}