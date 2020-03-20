package com.org.assignment

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row


object SparkSessionBuilder {
  def build : SparkSession = {
    val conf = new SparkConf().setMaster("local")
    
    val sparkSession = SparkSession.builder
      .config(conf)
      .appName("Test Pack")
      .getOrCreate()
      sparkSession
  }
  
  // Generic method to fetch datasets and get dataframes
  def getDataframeFromCsv(path: String, format: String, delimiter: String) : Dataset[Row] = {
    val spark = SparkSessionBuilder.build
    val df = spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("delimiter", delimiter)
              .option("path", path)
              .load
    df
  }
  
  def getDataframeFromParquete : Dataset[Row] = {
    val spark = SparkSessionBuilder.build
    import spark.implicits._
    
    return spark.read.format("parquet").load("resources/input/000000_0")
  }
}