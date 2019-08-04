/*
 * Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

import org.apache.log4j._
import org.apache.spark._

/** Compute the total amount spent per customer in some fake e-commerce data. */
object TotalSpentByCustomerSorted {
  
  /** Convert input data to (customerID, amountSpent) tuples */
  def extractCustomerPricePairs(line: String) = {
    var fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalSpentByCustomerSorted")   
    
    val input = sc.textFile("../migas-app/customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)
    
    val totalByCustomer = mappedInput.reduceByKey( (x,y) => x + y )
    
    val flipped = totalByCustomer.map( x => (x._2, x._1) )
    
    val totalByCustomerSorted = flipped.sortByKey()
    
    val results = totalByCustomerSorted.collect()
    
    // Print the results.
    results.foreach(println)
  }
  
}

