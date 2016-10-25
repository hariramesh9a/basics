package com.hari.sparkstream
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object basics {
  
  
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local[*]").setAppName("Basics")
    val sc=new SparkContext(conf)
    
    val myFile=sc.textFile("data/product.csv", 2)
    val myData=myFile.map(line=>line.split(",")).map(rec=>(rec(0),rec(1),rec(2)))
    myData.cache()
    
    val myTotalRevenue=myData.map{case(name,product,price)=>price.toDouble}.sum()
    println("Total Revenue "+myTotalRevenue)
    
    
    val popularProduct=myData.map{case(name,product,price)=>(product,1)}.reduceByKey(_+_).collect().sortBy(-_._2)
    println("Popular Product "+popularProduct(0)._1)
    
    println("Sales "+myData.count())
    
    
    
    
  }
  
  
  def formattedPrint[Any](f:()=>Any , funcDescr:String ,myData: Any):Unit={
    
    
    
    
  }
  
  
}