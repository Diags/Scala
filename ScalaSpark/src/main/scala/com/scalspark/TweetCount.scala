

package com.scalspark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TweetCount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val context = new SparkContext(conf)
    val fileText = context.textFile("/tweetFile")
    val splitsTab = fileText.flatMap(line => line.split(" "))
    val counts = splitsTab.map(word => (word, 1))
    val wordCount = counts.reduceByKey(_ + _)
    val wordcount_sorted = wordCount.sortByKey()
    wordcount_sorted.foreach(println)
    //store output into a file system
    wordcount_sorted.saveAsTextFile("bouna")

  }
}