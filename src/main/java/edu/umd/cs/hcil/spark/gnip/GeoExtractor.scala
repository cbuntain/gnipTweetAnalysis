package edu.umd.cs.hcil.spark.gnip

import com.zaubersoftware.gnip4j.api.model.Activity
import edu.umd.cs.hcil.spark.gnip.utils.JsonToActivity
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Given a set of rules in a file, save the tweets that match the given rule
  *
  * Created by cbuntain on 8/14/16.
  */
object GeoExtractor {

  /**
    * Filter function that performs the test
    *
    * @param activity Tweet to test
    * @return
    */
  def containsGeo(activity : Activity) : Boolean = {
    return activity.getGeo != null
  }

  /**
    * Keep only those tweets that matched the given rule
    *
    * @param tweets The RDD of Activity objects and JSON strings
    */
  def selectGeoTweets(tweets : RDD[(Activity, String)]) : RDD[(Activity, String)] = {
    val filteredTweets = tweets.filter(pair => {
      val activity = pair._1
      containsGeo(activity)
    })

    return filteredTweets
  }

  /**
    * Driver for selecting specific Gnip-matched rules from data and saving
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Gnip Rule Selector")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    // Repartition if desired using the new partition count
    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 2 ) {
      val initialPartitions = args(2).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }
    val newPartitionSize = twitterMsgs.partitions.size

    // Convert each JSON line in the file to a status using Gnip4j
    //  Note that not all lines are Activity lines, so we catch any exception
    //  generated during this conversion and set to null since we don't care
    //  about non-status lines.
    val tweets = twitterMsgs
      .filter(line => line.length > 0)
      .map(line => (JsonToActivity.jsonToStatus(line), line))
      .filter(pair => pair._1 != null)

    val matchingActivities = selectGeoTweets(tweets).map(tup => tup._2)

    matchingActivities.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }
}
