package edu.umd.cs.hcil.spark.gnip

import com.zaubersoftware.gnip4j.api.model.Activity
import edu.umd.cs.hcil.spark.gnip.utils.JsonToActivity
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by cbuntain on 8/13/16.
  */
object TweetSelector {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Find Tweets")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val idPath = args(2)

    val idSet = scala.io.Source.fromFile(idPath).getLines.toSet

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    // Repartition if desired using the new partition count
    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
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

    println("Tweet Count: " + tweets.count())

    val matchingActivities = selectTweetsById(tweets, idSet)

    matchingActivities.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }


  /**
    * Keep only those tweets that matched the given set of tweet ids
    *
    * @param tweets The RDD of Activity objects and JSON strings
    * @param idSet Set of rules we're looking for
    */
  def selectTweetsById(tweets : RDD[(Activity, String)], idSet : Set[String]) : RDD[(Activity, String)] = {
    val filteredTweets = tweets.filter(pair => {
      val activityId = pair._1.getId
      idSet.contains(activityId)
    })

    return filteredTweets
  }

}
