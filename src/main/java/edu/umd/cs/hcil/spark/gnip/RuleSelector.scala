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
object RuleSelector {

  /**
    * Filter function that performs the test
    *
    * @param activity Tweet to test
    * @param ruleName Name of the rule we're looking for
    * @return
    */
  def filterByName(activity : Activity, ruleName : String) : Boolean = {
    return activity.getGnip.getMatchingRules.map(rule => rule.getTag).contains(ruleName)
  }

  /**
    * Filter function that performs the test
    *
    * @param activity Tweet to test
    * @param ruleSet Set of the rule we're looking for
    * @return
    */
  def filterByNameSet(activity : Activity, ruleSet : Set[String]) : Boolean = {
    return activity.getGnip.getMatchingRules.map(rule => rule.getTag).toSet.intersect(ruleSet).size > 0
  }

  /**
    * Keep only those tweets that matched the given rule
    *
    * @param tweets The RDD of Activity objects and JSON strings
    * @param ruleName Name of the rule we're looking for
    */
  def selectTweetsByRule(tweets : RDD[(Activity, String)], ruleName : String) : RDD[(Activity, String)] = {
    val filteredTweets = tweets.filter(pair => {
      val activity = pair._1
      filterByName(activity, ruleName)
    })

    return filteredTweets
  }

  /**
    * Keep only those tweets that matched the given set of rules
    *
    * @param tweets The RDD of Activity objects and JSON strings
    * @param ruleSet Set of rules we're looking for
    */
  def selectTweetsByRules(tweets : RDD[(Activity, String)], ruleSet : Set[String]) : RDD[(Activity, String)] = {
    val filteredTweets = tweets.filter(pair => {
      val activity = pair._1
      filterByNameSet(activity, ruleSet)
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
    val ruleTagsFile = args(1)
    val outputPath = args(2)

    val ruleSet = scala.io.Source.fromFile(ruleTagsFile).getLines.toSet

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    // Repartition if desired using the new partition count
    var twitterMsgs = twitterMsgsRaw
    if ( args.size >= 4 ) {
      val initialPartitions = args(4).toInt
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

    val matchingActivities = selectTweetsByRules(tweets, ruleSet)

    matchingActivities.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }
}
