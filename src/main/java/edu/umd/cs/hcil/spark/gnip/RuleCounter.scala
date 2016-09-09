package edu.umd.cs.hcil.spark.gnip

import com.zaubersoftware.gnip4j.api.model.MatchingRules
import edu.umd.cs.hcil.spark.gnip.utils.JsonToActivity
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * Created by cbuntain on 8/13/16.
  */
object RuleCounter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Gnip Rule Counter")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)

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
      .map(JsonToActivity.jsonToStatus(_))
      .filter(activity => activity != null)

    println("Tweet Count: " + tweets.count())

    val ruleCounts = tweets
      .flatMap(activity => (activity.getGnip.getMatchingRules.map(rule => (rule.getTag, 1))))
      .reduceByKey((l, r) => l + r)
      .collect()

    for ( pair <- ruleCounts ) {
      println("Rule: " + pair._1 + ", Count: " + pair._2)
    }
  }

}
