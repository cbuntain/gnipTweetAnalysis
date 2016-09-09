package edu.umd.cs.hcil.spark.gnip.utils

import com.zaubersoftware.gnip4j.api.model._
import collection.JavaConversions._
import java.util.regex.Pattern

import scala.io.Source

/**
  * Created by cbuntain on 8/29/16.
  */
object ActivityTokenizer {
  val punctRegex = Pattern.compile("[\\p{Punct}\n\t]")

  def tokenize(status : Activity) : List[String] = {

    var tweetText = status.getBody
    var tokens : List[String] = List.empty

    val entities : TwitterEntities = status.getTwitterEntities

    // Replace hashtags with space
    for ( hashtagEntity : Hashtags <- entities.getHashtags ) {
      val hashtag = "#" + hashtagEntity.getText
      tokens = tokens :+ hashtag

      tweetText = tweetText.replace(hashtag, " ")
    }

    // Replace mentions
    for ( mentionEntity : UserMentions <- entities.getUserMentions ) {
      val mention = "@" + mentionEntity.getScreenName
      tokens = tokens :+ mention

      tweetText = tweetText.replace(mention, " ")
    }

    // Replace URLs
    for ( urlEntity : Urls <- entities.getUrls ) {
      val url = urlEntity.getExpandedUrl

      if ( url != null ) {
        tokens = tokens :+ url

        tweetText = tweetText.replace(url, " ")
      }
    }

    // Replace media with URLs
    for ( mediaEntity : MediaUrls <- entities.getMediaUrls ) {
      val mediaUrl = mediaEntity.getExpandedUrl
      tokens = tokens :+ mediaUrl

      tweetText = tweetText.replace(mediaEntity.getExpandedUrl, " ")
    }

    val cleanedTweetText = punctRegex.matcher(tweetText).replaceAll(" ")
    tokens = tokens ++ cleanedTweetText.split(" ").filter(token => token.length > 0)

    return tokens
  }

  def main(args: Array[String]): Unit = {
    println("Reading tweets from: " + args(0))

    val filename = args(0)

    for (line <- Source.fromFile(filename).getLines()) {

      val status = JsonToActivity.jsonToStatus(line)
      println(status.getBody)
      for (token <- tokenize(status)) {
        println("[" + token + "]")
      }
    }
  }

}
