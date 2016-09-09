package edu.umd.cs.hcil.spark.gnip.utils

import twitter4j.HashtagEntity

/**
  * Created by cbuntain on 9/9/16.
  */
@SerialVersionUID(100L)
class AdaptedHashtagEntity (start : Int, end : Int, hashtag : String) extends HashtagEntity with Serializable {
  override def getStart: Int = start

  override def getEnd: Int = end

  override def getText: String = hashtag
}