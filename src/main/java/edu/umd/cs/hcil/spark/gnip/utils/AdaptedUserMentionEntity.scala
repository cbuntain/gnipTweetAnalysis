package edu.umd.cs.hcil.spark.gnip.utils

import twitter4j.UserMentionEntity

/**
  * Created by cbuntain on 9/9/16.
  */
@SerialVersionUID(100L)
class AdaptedUserMentionEntity (
                                 start : Int,
                                 end : Int,
                                 text : String,
                                 id : Long,
                                 name : String,
                                 screenName : String)
    extends UserMentionEntity with Serializable {

  override def getScreenName: String = screenName

  override def getName: String = name

  override def getId: Long = id

  override def getStart: Int = start

  override def getEnd: Int = end

  override def getText: String = text
}
