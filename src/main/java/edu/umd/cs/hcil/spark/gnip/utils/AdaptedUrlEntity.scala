package edu.umd.cs.hcil.spark.gnip.utils

import twitter4j.URLEntity

/**
  * Created by cbuntain on 9/9/16.
  */
@SerialVersionUID(100L)
class AdaptedUrlEntity (start : Int, end : Int, url : String, text : String, expUrl : String, display : String)
  extends URLEntity with Serializable {

  override def getDisplayURL: String = display

  override def getURL: String = url

  override def getStart: Int = start

  override def getEnd: Int = end

  override def getExpandedURL: String = expUrl

  override def getText: String = text
}

