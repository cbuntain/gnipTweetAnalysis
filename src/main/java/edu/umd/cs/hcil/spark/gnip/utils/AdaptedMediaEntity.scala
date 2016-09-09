package edu.umd.cs.hcil.spark.gnip.utils

import twitter4j.MediaEntity

import twitter4j.MediaEntity.Size

/**
  * Created by cbuntain on 9/9/16.
  */
@SerialVersionUID(100L)
class AdaptedMediaEntity (start : Int, end : Int, url : String, text : String, expUrl : String, display : String,
                          mediaUrl : String, mediaHttps : String, id : String, mediaType : String, sizes : java.util.Map[Integer, Size])
  extends AdaptedUrlEntity(start, end, url, text, expUrl, display) with MediaEntity with Serializable {

  override def getType: String = mediaType

  override def getMediaURLHttps: String = mediaHttps

  override def getId: Long = id.toLong

  override def getMediaURL: String = mediaUrl

  override def getSizes: java.util.Map[Integer, Size] = sizes
}
