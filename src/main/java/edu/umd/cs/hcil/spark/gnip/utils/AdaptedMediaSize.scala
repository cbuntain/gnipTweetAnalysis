package edu.umd.cs.hcil.spark.gnip.utils

import twitter4j.MediaEntity

/**
  * Created by cbuntain on 9/9/16.
  */
@SerialVersionUID(100L)
class AdaptedMediaSize (height : Int, width : Int, resize : Int) extends MediaEntity.Size with Serializable {
  override def getHeight: Int = height

  override def getWidth: Int = width

  override def getResize: Int = resize
}
