package edu.umd.cs.hcil.spark.gnip.utils

import twitter4j.{GeoLocation, Place, RateLimitStatus}

/**
  * Created by cbuntain on 9/9/16.
  */
@SerialVersionUID(100L)
class AdaptedPlace(boxList : Array[Array[GeoLocation]]) extends Place with Serializable {
  override def getStreetAddress: String = ???

  override def getPlaceType: String = ???

  override def getCountry: String = ???

  override def getName: String = ???

  override def getGeometryType: String = ???

  override def getURL: String = ???

  override def getId: String = ???

  override def getBoundingBoxType: String = ???

  override def getGeometryCoordinates: Array[Array[GeoLocation]] = boxList

  override def getBoundingBoxCoordinates: Array[Array[GeoLocation]] = boxList

  override def getCountryCode: String = ???

  override def getFullName: String = ???

  override def getContainedWithIn: Array[Place] = ???

  override def compareTo(o: Place): Int = ???

  override def getAccessLevel: Int = ???

  override def getRateLimitStatus: RateLimitStatus = ???
}
