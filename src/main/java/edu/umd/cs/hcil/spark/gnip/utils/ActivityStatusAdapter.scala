package edu.umd.cs.hcil.spark.gnip.utils

import java.util
import java.util.{Date, Locale}
import javax.xml.datatype.XMLGregorianCalendar

import collection.JavaConversions._
import com.zaubersoftware.gnip4j.api.model._
import twitter4j.{GeoLocation, MediaEntity, Place, _}

/**
  * The actual Status instance
  */
class ActivityStatusAdapter(
                            boundingBox : Place,
                            createdAt : Date,
                            extendedMediaEntities : Array[ExtendedMediaEntity],
                            hashtagEntities : Array[HashtagEntity],
                            inReplyToScreenName: String,
                            retweetFlag : Boolean,
                            lang: String,
                            mediaEntities : Array[MediaEntity],
                            points : GeoLocation,
                            retweetCount: Int,
                            retweetedStatus : Status,
                            source: String,
                            statusId : Long,
                            symbolEntities : Array[SymbolEntity],
                            text : String,
                            urlEntities : Array[URLEntity],
                            user: User,
                            userMentionEntities : Array[UserMentionEntity],
                            withheldInCountries: Array[String]
  ) extends Status with Serializable {

  override def getAccessLevel: Int = ???

  override def getContributors: Array[Long] = ???

  override def getCreatedAt: Date = createdAt

  override def getCurrentUserRetweetId: Long = ???

  override def getExtendedMediaEntities: Array[ExtendedMediaEntity] = extendedMediaEntities

  override def getFavoriteCount: Int = ???

  override def getGeoLocation: GeoLocation = points

  override def getHashtagEntities: Array[HashtagEntity] = hashtagEntities

  override def getId: Long = statusId

  override def getInReplyToScreenName: String = inReplyToScreenName

  override def getInReplyToStatusId: Long = ???

  override def getInReplyToUserId: Long = ???

  override def getLang: String = lang

  override def getMediaEntities: Array[MediaEntity] = mediaEntities

  override def getPlace: Place = boundingBox

  override def getQuotedStatus: Status = ???

  override def getQuotedStatusId: Long = ???

  override def getRateLimitStatus: RateLimitStatus = ???

  override def getRetweetCount: Int = retweetCount

  override def getRetweetedStatus: Status = retweetedStatus

  override def getScopes: Scopes = ???

  override def getSource: String = source

  override def getSymbolEntities: Array[SymbolEntity] = symbolEntities

  override def getText: String = text

  override def getURLEntities: Array[URLEntity] = urlEntities

  override def getUser: User = user

  override def getUserMentionEntities: Array[UserMentionEntity] = userMentionEntities

  override def getWithheldInCountries: Array[String] = withheldInCountries

  override def isFavorited: Boolean = ???

  override def isPossiblySensitive: Boolean = ???

  override def isRetweet: Boolean = retweetFlag

  override def isRetweeted: Boolean = ???

  override def isRetweetedByMe: Boolean = ???

  override def isTruncated: Boolean = ???

  override def compareTo(that : Status): Int = {
    val delta = this.getId - that.getId

    if (delta < Integer.MIN_VALUE) {
      return Integer.MIN_VALUE
    } else if (delta > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE
    }

    return delta.toInt
  }

}

object ActivityStatusAdapter {

  // Utility functions
  val utc = java.util.TimeZone.getTimeZone("UTC")

  /**
    * Convert XML gregorian calendar to regular Java Date
    *
    * @param cal Calendar type for posted timetime
    */
  def gregorianCalendarToDate(cal : XMLGregorianCalendar) : Date = {
    return cal.toGregorianCalendar(utc, Locale.getDefault, null).getTime
  }

  /**
    * Given a tweet in Gnip's Activity, convert to Twitter's Status format
    *
    * @param activity Activity to convert
    */
  def createStatus(activity : Activity) : Status = {

    // Determine geolocation information
    var points : GeoLocation = null
    var boundingBox : Place = null

    // If we have a geo object, determine whether we set points or place
    if ( activity.getGeo != null ) {
      val geo = activity.getGeo

      if ( geo.getType == "point" || geo.getType == "Point" ) {
        val point : Point = geo.getCoordinates.asInstanceOf[Point]

        points = new GeoLocation(point.getLatitude, point.getLongitude)
      } else if ( geo.getType == "Polygon" ) {
        val polygon : Polygon = geo.getCoordinates.asInstanceOf[Polygon]


        val box : Array[GeoLocation] = polygon.getPoints.map(p => new GeoLocation(p.getLatitude, p.getLongitude)).toArray
        val boxList : Array[Array[GeoLocation]] = Array(box)

        boundingBox = new AdaptedPlace(boxList)
      }
    }

    val isRetweet : Boolean = activity.getVerb == "share"

    val createdAt : Date = gregorianCalendarToDate(activity.getPostedTime)

    val withheldInCountries: Array[String] = Array[String]()

    val user: User = AdaptedUser.fromActor(activity.getActor)

    val inReplyToScreenName: String = if ( activity.getInReplyTo != null && activity.getInReplyTo.getLink != null ) {
        val link = activity.getInReplyTo.getLink

        link.replaceFirst(".*twitter.com/", "").replaceFirst("/statuses/.*", "")
      } else { null }

    val lang: String = if ( activity.getGnip != null && activity.getGnip.getLanguage != null ) {
      activity.getGnip.getLanguage.getValue
    } else {
      "und"
    }

    val statusId = activity.getId.replaceFirst(".*:.*:", "").toLong

    val retweetCount: Int = activity.getRetweetCount.toInt

    val source: String = activity.getGenerator.getDisplayName

    val text : String = activity.getBody

    val hashtagEntities = getHashtagEntities(activity)

    val urlEntities = getURLEntities(activity)

    val symbolEntities = getSymbolEntities(activity)

    val mediaEntities = getMediaEntities(activity)

    val userMentionEntities = getUserMentionEntities(activity)

    val extendedMediaEntities = getExtendedMediaEntities(activity)

    val retweetedStatus : Status = if ( isRetweet ) {
      ActivityStatusAdapter.createStatus(activity.getObject)
    } else {
      null
    }

    val status = new ActivityStatusAdapter(
      boundingBox,
      createdAt,
      extendedMediaEntities,
      hashtagEntities,
      inReplyToScreenName,
      isRetweet,
      lang,
      mediaEntities,
      points,
      retweetCount,
      retweetedStatus,
      source,
      statusId,
      symbolEntities,
      text,
      urlEntities,
      user,
      userMentionEntities,
      withheldInCountries
    )

    return status
  }

  /**
    * Extract hashtags from Activity object
    *
    * @param activity Activity to convert
    */
  def getHashtagEntities(activity : Activity) : Array[HashtagEntity] = {
    var entityList : Array[HashtagEntity] = Array[HashtagEntity]()

    for ( obj : Hashtags <- activity.getTwitterEntities.getHashtags ) {
      val body = if ( obj.getText.startsWith("#") ) { obj.getText.drop(1) } else { obj.getText }

      val start = obj.getIndices.head
      val end = obj.getIndices.last

      val hashtagEntity : HashtagEntity = new AdaptedHashtagEntity(start, end, body)

      entityList = entityList :+ hashtagEntity
    }

    return entityList
  }

  /**
    * Extract URLs from Activity object
    *
    * @param activity Activity to convert
    */
  def getURLEntities(activity : Activity) : Array[URLEntity] = {
    var entityList : Array[URLEntity] = Array[URLEntity]()

    for ( obj : Urls <- activity.getTwitterEntities.getUrls ) {
      val body = obj.getUrl

      val start = obj.getIndices.head
      val end = obj.getIndices.last

      val entity : AdaptedUrlEntity = new AdaptedUrlEntity(start, end, body, body, obj.getExpandedUrl, obj.getDisplayUrl)

      entityList = entityList :+ entity
    }

    return entityList
  }

  /**
    * Extract media entities from Activity object
    *
    * @param activity Activity to convert
    */
  def getMediaEntities(activity : Activity) : Array[MediaEntity] = {
    var entityList : Array[MediaEntity] = Array[MediaEntity]()

    for ( obj : MediaUrls <- activity.getTwitterEntities.getMediaUrls ) {
      val body = obj.getMediaURL

      val start = obj.getIndices.head
      val end = obj.getIndices.last

      val sizeList = obj.getSizes
      val sizeMap : java.util.Map[Integer, MediaEntity.Size] = new util.HashMap[Integer, MediaEntity.Size]()

      sizeMap.put(MediaEntity.Size.SMALL, new AdaptedMediaSize(sizeList.getSmall.getHeight,
          sizeList.getSmall.getWidth,
          if ( sizeList.getSmall.getResize.equalsIgnoreCase("fit") ) { MediaEntity.Size.FIT } else { MediaEntity.Size.CROP }))

      sizeMap.put(MediaEntity.Size.MEDIUM, new AdaptedMediaSize(sizeList.getMedium.getHeight,
          sizeList.getMedium.getWidth,
          if ( sizeList.getMedium.getResize.equalsIgnoreCase("fit") ) { MediaEntity.Size.FIT } else { MediaEntity.Size.CROP }))

      sizeMap.put(MediaEntity.Size.LARGE, new AdaptedMediaSize(sizeList.getLarge.getHeight,
          sizeList.getLarge.getWidth,
          if ( sizeList.getLarge.getResize.equalsIgnoreCase("fit") ) { MediaEntity.Size.FIT } else { MediaEntity.Size.CROP }))

      //mediaUrl : String, mediaHttps : String, id : String, mediaType : String, sizes : util.Map[Integer, Size])
      val entity : AdaptedMediaEntity = new AdaptedMediaEntity(start,
        end,
        body,
        body,
        obj.getExpandedUrl,
        obj.getDisplayUrl,
        obj.getMediaURL,
        obj.getMediaURLHttps,
        obj.getId,
        obj.getType,
        sizeMap
      )

      entityList = entityList :+ entity
    }

    return entityList
  }

  /**
    * Extract user mentions from Activity object
    *
    * @param activity Activity to convert
    */
  def getUserMentionEntities(activity : Activity) : Array[UserMentionEntity] = {
    var entityList : Array[UserMentionEntity] = Array[UserMentionEntity]()

    for ( obj : UserMentions <- activity.getTwitterEntities.getUserMentions ) {
      val body = if ( obj.getScreenName.startsWith("@") ) { obj.getScreenName.drop(1) } else { obj.getScreenName }

      val start = obj.getIndices.head
      val end = obj.getIndices.last

      val userId = if ( obj.getId != null ) { obj.getId.intValue() } else { -1 }

      // start : Int, end : Int, text : String, id : Long, name : String, screenName : String
      val entity = new AdaptedUserMentionEntity(start, end, body, userId, obj.getName, body)
      entityList = entityList :+ entity
    }

    return entityList
  }


  /**
    * Extract symbols from Activity object (currently empty)
    *
    * @param activity Activity to convert
    */
  def getSymbolEntities(activity : Activity) : Array[SymbolEntity] = {

    return Array[SymbolEntity]()
  }

  /**
    * Extract extended mentia entities from Activity object (empty)
    *
    * @param activity Activity to convert
    */
  def getExtendedMediaEntities(activity : Activity) : Array[ExtendedMediaEntity] = {

    return Array[ExtendedMediaEntity]()
  }
}
