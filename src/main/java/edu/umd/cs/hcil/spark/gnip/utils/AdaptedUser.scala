package edu.umd.cs.hcil.spark.gnip.utils

import java.util.Date
import collection.JavaConversions._
import com.zaubersoftware.gnip4j.api.model.Actor
import twitter4j.{RateLimitStatus, Status, URLEntity, User}

/**
  * Created by cbuntain on 9/9/16.
  */
@SerialVersionUID(100L)
class AdaptedUser(imageUrl : String, screenName : String, listedCount : Int, utcOffset : Int,
                  timezone : String, displayName : String, creationDate : Date, url : String,
                  lang : String, id : Long, desc : String, followerCount : Int, urlEntity : URLEntity,
                  verified : Boolean, friendCount : Int, statusCount : Int
                 )

  extends User with Serializable {

  override def getBiggerProfileImageURL: String = ???

  override def isProtected: Boolean = ???

  override def isTranslator: Boolean = ???

  override def getProfileLinkColor: String = ???

  override def getProfileImageURL: String = imageUrl

  override def getProfileBannerIPadRetinaURL: String = ???

  override def getMiniProfileImageURLHttps: String = ???

  override def getProfileSidebarFillColor: String = ???

  override def getScreenName: String = screenName

  override def getListedCount: Int = listedCount

  override def getOriginalProfileImageURLHttps: String = imageUrl

  override def isProfileBackgroundTiled: Boolean = ???

  override def isProfileUseBackgroundImage: Boolean = ???

  override def getUtcOffset: Int = utcOffset

  override def getProfileSidebarBorderColor: String = ???

  override def isContributorsEnabled: Boolean = ???

  override def getTimeZone: String = timezone

  override def getName: String = displayName

  override def getCreatedAt: Date = creationDate

  override def getDescriptionURLEntities: Array[URLEntity] = ???

  override def getWithheldInCountries: Array[String] = ???

  override def getURL: String = url

  override def getLang: String = lang

  override def getId: Long = id

  override def getProfileImageURLHttps: String = ???

  override def getStatus: Status = ???

  override def isDefaultProfileImage: Boolean = ???

  override def getMiniProfileImageURL: String = ???

  override def isDefaultProfile: Boolean = ???

  override def getDescription: String = desc

  override def getProfileBannerRetinaURL: String = ???

  override def getFollowersCount: Int = followerCount

  override def isGeoEnabled: Boolean = ???

  override def getURLEntity: URLEntity = urlEntity

  override def getProfileBackgroundColor: String = ???

  override def isFollowRequestSent: Boolean = ???

  override def getProfileBannerMobileURL: String = ???

  override def getFavouritesCount: Int = ???

  override def getProfileBannerURL: String = ???

  override def getProfileBackgroundImageUrlHttps: String = ???

  override def getProfileBackgroundImageURL: String = ???

  override def isVerified: Boolean = verified

  override def getLocation: String = ???

  override def getFriendsCount: Int = friendCount

  override def getProfileBannerMobileRetinaURL: String = ???

  override def getProfileTextColor: String = ???

  override def getStatusesCount: Int = statusCount

  override def isShowAllInlineMedia: Boolean = ???

  override def getProfileBannerIPadURL: String = ???

  override def getOriginalProfileImageURL: String = imageUrl

  override def getBiggerProfileImageURLHttps: String = ???

  override def compareTo(that: User): Int = {
    val delta = this.getId - that.getId

    if (delta < Integer.MIN_VALUE) {
      return Integer.MIN_VALUE
    } else if (delta > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE
    }

    return delta.toInt
  }

  override def getAccessLevel: Int = ???

  override def getRateLimitStatus: RateLimitStatus = ???

}

object AdaptedUser {

  def fromActor(actor : Actor) : AdaptedUser = {

    val imageUrl : String = actor.getImage

    val screenName : String = actor.getPreferredUsername

    val listedCount : Int = actor.getListedCount.intValue()

    val utcOffset : Int = actor.getUtcOffset

    val timeZone : String = actor.getTwitterTimeZone

    val displayName: String = actor.getDisplayName

    val createdAt : Date = ActivityStatusAdapter.gregorianCalendarToDate(actor.getPostedTime)

    val url : String = actor.getLink

    val lang: String = actor.getLanguages.head

    val userId : Long = actor.getId.replaceFirst(".*:.*:", "").toLong

    val description: String = actor.getSummary

    val followersCount: Int = actor.getFollowersCount.intValue()

    val urlEntity: URLEntity = new AdaptedUrlEntity(0, url.length-1, url, url, url, url)

    val verified: Boolean = actor.isVerified

    val friendsCount: Int = actor.getFriendsCount.intValue()

    val statusesCount: Int = actor.getStatusesCount

    return new AdaptedUser(
      imageUrl, screenName, listedCount, utcOffset,
      timeZone, displayName, createdAt, url,
      lang, userId, description, followersCount, urlEntity,
      verified, friendsCount, statusesCount
    )
  }
}