package edu.umd.cs.hcil.spark.gnip.utils

import com.zaubersoftware.gnip4j.api.impl.formats.JsonActivityFeedProcessor
import com.zaubersoftware.gnip4j.api.model.Activity

/**
  * Given a string, convert it to an Acitivty class
  * Created by cbuntain on 8/13/16.
  */
object JsonToActivity {

  val JsonFactory = JsonActivityFeedProcessor.getObjectMapper.getJsonFactory

  def jsonToStatus(line : String) : Activity = {
    try {
      val parser = JsonFactory.createJsonParser(line)
      val activity = parser.readValueAs(classOf[Activity])

      return activity
    } catch {
      case npe : NullPointerException => {
        println("NPE on line: " + line)
        return null
      }
      case e : Exception => {
        println("Error on line: " + line)
        return null
      }
    }
  }
}
