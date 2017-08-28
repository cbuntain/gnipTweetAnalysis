package edu.umd.cs.hcil.spark.gnip

import java.io.{File, FileWriter}

import org.apache.commons.lang3.StringEscapeUtils
import com.zaubersoftware.gnip4j.api.model.Activity
import edu.umd.cs.hcil.spark.gnip.utils.JsonToActivity
import it.uniroma1.dis.wsngroup.gexf4j.core.data.{AttributeClass, AttributeType}
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.{GexfImpl, StaxGraphWriter}
import it.uniroma1.dis.wsngroup.gexf4j.core.{EdgeType, Mode, Node}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by cbuntain on 8/14/16.
  */
object TweetsToGraph {

  case class TwitterUser(id : Long, name : String)

  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("User Network")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)

    val twitterMsgsRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + twitterMsgsRaw.partitions.size)

    // Repartition if desired using the new partition count
    var twitterMsgs = twitterMsgsRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      twitterMsgs = twitterMsgsRaw.repartition(initialPartitions)
      println("New Partition Count: " + twitterMsgs.partitions.size)
    }
    val newPartitionSize = twitterMsgs.partitions.size

    // Convert each JSON line in the file to a status using Gnip4j
    //  Note that not all lines are Activity lines, so we catch any exception
    //  generated during this conversion and set to null since we don't care
    //  about non-status lines.
    val tweets = twitterMsgs
      .filter(line => line.length > 0)
      .map(line => JsonToActivity.jsonToStatus(line))
      .filter(activity => activity != null)

    // Create the graphx graph and the collect the users and edges
    val graph = buildGraphxGraph(tweets)

    // Create the GEXF-formatted graph
    val gexf = buildGexfGraph(graph)

    // Now write to disk
    writeGexfGraph(gexf, outputPath)
  }

  /**
    * Given an RDD of activities, build the graph and return a GEXF-formatted object
    * @param gexf Gexf model
    * @param outputPath Where to save the data
    */
  def writeGexfGraph(gexf : GexfImpl, outputPath : String) : Unit = {
    // Write the file to disk
//    try {
//      val graphWriter = new StaxGraphWriter()
//      val outFile = new File(outputPath)
//      val fileWriter = new FileWriter(outFile, false)
//
//      graphWriter.writeToStream(gexf, fileWriter, "UTF-8")
//    } catch {
//      case e : Throwable => println("Caught exception:" + e.getMessage)
//    }

    val graphWriter = new StaxGraphWriter()
    val outFile = new File(outputPath)
    val fileWriter = new FileWriter(outFile, false)

    graphWriter.writeToStream(gexf, fileWriter, "UTF-8")

  }

  /**
    * Given an RDD of activities, build the graph and return a GEXF-formatted object
    * @param tweets RDD of activities
    * @return GEXF model
    */
  def buildGexfGraph(tweets : RDD[Activity]) : GexfImpl = {
    val graph = buildGraphxGraph(tweets)
    return buildGexfGraph(graph)
  }

  /**
    * Given an RDD of activities, build the graph and return a GEXF-formatted object
    * @param graph GraphX graph of activities
    * @return GEXF model
    */
  def buildGexfGraph(graph : Graph[TwitterUser, Int]) : GexfImpl = {

    val userMap = graph.vertices.collectAsMap()
    val edges = graph.edges.collect()

    // Construct the gexf graph
    val gexf = new GexfImpl()

    val gexfGraph = gexf.getGraph()
    gexfGraph.setDefaultEdgeType(EdgeType.DIRECTED).setMode(Mode.STATIC)

    val nodeAttributes = new AttributeListImpl(AttributeClass.NODE)
    val nodeAttrId = nodeAttributes.createAttribute("0", AttributeType.LONG, "userId")
    val nodeAttrName = nodeAttributes.createAttribute("1", AttributeType.STRING, "screenname")
    gexfGraph.getAttributeLists().add(nodeAttributes)

    // Add all the nodes
    var nodeMap = Map[Long,Node]()
    for ( userTuple <- userMap.toList ) {
      val nodeId = userTuple._1
      val user = userTuple._2

      val node = gexfGraph.createNode(nodeId.toString)
      node.setLabel(user.name)
        .getAttributeValues
        .addValue(nodeAttrId, user.id.toString)
        .addValue(nodeAttrName, user.name)

      nodeMap = nodeMap + (nodeId -> node)
    }

    // Add the edges
    for ( edge <- edges ) {
      val srcId = edge.srcId.toLong
      val dstId = edge.dstId.toLong
      val weight = edge.attr

      val srcNode = nodeMap(srcId)
      val dstNode = nodeMap(dstId)

      val srcName = srcNode.getAttributeValues.get(1).getValue
      val dstName = dstNode.getAttributeValues.get(1).getValue

      val newEdge = srcNode.connectTo(edge.hashCode().toString, "mentions", EdgeType.DIRECTED, dstNode)
      newEdge.setWeight(weight)
    }

    return gexf
  }


  /**
    * Given an RDD of activities, build the GraphX model
    * @param tweets RDD of activities
    * @return GraphX graph model
    */
  def buildGraphxGraph(tweets : RDD[Activity]) : Graph[TwitterUser, Int] = {
    return buildGraphxGraph(tweets, 0)
  }

  /**
    * Given an RDD of activities, build the GraphX model
    * @param tweets RDD of activities
    * @param minDegree Minimum degree for a node to be included
    * @return GraphX graph model
    */
  def buildGraphxGraph(tweets : RDD[Activity], minDegree : Int) : Graph[TwitterUser, Int] = {

    val userMentionMap = tweets.filter(activity => activity.getActor != null).flatMap(activity => {
      val authorGnipId : String = activity.getActor.getId
      val authorTwitterId = authorGnipId.replace("id:twitter.com:", "").toLong
      val authorName = StringEscapeUtils.escapeXml11(activity.getActor.getPreferredUsername)

      val author = TwitterUser(authorTwitterId, authorName)

      activity.getTwitterEntities.getUserMentions
        .filter(entity => { entity.getIdStr != null && entity.getIdStr.length > 0 })
        .map(entity => {
          val otherUserId = entity.getIdStr
          val otherUserIdLong = otherUserId.toLong
          val otherScreenname = StringEscapeUtils.escapeXml11(entity.getScreenName)

          val mentionedUser = TwitterUser(otherUserIdLong, otherScreenname)

          ("%d,%d".format(author.id, mentionedUser.id), (author, mentionedUser, 1))
      })
    }).reduceByKey((l, r) => {
      val src = l._1
      val dst = l._2

      (src, dst, l._3 + r._3)
    })

    val users = userMentionMap.values.flatMap(tuple => {
      val src : TwitterUser = tuple._1
      val dst : TwitterUser = tuple._2
      Array((src.id, src), (dst.id, dst))
    })

    val edges : RDD[Edge[Int]] = userMentionMap.values.map(tuple => {
      val src = tuple._1
      val dst = tuple._2
      val weight : Int = tuple._3

      Edge(src.id, dst.id, weight)
    })

    val graph = Graph(users, edges)
    val degreeGraph = graph.outerJoinVertices(graph.degrees) { (id, oldAttr, degreeOpt) =>
      degreeOpt match {
        case Some(degree) => (oldAttr, degree)
        case None => (oldAttr, 0) // No degreeOpt means zero degree
      }
    }

    val subgraphWithDegree = degreeGraph.subgraph(vpred = (id, attr) => attr._2 > minDegree)
    val trimmedSub = subgraphWithDegree.mapVertices((id, attr) => attr._1)

    return trimmedSub
  }
}
