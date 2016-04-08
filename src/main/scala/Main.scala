import java.io.FileInputStream
import java.util.{HashMap, Map ⇒ JMap}
import java.util.Properties
import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.collection.mutable

import kafka.admin.AdminUtils
import kafka.api.PartitionMetadata
import kafka.api.TopicMetadata
import kafka.common.ErrorMapping
import kafka.utils.ZkUtils
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j
//import org.kohsuke.args4j.Option
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

object Main extends App {
  @args4j.Option(name = "-config", required = true, usage = "Topic configuration file")
  var file: String = null
  @args4j.Option(name = "-zk", required = true, usage = "ZooKeeper connection string")
  var zookeeper: String = null
  @args4j.Option(name = "-version", required = true, usage = "Topic version")
  var version: String = null
  @args4j.Option(name = "-for_real", usage = "Enables creation and deletion of topics")
  var unlock: Boolean = false
  @Argument(required = true, metaVar = "COMMAND", index = 0)
  var command: Command = null

  val parser = new CmdLineParser(this)
  try {
    parser.parseArgument(args.toList.asJava)
  } catch {
    case e: CmdLineException ⇒
      println(s"${e.getMessage}")
      parser.printSingleLineUsage(System.err)
      println()
      parser.printUsage(System.err)
      System.exit(1)
  }

  val yaml = new Yaml(new Constructor(classOf[Config]))
  val config = yaml.load(new FileInputStream(file)).asInstanceOf[Config]
  val zkUtils = ZkUtils(zookeeper, 6000, 6000, false)

  val topicConfigs = AdminUtils.fetchAllTopicConfigs(zkUtils)
  def retrieveConfig(topic: String): Map[String, String] = {
    topicConfigs.get(topic) match {
      case Some(props) ⇒
        props.stringPropertyNames.asScala.map(key ⇒ (key, props.getProperty(key))).toMap
      case None ⇒
        Map.empty
    }
  }

  val C_RESET = "\u001b[0m"
  val C_RED = "\u001b[31m"
  val C_GREEN = "\u001b[32m"
  val C_YELLOW = "\u001b[33m"
  val C_BLUE = "\u001b[34m"

  command match {
    case Command.status ⇒
      renderTopics(retrieveTopics())

    case Command.create ⇒
      val topics = retrieveTopics()
      val topicsToCreate = topics.filter {
        case (_, Left(error)) ⇒ error == classOf[UnknownTopicOrPartitionException].getSimpleName
        case _ ⇒ false
      }

      if (unlock)
        println(s"${C_BLUE}Creating topics:$C_RESET")
      else
        println(s"${C_BLUE}Would create topics:$C_RESET")

      topicsToCreate.keys.toList.sorted foreach { name ⇒
        val topic = config.topicForVersionedName(version, name).get
        print(s"-> $name")

        if (unlock) {
          print("... ")
          val props = new Properties()
          props.putAll(topic.config)
          AdminUtils.createTopic(zkUtils, name, topic.partitions, topic.replicas, props)
          print("done")
        }

        println()
      }

      if (topicsToCreate.size == 0)
        println(s"${C_GREEN}Nothing to create!$C_RESET")

      val otherTopics = topics.keySet diff topicsToCreate.keySet
      if (unlock)
        println(s"${C_BLUE}Not creating topics:$C_RESET")
      else
        println(s"${C_BLUE}Would leave topics:$C_RESET")
      otherTopics foreach (t ⇒ println(s"-> $t"))

    case Command.delete ⇒
      val topics = retrieveTopics()
      val topicsToDelete = topics.filter {
        case (name, Right(topic)) ⇒ Some(topic) == config.topicForVersionedName(version, name)
        case _ ⇒ false
      }

      if (unlock)
        println(s"${C_BLUE}Deleting topics:$C_RESET")
      else
        println(s"${C_BLUE}Would delete topics:$C_RESET")

      topicsToDelete.keys.toList.sorted foreach { name ⇒
        print(s"-> $name")

        if (unlock) {
          print("... ")
          AdminUtils.deleteTopic(zkUtils, name)
          print("done")
        }

        println()
      }

      if (topicsToDelete.size == 0)
        println(s"${C_GREEN}Nothing to delete!$C_RESET")

      val otherTopics = topics.keySet diff topicsToDelete.keySet
      if (unlock)
        println(s"${C_BLUE}Not deleting topics:$C_RESET")
      else
        println(s"${C_BLUE}Would leave topics:$C_RESET")
      otherTopics foreach (t ⇒ println(s"-> $t"))
  }

  private def renderTopics(topics: Map[String, Either[String, Topic]]): Unit =
    topics.keys.toList.sorted foreach { name ⇒
      topics(name) match {
        case Right(topic) ⇒
          val target = config.topicForVersionedName(version, name)
          val colour = if (target == Some(topic)) C_GREEN else C_RED
          println(s"$colour$name:$C_RESET")
          println(s"  current: $topic")
          println(s"  target:  ${target.getOrElse("-")}")
        case Left(error) ⇒
          val target = config.topicForVersionedName(version, name)
          val colour = if (error == classOf[UnknownTopicOrPartitionException].getSimpleName) C_YELLOW else C_RED
          println(s"$colour$name:$C_RESET")
          println(s"  current: $error")
          println(s"  target:  ${target.getOrElse("-")}")
      }
    }

  private def retrieveTopics(): Map[String, Either[String, Topic]] = {
    val metadata = AdminUtils.fetchTopicMetadataFromZk(config.versionedTopics(version), zkUtils)
    metadata.map { topic ⇒
      topic.errorCode match {
        case ErrorMapping.NoError ⇒
          val partitions = topic.partitionsMetadata.size
          val replicas = topic.partitionsMetadata.map(_.replicas.size).distinct
          val configs = retrieveConfig(topic.topic)
          val out = Topic(partitions, replicas.max, configs.asJava)
          out.alternateReplicas ++= replicas
          (topic.topic, Right(out))
        case _ ⇒
          (topic.topic, Left(ErrorMapping.exceptionFor(topic.errorCode).getClass.getSimpleName))
      }
    }.toMap
  }
}

case class Config(
  @BeanProperty var topics: JMap[String, Topic]
) {
  def this() = this(new HashMap[String, Topic]())

  def versionedTopics(version: String): Set[String] =
    topics.keySet().asScala.toSet map versionedTopic(version)

  def topicForVersionedName(version: String, versionedName: String): Option[Topic] =
    topics.asScala.collectFirst {
      case (name, topic) if versionedTopic(version)(name) == versionedName ⇒ topic
    }

  private def versionedTopic(version: String)(topic: String): String =
    topic.replaceAll("""\$VERSION""", version)
}

case class Topic(
  @BeanProperty var partitions: Int,
  @BeanProperty var replicas: Int,
  @BeanProperty var config: JMap[String, String]
) {
  def this() = this(1, 1, new HashMap[String, String]())
  val alternateReplicas: mutable.Set[Int] = mutable.Set.empty

  override def toString(): String = {
    val maybeInconsistent =
      if (alternateReplicas.size > 1)
        s" (inconsistent! found {${alternateReplicas.mkString(",")}})"
      else
        ""
    val configs = config.asScala.map { case (key, value) ⇒ s"$key=$value" }
    s"$partitions partitions, $replicas replicas$maybeInconsistent [${configs.mkString(",")}]"
  }
}
