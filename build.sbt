scalaVersion := "2.11.7"

organization := "co.movio"

name := "kafka-versioned-topics"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "0.9.0.1" excludeAll(
    ExclusionRule("org.slf4j", "slf4j-log4j12")
  ),
  "args4j" % "args4j" % "2.33",
  "org.yaml" % "snakeyaml" % "1.17",
  "org.slf4j" % "slf4j-nop" % "1.7.6"
)

assemblyJarName := s"${name.value}.jar"
