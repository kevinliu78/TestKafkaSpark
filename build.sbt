lazy val root = (project in file(".")).
settings(
name := "hello",
version := "1.0",
scalaVersion := "2.11.8",
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0",
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0",
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.1.0",
libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.8.2.1" % "provided",
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.1" % "provided",
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0" % "provided",
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-assembly_2.11" % "1.5.1",
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10-assembly_2.11" % "2.3.0"
)