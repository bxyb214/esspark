import AssemblyKeys._

assemblySettings

name := "elasticsearchspark"

version := "0.0.1"

scalaVersion := "2.10.4"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.0",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.0",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.6.0" excludeAll(ExclusionRule(organization = "org.twitter4j")),
  "org.apache.commons" % "commons-lang3" % "3.4",
  "org.eclipse.jetty" % "jetty-client" % "9.3.7.RC0",
  "com.typesafe.play" % "play-json_2.10" % "2.4.6",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.0",
  "org.elasticsearch" % "elasticsearch-hadoop" % "2.2.0-rc1",
  "net.sf.opencsv" % "opencsv" % "2.3",
  "com.twitter.elephantbird" % "elephant-bird" % "4.12",
  "com.twitter.elephantbird" % "elephant-bird-core" % "4.12",
  "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.17",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4"
)

resolvers ++= Seq(
   "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
   "Spray Repository" at "http://repo.spray.cc/",
   "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Twitter4J Repository" at "http://twitter4j.org/maven2/",
   "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
   "conjars.org" at "http://conjars.org/repo",
   "Twitter Maven Repo" at "http://maven.twttr.com/",
   "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
   "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
   "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
   "clo jars" at "http://clojars.org/repo"
)

// temporary hack until we no longer need the customized elasticsearch-hadoop-mr
resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}