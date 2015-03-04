name := "dbscan"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.2.0"

libraryDependencies  ++= Seq(
"org.scalanlp" %% "breeze" % "0.10",
"org.scalanlp" %% "breeze-natives" % "0.10"
)

resolvers ++= Seq(
"Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
"Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
"Akka Repository" at "http://repo.akka.io/releases/"
)

