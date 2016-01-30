name := "lsh-scala"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.6"

resolvers ++= Seq(
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven",
  "Repo at github.com/ankurdave/maven-repo" at "https://github.com/ankurdave/maven-repo/raw/master"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "com.ankurdave" %% "part" % "0.1",
  "amplab" % "spark-indexedrdd" % "0.3",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)
