import AssemblyKeys._

assemblySettings

name := "template-scala-parallel-prediction"

organization := "io.prediction"

libraryDependencies ++= Seq(
  "io.prediction"    %% "core"          % pioVersion.value % "provided",
  "org.apache.spark" %% "spark-core"    % "1.2.1" % "provided",
  "org.apache.spark" %% "spark-mllib"   % "1.2.1" % "provided",
  "org.xerial.snappy" % "snappy-java" % "1.1.1.6",
  "org.mortbay.jetty" % "servlet-api" % "3.0.20100224"
)