scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" %  "cassandra-driver-core" % "3.3.0",
  "org.slf4j"              %  "slf4j-api"             % "1.7.25",
  "org.slf4j"              %  "slf4j-simple"          % "1.7.25",
  "org.rogach"             %% "scallop"               % "2.1.1"
)



scalacOptions ++= Seq(
  "-Xfatal-warnings",
  "-Xlint",
  "-deprecation",
  "-feature",
  "-language:_",
  "-unchecked",
  "-Ywarn-unused-import"
)

//scalastyleFailOnError := true

parallelExecution in Test := false

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}

addCommandAlias("runLoad", ";runMain com.mikoff.pg.CassandraLoad")
addCommandAlias("runFetch", ";runMain com.mikoff.pg.CassandraFetch")
