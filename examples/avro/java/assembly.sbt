assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith "pom.properties" =>
    MergeStrategy.concat
  case PathList(ps @ _*) if ps.last endsWith "pom.xml" =>
    MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
