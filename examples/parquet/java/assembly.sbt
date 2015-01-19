import AssemblyKeys._ // put this at the top of the file

assemblySettings


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
 //   case PathList("META-INF", "maven", "org.slf4j", "slf4j-api", xs @ _*) 
//                  if xs.last endsWith "pop.xml" => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.concat
    case PathList(ps @ _*) if ps.last endsWith "pom.xml" => MergeStrategy.discard
    case x => old(x)
  }
}
