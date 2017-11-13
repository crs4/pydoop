resolvers += "sbt-plugin-releases" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases"

addSbtPlugin("com.cavorite" % "sbt-avro-1-8" % "1.1.3")

resolvers += Resolver.sonatypeRepo("snapshots")

addSbtPlugin("org.ensime" % "sbt-ensime" % "2.0.1")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")
