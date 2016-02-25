import java.text.SimpleDateFormat
import java.util.Date

import com.carrefour.phenix.sbt.build.gitInfo
import sbt._

object Packaging {

  val KeywordSnapshot = "-SNAPSHOT"
  val FormatDate = new SimpleDateFormat("yyyyMMddHHmm")
  val DevelopementFactoryUrl="http://udd-phenix.edc.carrefour.com/nexus/content"

  def rpmReleaseVersion(version: String, baseDirectory: File): String = {
    val end = if (version.contains(KeywordSnapshot)) {
      ".ea"
    } else {
      ".ga"
    }
    gitInfo.getRepository(baseDirectory).map(infoGit => {
      val head = Option(infoGit.resolve(gitInfo.Head)).map(what => what.name()).getOrElse(gitInfo.Undefined)
      s"${FormatDate.format(new Date())}.$head"
    }).getOrElse(gitInfo.Nogit).concat(end)
  }

  def makeVersion(version: String) = version.replace("-", ".")

  def phenixRepo(isSnapshot: Boolean) = {
    if (isSnapshot)
      Some("snapshots" at DevelopementFactoryUrl + "/repositories/snapshots")
    else
      Some("releases" at DevelopementFactoryUrl + "/repositories/releases")

  }

  //add credential if exist
  val credentialFile = Path.userHome / ".sbt" / ".credentials"

}