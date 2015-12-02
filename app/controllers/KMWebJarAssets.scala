package controllers

import play.api.http.{LazyHttpErrorHandler, HttpErrorHandler}
import play.api.mvc.Action
import play.api.mvc.AnyContent
import scala.util.matching.Regex
import play.api.{Configuration, Play}
import org.webjars.WebJarAssetLocator

import javax.inject.{ Inject, Singleton }

/**
 * A Play framework controller that is able to resolve WebJar paths.
 * <p>org.webjars.play.webJarFilterExpr can be used to declare a regex for the
 * files that should be looked for when searching within WebJars. By default
 * all files are searched for.
 */
@Singleton
class KMWebJarAssets @Inject() (errorHandler: HttpErrorHandler, configuration: Configuration) extends AssetsBuilder(errorHandler) {

  val WebjarFilterExprDefault = """.*"""
  val WebjarFilterExprProp = "org.webjars.play.webJarFilterExpr"

  val webJarFilterExpr = configuration.getString(WebjarFilterExprProp).getOrElse(WebjarFilterExprDefault)

  import play.api.Play.current
  lazy val webJarAssetLocator = new WebJarAssetLocator(
    WebJarAssetLocator.getFullPathIndex(
      new Regex(webJarFilterExpr).pattern, Play.application.classloader))

  /**
   * Controller Method to serve a WebJar asset
   *
   * @param file the file to serve
   * @return the Action that serves the file
   */
  def at(file: String): Action[AnyContent] = {
    this.at("/" + WebJarAssetLocator.WEBJARS_PATH_PREFIX, file)
  }

  /**
   * Locate a file in a WebJar
   *
   * @example Passing in `jquery.min.js` will return `jquery/1.8.2/jquery.min.js` assuming the jquery WebJar version 1.8.2 is on the classpath
   *
   * @param file the file or partial path to find
   * @return the path to the file (sans-the webjars prefix)
   *
   */
  def locate(file: String): String = {
    webJarAssetLocator.getFullPath(file).stripPrefix(WebJarAssetLocator.WEBJARS_PATH_PREFIX + "/")
  }

  /**
   * Locate a file in a WebJar
   *
   * @param webjar the WebJar artifactId
   * @param file the file or partial path to find
   * @return the path to the file (sans-the webjars prefix)
   *
   */
  def locate(webjar: String, file: String): String = {
    webJarAssetLocator.getFullPath(webjar, file).stripPrefix(WebJarAssetLocator.WEBJARS_PATH_PREFIX + "/")
  }

  /**
   * Get the full path to a file in a WebJar without validating that the file actually exists
   *
   * @example Calling fullPath("react", "react.js") will return the full path to the file in the WebJar because react.js exists at the root of the WebJar
   *
   * @param webjar the WebJar artifactId
   * @param path the full path to a file in the WebJar
   * @return the path to the file (sans-the webjars prefix)
   *
   */
  def fullPath(webjar: String, path: String): String = {
    val version = webJarAssetLocator.getWebJars.get(webjar)
    s"$webjar/$version/$path"
  }

}

object KMWebJarAssets extends KMWebJarAssets(LazyHttpErrorHandler, play.api.Play.current.configuration)
