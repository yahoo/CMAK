/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
import play.twirl.api.Html

package object features {
  val empty: Html = Html("")
  
  def app(f: ApplicationFeature)(content: Html)(implicit af: ApplicationFeatures): Html = {
    if(af.features(f))
      content
    else
      empty
  }

}
