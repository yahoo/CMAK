package loader

import net.kaliber.basicAuthentication.BasicAuthenticationFilter
import play.api.GlobalSettings
import play.api.mvc.WithFilters

/**
  * Created by radu on 15/01/16.
  */
object Global extends WithFilters(BasicAuthenticationFilter()) with GlobalSettings
