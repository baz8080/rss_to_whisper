package com.rsstowhisper.web

import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import jakarta.inject.Singleton
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.thymeleaf.TemplateEngine
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver

/**
 * Produces the Thymeleaf [TemplateEngine] as a CDI bean.
 *
 * Templates are resolved from the classpath under `/templates/` with a `.html`
 * suffix — i.e. `src/main/resources/templates/search.html` is addressed as
 * `"search"` in `templateEngine.process("search", ctx)`.
 *
 * Template caching is disabled in dev mode so that Quarkus live-reload picks up
 * changes without requiring a manual cache flush. Set `app.templates.cache=true`
 * (or omit it) in production to enable caching.
 */
@ApplicationScoped
class TemplateEngineProducer {
    @ConfigProperty(name = "app.templates.cache", defaultValue = "true")
    lateinit var cacheTemplates: String

    @Produces
    @Singleton
    fun produce(): TemplateEngine {
        val resolver = ClassLoaderTemplateResolver().apply {
            prefix = "/templates/"
            suffix = ".html"
            characterEncoding = "UTF-8"
            isCacheable = cacheTemplates.toBooleanStrict()
        }
        return TemplateEngine().apply {
            setTemplateResolver(resolver)
        }
    }
}
