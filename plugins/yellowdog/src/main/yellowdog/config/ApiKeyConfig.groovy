package yellowdog.config

import groovy.transform.CompileStatic
import nextflow.exception.AbortOperationException

import static yellowdog.config.ConfigUtils.requireString

@CompileStatic
class ApiKeyConfig {

    private String id

    private String secret

    ApiKeyConfig(Map config) {

        if (config == null) {
            throw new AbortOperationException("YellowDog plugin configuration is missing required apiKey properties")
        }

        try {
            id = requireString(config, "id")
            secret = requireString(config, "secret")
        } catch (Exception ex) {
            throw new AbortOperationException("Unable to load YellowDog API Key configuration: " + ex.message, ex)
        }
    }

    String getId() { id }

    String getSecret() { secret }
}
