package yellowdog.config

import groovy.transform.CompileStatic
import nextflow.exception.IllegalConfigException
import nextflow.util.ConfigHelper
import nextflow.util.Duration

@CompileStatic
class ConfigUtils {

    static String getOrDefaultString(Map config, String property, String defaultValue) {
        getString(config, property) ?: defaultValue
    }

    static String getString(Map config, String property) {
        getObject(config, property) as String
    }

    static Integer getInteger(Map config, String property) {
        getObject(config, property) as Integer
    }

    static Double getDouble(Map config, String property) {
        getObject(config, property) as Double
    }

    static Boolean getBoolean(Map config, String property) {
        getObject(config, property) as Boolean
    }

    static Duration getDuration(Map config, String property) {
        getObject(config, property) as Duration
    }

    static Object getObject(Map config, String property) {
        ConfigHelper.parseValue(config?.get(property))
    }

    static String requireString(Map config, String property) {

        Object propertyValue = getObject(config, property)
        if (propertyValue instanceof String) {
            propertyValue as String
        } else {
            throw new IllegalConfigException("YellowDog plugin configuration is missing required property: " + property)
        }
    }
}
