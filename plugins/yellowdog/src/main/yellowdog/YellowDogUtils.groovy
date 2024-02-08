package yellowdog

import groovy.transform.CompileStatic

import static co.yellowdog.platform.model.Constants.MAX_NAME_LENGTH

@CompileStatic
class YellowDogUtils {

    static String sanitizeName(String name) {

        String sanitized = name
                .replaceAll("([a-z0-9])([A-Z][a-z0-9])", "\$1_\$2")
                .toLowerCase()
                .replaceAll("[^a-z\\d_-]", "_")
                .replaceAll("_{2,}", "_")
                .replaceAll("^[^a-z]", "")

        if (sanitized.length() > MAX_NAME_LENGTH) {
            sanitized = sanitized.substring(0, MAX_NAME_LENGTH)
        }

        sanitized.replaceAll("[^a-z\\d]+\$", "")
    }
}
