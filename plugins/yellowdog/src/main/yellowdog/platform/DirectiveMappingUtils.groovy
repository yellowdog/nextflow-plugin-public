package yellowdog.platform

import co.yellowdog.platform.model.CloudProvider
import co.yellowdog.platform.model.DoubleRange
import nextflow.exception.AbortOperationException
import nextflow.util.MemoryUnit

import java.time.Duration
import java.util.function.Function

class DirectiveMappingUtils {

    static <T> T getMappedValue(Object value, Function<Object, T> mapValue, String name) {
        try {
            mapValue value
        } catch (Exception ex) {
            throw new AbortOperationException(String.format("Unable to parse directive '%s' -> %s", name, ex.message), ex)
        }
    }

    static Double getDouble(Object value) {
        value instanceof Number
                ? ((Number) value).doubleValue()
                : null
    }

    static Integer getInteger(Object value) {
        value instanceof Number
                ? ((Number) value).intValue()
                : null
    }

    static Boolean getBoolean(Object value) {
        value instanceof Boolean
                ? (Boolean) value
                : null
    }

    static Duration getDuration(Object value) {
        value instanceof nextflow.util.Duration
                ? Duration.ofMillis(((nextflow.util.Duration) value).toMillis())
                : null
    }

    static DoubleRange getDoubleRangeMin(Object value, Function<Object, Double> valueMapper) {
        if (value == null) return
        DoubleRange.builder()
                .min(valueMapper.apply(value))
                .build()
    }

    static DoubleRange getDoubleRangeMin(Object value) {
        getDoubleRangeMin(value, { it as Double })
    }

    static <T> List<T> getList(Object value, Function<String, T> valueMapper) {
        if (!(value instanceof String)) return Collections.emptyList()
        value.toString().tokenize(",")
                .collect { it.trim() }
                .collect { valueMapper.apply(it) }
    }

    static List<String> getStringList(Object value) {
        getList(value, Function.identity())
    }

    static CloudProvider parseProvider(String value) {
        CloudProvider.valueOf(value.toUpperCase())
    }

    static Double parseMemoryToRam(Object value) {
        (parseMemory(value)?.toMega() / 1024.0) as Double
    }

    static MemoryUnit parseMemory(Object value) {

        if (!value)
            return null

        if (value instanceof MemoryUnit)
            return (MemoryUnit) value

        try {
            new MemoryUnit(value.toString().trim())
        }
        catch (ignored) {
            throw new AbortOperationException("Not a valid 'memory' value in process definition: $value")
        }
    }
}
