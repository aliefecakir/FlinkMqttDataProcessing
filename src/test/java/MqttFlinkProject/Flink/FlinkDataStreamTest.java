package MqttFlinkProject.Flink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the {@link FlinkDataStream#extractSensorId(String)} method.
 * <p>
 * This class verifies different scenarios such as valid sensorId,
 * missing fields, null values, empty strings, and invalid JSON input.
 */
public class FlinkDataStreamTest {

    /**
     * Tests that a valid JSON with a sensorId returns the correct sensorId.
     */
    @Test
    void testExtractSensorId_Valid() {
        String json = "{\"sensorId\":\"sensor-1\",\"value\":25}";
        assertEquals("sensor-1", FlinkDataStream.extractSensorId(json));
    }

    /**
     * Tests that when the JSON is missing the sensorId field,
     * the method returns "UNKNOWN".
     */
    @Test
    void testExtractSensorId_Missing() {
        String json = "{\"value\":25}";
        assertEquals("UNKNOWN", FlinkDataStream.extractSensorId(json));
    }

    /**
     * Tests that when the sensorId is explicitly null in the JSON,
     * the method returns "UNKNOWN".
     */
    @Test
    void testExtractSensorId_Null() {
        String json = "{\"sensorId\":null,\"value\":25}";
        assertEquals("UNKNOWN", FlinkDataStream.extractSensorId(json));
    }

    /**
     * Tests that when the sensorId field is an empty string,
     * the method returns "UNKNOWN".
     */
    @Test
    void testExtractSensorId_Empty() {
        String json = "{\"sensorId\":\"\",\"value\":25}";
        assertEquals("UNKNOWN", FlinkDataStream.extractSensorId(json));
    }

    /**
     * Tests that when the input JSON is invalid,
     * the method returns "INVALID_SENSOR".
     */
    @Test
    void testExtractSensorId_InvalidJson() {
        String json = "{invalid json}";
        assertEquals("INVALID_SENSOR", FlinkDataStream.extractSensorId(json));
    }
}
