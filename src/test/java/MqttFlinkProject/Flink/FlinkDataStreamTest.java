package MqttFlinkProject.Flink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkDataStreamTest {

    @Test
    void testExtractSensorId_Valid() {
        String json = "{\"sensorId\":\"sensor-1\",\"value\":25}";
        assertEquals("sensor-1", FlinkDataStream.extractSensorId(json));
    }

    @Test
    void testExtractSensorId_Missing() {
        String json = "{\"value\":25}";
        assertEquals("UNKNOWN", FlinkDataStream.extractSensorId(json));
    }

    @Test
    void testExtractSensorId_Null() {
        String json = "{\"sensorId\":null,\"value\":25}";
        assertEquals("UNKNOWN", FlinkDataStream.extractSensorId(json));
    }

    @Test
    void testExtractSensorId_Empty() {
        String json = "{\"sensorId\":\"\",\"value\":25}";
        assertEquals("UNKNOWN", FlinkDataStream.extractSensorId(json));
    }

    @Test
    void testExtractSensorId_InvalidJson() {
        String json = "{invalid json}";
        assertEquals("INVALID_SENSOR", FlinkDataStream.extractSensorId(json));
    }

}
