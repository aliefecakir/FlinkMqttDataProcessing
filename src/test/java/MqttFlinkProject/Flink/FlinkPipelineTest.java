package MqttFlinkProject.Flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for verifying the Flink pipeline processing logic.
 *
 * <p>This test simulates a small Flink environment and a sample input stream
 * containing valid, missing, null, empty, and invalid sensorId JSONs.
 * It verifies that the Flink pipeline correctly extracts sensor IDs and handles
 * edge cases.</p>
 */
public class FlinkPipelineTest {

    private static final List<String> results = new ArrayList<>();

    /**
     * Custom test sink to capture the output of the pipeline.
     */
    private static class CollectSink implements SinkFunction<String> {
        @Override
        public synchronized void invoke(String value, Context context) {
            results.add(value);
        }
    }

    /**
     * Setup method to clear previous test results before each test.
     */
    @BeforeEach
    void setup() {
        results.clear(); // clear before each test
    }

    /**
     * Tests the Flink pipeline with sample JSON inputs.
     *
     * <p>The test verifies that:
     * <ul>
     *   <li>Valid sensorId is extracted correctly</li>
     *   <li>Empty or missing sensorId returns "UNKNOWN"</li>
     *   <li>Invalid JSON input returns "INVALID_SENSOR"</li>
     * </ul>
     * </p>
     *
     * @throws Exception if the Flink job execution fails
     */
    @Test
    void testPipeline() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.fromElements(
                "{\"sensorId\":\"sensor-1\",\"value\":25}",
                "{\"sensorId\":\"sensor-1\",\"value\":26}",
                "{\"sensorId\":\"\",\"value\":27}",
                "{\"value\":28}",
                "{\"sensorId\":null,\"value\":29}",
                "{invalid json}"
        );

        DataStream<String> keyed = input
                .map(FlinkDataStream::extractSensorId);

        keyed.addSink(new CollectSink());

        JobExecutionResult result = env.execute("Test Job");

        assertTrue(results.contains("sensor-1"));
        assertTrue(results.contains("UNKNOWN"));
        assertTrue(results.contains("INVALID_SENSOR"));
    }
}
