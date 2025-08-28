package MqttFlinkProject.Flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlinkPipelineTest {

    private static final List<String> results = new ArrayList<>();

    // Custom test sink to capture output
    private static class CollectSink implements SinkFunction<String> {
        @Override
        public synchronized void invoke(String value, Context context) {
            results.add(value);
        }
    }

    @BeforeEach
    void setup() {
        results.clear(); // clear before each test
    }

    @Test
    void testPipeline() throws Exception {
        // Create test Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Fake input stream
        DataStream<String> input = env.fromElements(
                "{\"sensorId\":\"sensor-1\",\"value\":25}",
                "{\"sensorId\":\"sensor-1\",\"value\":26}",
                "{\"sensorId\":\"\",\"value\":27}",       // empty -> UNKNOWN
                "{\"value\":28}",                         // missing -> UNKNOWN
                "{\"sensorId\":null,\"value\":29}",       // null -> UNKNOWN
                "{invalid json}"                          // invalid -> INVALID_SENSOR
        );

        // Use your extractSensorId
        DataStream<String> keyed = input
                .map(FlinkDataStream::extractSensorId); // now outputs the extracted keys


        // Just send it directly to sink for test (no windowing to keep it simple)
        keyed.addSink(new CollectSink());

        JobExecutionResult result = env.execute("Test Job");

        // Assertions
        assertTrue(results.contains("sensor-1"));
        assertTrue(results.contains("UNKNOWN"));
        assertTrue(results.contains("INVALID_SENSOR"));
    }
}
