package MqttFlinkProject.Flink;

import MqttFlinkProject.Connector.MqttSource;
import MqttFlinkProject.Connector.MqttSink;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.utils.ParameterTool;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink streaming job for processing sensor data from MQTT.
 *
 * <p>This job reads raw sensor messages from an MQTT source, extracts
 * sensor IDs, processes values in 60-second tumbling windows, converts
 * them to Fahrenheit, and publishes aggregated results back to an MQTT sink.</p>
 *
 * <p>Configuration parameters (e.g., broker address, topics, client IDs)
 * are read from <code>config.properties</code>.</p>
 */
public class FlinkDataStream {

    /**
     * Extracts the sensor ID from the given JSON string.
     *
     * @param jsonString the JSON input string
     * @return the extracted sensor ID, "UNKNOWN" if missing/empty,
     *         or "INVALID_SENSOR" if parsing fails
     */
    public static String extractSensorId(String jsonString) {
        try {
            JsonNode jsonNode = objMapper.readTree(jsonString);

            if (!jsonNode.has("sensorId") ||
                    jsonNode.get("sensorId").isNull() ||
                    jsonNode.get("sensorId").asText().trim().isEmpty()) {
                return "UNKNOWN";
            }
            return jsonNode.get("sensorId").asText();

        } catch (Exception e) {
            return "INVALID_SENSOR";
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(FlinkDataStream.class);
    private static final ObjectMapper objMapper = new ObjectMapper();
    static ParameterTool params;

    static {
        try {
            params = ParameterTool.fromPropertiesFile("src/main/resources/config.properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Main entry point of the Flink job.
     *
     * <p>The pipeline consists of:
     * <ul>
     *   <li>MQTT source: reads sensor messages</li>
     *   <li>KeyBy sensorId: groups messages by sensor</li>
     *   <li>Tumbling window (60s): aggregates readings in time windows</li>
     *   <li>Process function: filters duplicates, converts Celsius to Fahrenheit,
     *       calculates averages</li>
     *   <li>MQTT sink: publishes processed results back to another topic</li>
     * </ul>
     *
     * @param args command line arguments (not used, configs come from properties file)
     * @throws Exception if job execution fails
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String broker = params.get("mqtt.broker", "tcp://localhost:1883");
        String sourceTopic = params.get("mqtt.source.topic", "sensor/input/temp");
        String sinkTopic = params.get("mqtt.sink.topic", "sensor/output/temp");
        String sourceClientId = params.get("mqtt.source.clientId", "flink-client-source");
        String sinkClientId = params.get("mqtt.sink.clientId", "flink-client-sink");

        DataStream<String> mqttStream = env.addSource(new MqttSource(broker, sourceTopic, sourceClientId));

        DataStream<String> processedStream = mqttStream
                .keyBy(FlinkDataStream::extractSensorId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {

                    private transient ValueState<Double> lastValueState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Double> descriptor =
                                new ValueStateDescriptor<>("lastValue", Double.class);
                        lastValueState = getRuntimeContext().getState(descriptor);
                    }

                    /**
                     * Processes all elements in the given window for a specific sensor.
                     *
                     * <p>Steps:
                     * <ol>
                     *   <li>Parse each JSON message</li>
                     *   <li>Convert Celsius to Fahrenheit</li>
                     *   <li>Filter out duplicate values using state</li>
                     *   <li>Compute average Fahrenheit value per window</li>
                     *   <li>Emit result as JSON string</li>
                     * </ol>
                     *
                     * @param key      the sensor ID
                     * @param context  the window context
                     * @param elements all input messages for the window
                     * @param out      the collector for output results
                     * @throws Exception if JSON parsing or state update fails
                     */
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<String> elements,
                                        Collector<String> out) throws Exception {
                        ObjectMapper objMapper = new ObjectMapper();

                        double sum = 0;
                        int count = 0;

                        Double lastValue = lastValueState.value();

                        for (String jsonString : elements) {

                            try {
                                JsonNode jsonNode = objMapper.readTree(jsonString);
                                if(jsonNode.has("value")){
                                    double value = jsonNode.get("value").asDouble();
                                    double fahValue = (value * 1.8) + 32;
                                    if (lastValue == null || value != lastValue) {
                                        sum += fahValue;
                                        count++;
                                        lastValue = value;
                                        lastValueState.update(value);
                                        logger.info(String.format("{\"sensorId\": \"%s\", \"valueFahrenheit\": %.1f} (processed)", key, fahValue));

                                    } else {
                                        logger.info(
                                                "{\"sensorId\": \"{}\", \"value\": {}} (skipped duplicate)",
                                                key, value);
                                    }
                                } else {
                                    logger.error("JSON parse error during process! Data: {}", jsonString);
                                }
                            } catch(Exception e){
                                logger.error("JSON parse error during process! Data: {}", jsonString, e);
                            }
                        }

                        if (count > 0) {
                            double avg = sum / count;
                            String outputJson = String.format("{\"sensorId\":\"%s\", \"avgFahrenheit\": %.1f}", key, avg);
                            logger.info("Average sent: {}", outputJson);
                            out.collect(outputJson);
                        }
                    }
                });

        processedStream.addSink(new MqttSink(broker, sinkTopic, sinkClientId)).setParallelism(1);
        logger.info("Flink job is running. Listening on '{}' and publishing to '{}'", sourceTopic, sinkTopic);
        env.execute();
    }
}
