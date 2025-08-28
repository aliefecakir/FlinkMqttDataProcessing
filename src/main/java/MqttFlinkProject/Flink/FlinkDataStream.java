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
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class FlinkDataStream {

    //private static final Logger logger = LoggerFactory.getLogger(TestClass.class);
    private static final ObjectMapper objMapper = new ObjectMapper();
    static ParameterTool params;

    static {
        try {
            params = ParameterTool.fromPropertiesFile("src/main/resources/config.properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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

                                        System.out.println(String.format(
                                                "{\"sensorId\": \"%s\", \"valueFahrenheit\": %.1f} (processed)",
                                                key, fahValue));

                                    } else {
                                        System.out.println(String.format(
                                                "{\"sensorId\": \"%s\", \"value\": %.1f} (skipped duplicate)",
                                                key, value));
                                    }
                                }else {
                                    System.out.println("JSON parse error during process! Data: " + jsonString);
                                }
                            }catch(Exception e){
                                System.out.println("JSON parse error during process! Data: " + jsonString);
                            }

                        }

                        if (count > 0) {
                            double avg = sum / count;
                            String outputJson = String.format("{\"sensorId\":\"%s\", \"avgFahrenheit\": %.1f}", key, avg);
                            System.out.println("Ortalama gönderildi: " + outputJson);
                            out.collect(outputJson);
                        }
                    }
                });

        processedStream.addSink(new MqttSink(broker, sinkTopic, sinkClientId)).setParallelism(1);

        System.out.println("Flink job is running. Listening on '" + sourceTopic + "' and publishing to '" + sinkTopic + "'...");
        env.execute();
    }
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
}