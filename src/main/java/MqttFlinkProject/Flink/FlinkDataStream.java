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

public class FlinkDataStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String broker = "tcp://localhost:1883";
        String sourceTopic = "sensor/input/temp";
        String sinkTopic = "sensor/output/temp";
        String sourceClientId = "flink-client-source";
        String sinkClientId = "flink-client-sink";

        DataStream<String> mqttStream = env.addSource(new MqttSource(broker, sourceTopic, sourceClientId));

        DataStream<String> processedStream = mqttStream
                .keyBy(jsonString -> {
                    ObjectMapper objMapper = new ObjectMapper();
                    JsonNode jsonNode = objMapper.readTree(jsonString);
                    return jsonNode.get("sensorId").asText();
                })
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
                            JsonNode jsonNode = objMapper.readTree(jsonString);
                            double value = jsonNode.get("value").asDouble();
                            value = (value * 1.8) + 32;

                            if (lastValue == null || value != lastValue) {
                                sum += value;
                                count++;
                                lastValue = value;
                                lastValueState.update(value);

                                System.out.println(String.format(
                                        "{\"sensorId\": \"%s\", \"valueFahrenheit\": %.1f} (processed)",
                                        key, value));
                            } else {
                                System.out.println(String.format(
                                        "{\"sensorId\": \"%s\", \"valueFahrenheit\": %.1f} (skipped duplicate)",
                                        key, value));
                            }
                        }

                        if (count > 0) {
                            double avg = sum / count;
                            out.collect(String.format(
                                    "{\"sensorId\": \"%s\", \"avgFahrenheit\": %.1f}",
                                    key, avg));
                        }
                    }
                });

        processedStream.addSink(new MqttSink(broker, sinkTopic, sinkClientId)).setParallelism(1);

        System.out.println("Flink job is running. Listening on '" + sourceTopic + "' and publishing to '" + sinkTopic + "'...");
        env.execute();
    }
}
