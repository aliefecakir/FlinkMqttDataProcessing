package MqttFlinkProject.Flink;
import MqttFlinkProject.Connector.MqttSource;
import MqttFlinkProject.Connector.MqttSink;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.io.IOException;

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
                        .process(new KeyedProcessFunction<>(){
                            private transient ValueState<Double> lastValueState;

                            @Override
                            public void open(org.apache.flink.configuration.Configuration parameters){
                                ValueStateDescriptor<Double> descriptor =
                                        new ValueStateDescriptor<>("lastValue", Double.class);
                                lastValueState = getRuntimeContext().getState(descriptor);
                            }

                            @Override
                            public void processElement(String jsonString, Context ctx, Collector<String> out) throws IOException {
                                ObjebasctMapper objMapper = new ObjectMapper();
                                JsonNode jsonNode = objMapper.readTree(jsonString);

                                double value = jsonNode.get("value").asDouble();
                                double newValue = (value*1.8) + 32;

                                Double lastValue = lastValueState.value();

                                if(lastValue == null || !lastValue.equals(newValue)) {
                                    lastValueState.update(newValue);
                                    out.collect(String.format("{\"sensorId\": \"%s\", \"valueFahrenheit\": %.1f}",
                                            jsonNode.get("sensorId").asText(), newValue));
                                }else
                                    System.out.println("This input already processed!");
                            }
                        });

        processedStream.addSink(new MqttSink(broker, sinkTopic, sinkClientId)).setParallelism(1);

        System.out.println("Flink job is running. Listening on '" + sourceTopic + "' and publishing to '" + sinkTopic + "'...");
        env.execute();
    }

}
