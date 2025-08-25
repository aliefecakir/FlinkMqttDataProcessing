package MqttFlinkProject.Flink;
import MqttFlinkProject.Connector.MqttSource;
import MqttFlinkProject.Connector.MqttSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkDataStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String broker = "tcp://localhost:1883";
        String sourceTopic = "test/flink/source";
        String sinkTopic = "test/flink/sink";
        String sourceClientId = "flink-client-source";
        String sinkClientId = "flink-client-sink";

        DataStream<String> mqttStream = env.addSource(new MqttSource(broker, sourceTopic, sourceClientId));
        mqttStream.addSink(new MqttSink(broker, sinkTopic, sinkClientId)).setParallelism(1);

        System.out.println("Flink job is running. Listening on '" + sourceTopic + "' and publishing to '" + sinkTopic + "'...");
        env.execute();

    }

}
