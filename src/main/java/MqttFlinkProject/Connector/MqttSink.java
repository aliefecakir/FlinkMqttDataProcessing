package MqttFlinkProject.Connector;

import MqttFlinkProject.Flink.FlinkDataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;

public class MqttSink extends RichSinkFunction<String> {
    private static final Logger logger = LoggerFactory.getLogger(MqttSink.class);
    String broker, topic, clientId;
    MqttClient client;

    public MqttSink(String broker, String topic, String clientId){
        this.broker = broker;
        this.topic = topic;
        this.clientId = clientId;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        client = new MqttClient(broker, clientId);
        MqttConnectOptions conOpts = new MqttConnectOptions();
        conOpts.setCleanSession(true);
        conOpts.setAutomaticReconnect(true);
        try {
            logger.info("Connecting sink client to broker: {} with clientId: {}",broker, clientId);
            client.connect(conOpts);
            logger.info("Sink client connected successfully.");
        } catch (MqttException e){
            logger.error("Sink failed to connect: {}",e.getMessage());
            throw new RuntimeException("Failed to connect to MQTT broker.", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null && client.isConnected()) {
            client.disconnect();
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        byte[] payload = value.getBytes(StandardCharsets.UTF_8);
        MqttMessage message = new MqttMessage(payload);
        message.setQos(1);
        if (client.isConnected()) {
            logger.info("Publishing to sink topic: {} -> {}",topic, value);
            client.publish(topic , message);
        }else
            logger.error("Client not connected, message dropped: {}",value);
    }
}