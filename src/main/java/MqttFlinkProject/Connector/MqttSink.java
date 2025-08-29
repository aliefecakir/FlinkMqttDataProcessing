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

/**
 * A custom Flink sink function for publishing data to an MQTT broker.
 *
 * <p>This sink connects to the specified MQTT broker and publishes
 * incoming records as messages to a given MQTT topic.</p>
 */
public class MqttSink extends RichSinkFunction<String> {
    private static final Logger logger = LoggerFactory.getLogger(MqttSink.class);
    private String broker, topic, clientId;
    private MqttClient client;

    /**
     * Creates a new MQTT sink.
     *
     * @param broker   the MQTT broker URL (e.g., tcp://localhost:1883)
     * @param topic    the MQTT topic to publish messages to
     * @param clientId the unique client identifier for the MQTT connection
     */
    public MqttSink(String broker, String topic, String clientId){
        this.broker = broker;
        this.topic = topic;
        this.clientId = clientId;
    }

    /**
     * Opens the MQTT connection when the sink is initialized.
     *
     * @param parameters the Flink configuration parameters
     * @throws Exception if the connection to the broker cannot be established
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        client = new MqttClient(broker, clientId);
        MqttConnectOptions conOpts = new MqttConnectOptions();
        conOpts.setCleanSession(true);
        conOpts.setAutomaticReconnect(true);
        try {
            logger.info("Connecting sink client to broker: {} with clientId: {}", broker, clientId);
            client.connect(conOpts);
            logger.info("Sink client connected successfully.");
        } catch (MqttException e){
            logger.error("Sink failed to connect: {}", e.getMessage());
            throw new RuntimeException("Failed to connect to MQTT broker.", e);
        }
    }

    /**
     * Closes the MQTT connection when the sink is disposed.
     *
     * @throws Exception if disconnection fails
     */
    @Override
    public void close() throws Exception {
        if (client != null && client.isConnected()) {
            client.disconnect();
        }
    }

    /**
     * Publishes the given value as an MQTT message.
     *
     * @param value   the message payload as a String
     * @param context the context for the current sink invocation
     * @throws Exception if publishing fails
     */
    @Override
    public void invoke(String value, Context context) throws Exception {
        byte[] payload = value.getBytes(StandardCharsets.UTF_8);
        MqttMessage message = new MqttMessage(payload);
        message.setQos(1);
        if (client.isConnected()) {
            logger.info("Publishing to sink topic: {} -> {}", topic, value);
            client.publish(topic , message);
        } else {
            logger.error("Client not connected, message dropped: {}", value);
        }
    }
}
