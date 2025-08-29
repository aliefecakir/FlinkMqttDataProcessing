package MqttFlinkProject.Connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;

/**
 * A custom Flink source function for consuming data from an MQTT broker.
 *
 * <p>This source connects to the specified MQTT broker, subscribes to
 * a given topic, and forwards incoming messages to the Flink data stream.</p>
 */
public class MqttSource extends RichSourceFunction<String> {
    private static final Logger logger = LoggerFactory.getLogger(MqttSource.class);
    private final String broker;
    private final String topic;
    private final String clientId;
    private transient MqttClient client;
    private volatile boolean isRunning = true;
    private transient Object lock;

    /**
     * Creates a new MQTT source.
     *
     * @param broker   the MQTT broker URL (tcp://localhost:1883)
     * @param topic    the MQTT topic to subscribe to
     * @param clientId the unique client identifier for the MQTT connection
     */
    public MqttSource(String broker, String topic, String clientId) {
        this.broker = broker;
        this.topic = topic;
        this.clientId = clientId;
    }

    /**
     * Initializes the MQTT client and prepares resources before the source starts.
     *
     * @param parameters the Flink configuration parameters
     * @throws Exception if the client cannot be created
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lock = new Object();
        client = new MqttClient(broker, clientId);
    }

    /**
     * Main execution method for the source. Connects to the MQTT broker,
     * subscribes to the given topic, and continuously emits received messages
     * into the Flink pipeline.
     *
     * @param ctx the context used to emit elements
     * @throws Exception if the client fails to connect or subscribe
     */
    @Override
    public void run(SourceFunction.SourceContext<String> ctx) throws Exception {

        MqttConnectOptions conOpts = new MqttConnectOptions();
        conOpts.setCleanSession(false);
        conOpts.setAutomaticReconnect(true);

        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                logger.error("MQTT connection lost: {}", cause.getMessage());
                synchronized (lock) {
                    lock.notifyAll();
                }
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String payloadString = new String(message.getPayload(), StandardCharsets.UTF_8);
                logger.info("Message received in Flink Source: {}", payloadString);
                ctx.collect(payloadString);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {}
        });

        while (isRunning) {
            try {
                if (!client.isConnected()) {
                    logger.info("Connecting to MQTT broker...");
                    client.connect(conOpts);
                    logger.info("Connected. Subscribing to topic: {}", topic);
                    client.subscribe(topic);
                }
                synchronized (lock) {
                    lock.wait();
                }
            } catch (MqttException e) {
                logger.error("Failed to connect or subscribe: {}", e.getMessage());
                Thread.sleep(2500);
            }
        }
    }

    /**
     * Cancels the source and signals it to stop execution.
     */
    @Override
    public void cancel() {
        isRunning = false;
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    /**
     * Closes the MQTT connection and releases resources when the source is disposed.
     *
     * @throws Exception if disconnection fails
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (client != null && client.isConnected()) {
            client.disconnect();
            client.close();
        }
    }
}
