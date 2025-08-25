package MqttFlinkProject.Connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.paho.client.mqttv3.*;
import java.nio.charset.StandardCharsets;

public class MqttSource extends RichSourceFunction<String> {

    private final String broker;
    private final String topic;
    private final String clientId;

    private transient MqttClient client;
    private volatile boolean isRunning = true;
    private transient Object lock;

    public MqttSource(String broker, String topic, String clientId) {
        this.broker = broker;
        this.topic = topic;
        this.clientId = clientId;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lock = new Object();
        client = new MqttClient(broker, clientId);
    }

    @Override
    public void run(SourceFunction.SourceContext<String> ctx) throws Exception {

        MqttConnectOptions conOpts = new MqttConnectOptions();
        conOpts.setCleanSession(false);
        conOpts.setAutomaticReconnect(true);

        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                System.err.println("MQTT connection lost: " + cause.getMessage());
                synchronized (lock) {
                    lock.notifyAll();
                }
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                String payloadString = new String(message.getPayload(), StandardCharsets.UTF_8);
                System.out.println("Message received in Flink Source: " + payloadString);
                ctx.collect(payloadString);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {}
        });

        while (isRunning) {
            try {
                if (!client.isConnected()) {
                    System.out.println("Connecting to MQTT broker...");
                    client.connect(conOpts);
                    System.out.println("Connected. Subscribing to topic: " + topic);
                    client.subscribe(topic);
                }
                synchronized (lock) {
                    lock.wait();
                }
            } catch (MqttException e) {
                System.err.println("Failed to connect or subscribe: " + e.getMessage());
                Thread.sleep(2500);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (client != null && client.isConnected()) {
            client.disconnect();
            client.close();
        }
    }
}