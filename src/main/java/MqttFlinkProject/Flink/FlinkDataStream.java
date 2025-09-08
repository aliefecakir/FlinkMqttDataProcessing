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
import java.net.*;
import java.io.FileWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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
    public static void tempMessage(String url, String message, HttpClient httpClient) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Title","High Temperature Warning!")
                .header("Priority", "urgent")
                .header("Tags","fire")
                .POST(HttpRequest.BodyPublishers.ofString(message))
                .build();

        httpClient.send(request, HttpResponse.BodyHandlers.discarding());
    }
    public static void prsMessage(String url, String message, HttpClient httpClient) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Title","High Pressure Warning!")
                .header("Priority", "urgent")
                .header("Tags","warning")
                .POST(HttpRequest.BodyPublishers.ofString(message))
                .build();

        httpClient.send(request, HttpResponse.BodyHandlers.discarding());
    }
    public static void humMessage(String url, String message, HttpClient httpClient) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Title","High Humidity Warning!")
                .header("Priority", "urgent")
                .header("Tags","droplet")
                .POST(HttpRequest.BodyPublishers.ofString(message))
                .build();

        httpClient.send(request, HttpResponse.BodyHandlers.discarding());
    }

    public static void writeToTxt(String message, File file) throws IOException {
        FileWriter fileWriter = new FileWriter(file.getAbsolutePath(), true);

        if(file.createNewFile()){
            logger.info(file.getName() + " created successfully.");}
        else{
            logger.info(file.getName() + " already created!");}

        fileWriter.write(message);
        fileWriter.close();
        logger.info("The warning written to " + file.getName());
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
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    private transient HttpClient httpClient;
                    private transient ValueState<Double> lastValueState;
                    private transient Connection conn;

                    @Override
                    public void open(Configuration parameters) throws SQLException {
                        httpClient = HttpClient.newHttpClient();
                        ValueStateDescriptor<Double> descriptor =
                                new ValueStateDescriptor<>("lastValue", Double.class);
                        lastValueState = getRuntimeContext().getState(descriptor);

                        String url = "jdbc:postgresql://localhost:5432/sensor_output_db";
                        String user = "postgres";
                        String password = "299464960";

                        try {
                            conn = DriverManager.getConnection(url, user, password);
                            logger.info("PostgreSQL connection established successfully!");
                        } catch (SQLException e) {
                            logger.error("Failed to connect to PostgreSQL!", e);
                            throw e;
                        }
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
                     * @param Id      the sensor ID
                     * @param context  the window context
                     * @param elements all input messages for the window
                     * @param out      the collector for output results
                     * @throws Exception if JSON parsing or state update fails
                     */
                    @Override
                    public void process(String Id,
                                        Context context,
                                        Iterable<String> elements,
                                        Collector<String> out) throws Exception {
                        ObjectMapper objMapper = new ObjectMapper();

                        double temperature, prs, sum = 0, fahTemp = 0, atm = 0;
                        int hum = 0, count = 0;

                        Double lastValue = lastValueState.value();

                        for (String jsonString : elements) {
                            try {
                                JsonNode jsonNode = objMapper.readTree(jsonString);
                                if (jsonNode.has("temperature") && jsonNode.has("pressure") && jsonNode.has("humidity")) {
                                    temperature = jsonNode.get("temperature").asDouble();
                                    fahTemp = (temperature * 1.8) + 32;
                                    prs = jsonNode.get("pressure").asDouble();
                                    atm = (prs / 101325);
                                    atm = Math.round(atm * 100.0) / 100.0;
                                    hum = jsonNode.get("humidity").asInt();

                                    if (lastValue == null || temperature != lastValue) {
                                        sum += fahTemp;
                                        count++;
                                        lastValue = temperature;
                                        lastValueState.update(temperature);

                                        logger.info(String.format(
                                                "{\"Sensor Id\": \"%s\", \"Temperature(°F)\": %.1f, \"Humidity\": %%%d, \"Pressure(atm)\": %.2f} (processed)",
                                                Id, fahTemp, hum, atm));

                                        File txtWarn = new File("C:\\Users\\Ally\\IdeaProjects\\MqttFlinkPipeline\\src\\main\\java\\MqttFlinkProject\\Flink\\TempWarns.txt");
                                        LocalDateTime now = LocalDateTime.now();
                                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                                        String formattedDateTime = now.format(formatter);

                                        String tempWarn = "The temperature of the sensor with ID '" + Id + "' is above 45 °C! (" +
                                                String.format("%.1f", fahTemp) + " °F) - " + formattedDateTime + "\n";
                                        String prsWarn = "The pressure of the sensor with ID '" + Id + "' is above 1.15 atm! (" +
                                                String.format("%.2f", atm) + " atm) - " + formattedDateTime + "\n";
                                        String humWarn = "The humidity of the sensor with ID '" + Id + "' is above %70! (%" +
                                                hum + ") - " + formattedDateTime + "\n";
                                        String insertSql = "INSERT INTO sensor_alerts(sensor_id, alert_type, value) VALUES (?, ?, ?)";

                                        String ntfyUrl = "https://ntfy.sh/sensor_temp_warn";

                                        try (PreparedStatement stmt = conn.prepareStatement(insertSql)){
                                            if (temperature > 45) {
                                                logger.error("The temperature of the sensor with ID '{}' is above 45 °C! ({} °F)", Id, String.format("%.1f", fahTemp));
                                                tempMessage(ntfyUrl, tempWarn, httpClient);
                                                writeToTxt(tempWarn, txtWarn);
                                                stmt.setString(1, Id);
                                                stmt.setString(2, "temperature");
                                                stmt.setDouble(3, temperature);
                                                int rows = stmt.executeUpdate();
                                                if (rows > 0) {
                                                    logger.info("Temperature alert for sensor '{}' successfully written to DB.", Id);
                                                }
                                            } if (atm > 1.15) {
                                                logger.error("The pressure of the sensor with ID '{}' is above 1.15 atm! ({} atm)", Id, String.format("%.2f", atm));
                                                prsMessage(ntfyUrl, prsWarn, httpClient);
                                                writeToTxt(prsWarn, txtWarn);
                                                stmt.setString(1, Id);
                                                stmt.setString(2, "pressure"); // veya pressure / humidity
                                                stmt.setDouble(3, atm);      // senin oluşturduğun mesaj
                                                int rows = stmt.executeUpdate();
                                                if (rows > 0) {
                                                    logger.info("Pressure alert for sensor '{}' successfully written to DB.", Id);
                                                }
                                            } if (hum > 70) {
                                                logger.error("The humidity of the sensor with ID '{}' is above %70 (%{})", Id, hum);
                                                humMessage(ntfyUrl, humWarn, httpClient);
                                                writeToTxt(humWarn, txtWarn);
                                                stmt.setString(1, Id);
                                                stmt.setString(2, "humidity"); // veya pressure / humidity
                                                stmt.setInt(3, hum);      // senin oluşturduğun mesaj
                                                int rows = stmt.executeUpdate();
                                                if (rows > 0) {
                                                    logger.info("Humidity alert for sensor '{}' successfully written to DB.", Id);
                                                }
                                            }
                                        } catch (Exception e) {
                                            logger.error("An error has occured!", e);
                                        }

                                    } else {
                                        logger.info(String.format(
                                                "{\"Sensor Id\": \"%s\", \"Temperature(°F)\": %.1f, \"Humidity\": %%%d, \"Pressure(atm)\": %.2f} (skipped duplicate)",
                                                Id, fahTemp, hum, atm));
                                    }

                                } else {
                                    logger.error("JSON parse error during process! Data: {}", jsonString);
                                }
                            } catch (Exception e) {
                                logger.error("JSON parse error during process! Data: {}", jsonString, e);
                            }
                        }

                        if (count > 0) {
                            double avg = sum / count;
                            String outputJson = String.format(
                                    "{\"Sensor Id\": \"%s\", \"Average Temperature(°F)\": %.1f}",
                                    Id, avg);
                            logger.info("Average sent: {}", outputJson);
                            out.collect(outputJson);
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        if (conn != null && !conn.isClosed()) {
                            conn.close();
                            logger.info("PostgreSQL connection closed.");
                        }
                    }
                });

        processedStream.addSink(new MqttSink(broker, sinkTopic, sinkClientId)).setParallelism(1);
        logger.info("Flink job is running. Listening on '{}' and publishing to '{}'", sourceTopic, sinkTopic);
        env.execute();
    }
}