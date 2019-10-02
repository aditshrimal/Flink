package connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class KafkaConnector {

    public static FlinkKafkaConsumer010<String> getKafkaConsumer(String kafkaBootstrapServers, String topicName){
        Properties kafkaProperties=new Properties();
        kafkaProperties.setProperty("bootstrap.servers", kafkaBootstrapServers);
        kafkaProperties.setProperty("group.id", topicName);
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<String>(topicName, new SimpleStringSchema(), kafkaProperties);
        return consumer;
    }
}
