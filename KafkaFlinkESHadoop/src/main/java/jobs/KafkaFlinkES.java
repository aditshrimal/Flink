package jobs;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import connectors.ESConnector;
import connectors.KafkaConnector;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import java.util.List;


public class KafkaFlinkES {
    private static final JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {

        try {

            //Flink Stream Execution Environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //Getting Kafka Consumer
            FlinkKafkaConsumer010<String> consumer = KafkaConnector.getKafkaConsumer("kafka.bootstrap.servers.ip", "topic_name");

            //Getting Elasticsearch IPs (comma seperated if multiple IPs)
            List<HttpHost> httpHosts = ESConnector.getESHttpHosts("es.cluster.ip");

            DataStream<String> input = env.addSource(consumer) //Input source is kafka consumer
                    .setParallelism(1)                         // I have set parallelism as 1, you can change as per your requirement
                    .name("KafkaSource")
                    .rebalance();

            DataStream<String> inputJson = input.flatMap((String s, Collector<String> stringCollector) -> {//here all the transformations or processing of input jsons take place


                String json = "";

                try {
                    JsonObject jsonObject = (JsonObject) jsonParser.parse(s);
                    json = jsonObject.toString();

                    if (json.trim().equalsIgnoreCase("")) //If input json is empty or null, return.
                        return;

                    stringCollector.collect(json);

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }).returns(Types.STRING).setParallelism(1).name("Map");

            ElasticsearchSink.Builder<String> elasticsearchSink = (ElasticsearchSink.Builder<String>) ESConnector.getElasticsearchSink(httpHosts, 1, 1000);//This is the elasticsearch sink
            inputJson.addSink(elasticsearchSink.build())                                                                                                                       // You can change bulk and max-interval as per your requirement
                    .setParallelism(1)
                    .name("EsSink");

            env.execute("Flink_ES");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
