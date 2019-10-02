package connectors;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

public class ESConnector {

    public static ElasticsearchSink.Builder getElasticsearchSink(List<HttpHost> httpHosts, int esBulk, int esMaxInterval) throws Exception{


        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<String>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {


                        try {

                            return Requests.indexRequest()
                                    .index("index_name")
                                    .type("_doc")
                                    .source(element, XContentType.JSON);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }

                    }

                    @Override

                    public void process(String element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        try {
                            if (!element.trim().equalsIgnoreCase("") && requestIndexer != null) {
                                element = element.trim();
                                requestIndexer.add(createIndexRequest(element));
                            }
                        }
                        catch(Exception e){
                            System.out.println("Exception in adding documents..");
                            e.printStackTrace();

                        }

                    }
                }


        );

        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {

                    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {

                            // elasticsearch username and password
                            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("username", "password"));
                            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });

                }
        );

        esSinkBuilder.setBulkFlushMaxActions(esBulk);
        esSinkBuilder.setBulkFlushInterval(esMaxInterval);
        esSinkBuilder.setFailureHandler(new ActionRequestFailureHandler() {
            @Override
            public void onFailure(ActionRequest actionRequest, Throwable throwable, int i, RequestIndexer requestIndexer) throws Throwable {
                if (ESConnector.containsThrowable(throwable, EsRejectedExecutionException.class)) {
                    // full queue; re-add document for indexing
                    requestIndexer.add(actionRequest);
                } else if (ESConnector.containsThrowable(throwable, ElasticsearchParseException.class)) {
                    // malformed document; simply drop request without failing sink
                    System.out.println("Exception onFailure Elasticsearch ParseException ...");
                }
                else if (ESConnector.containsThrowable(throwable, ElasticsearchException.class)) {
                    // malformed document; simply drop request without failing sink

                    System.out.println("Exception onFailure Elasticsearch Exception ....");

                } else if(ESConnector.containsThrowable(throwable,IllegalArgumentException.class)){

                    System.out.println("Elasticsearch IllegalArgumentException Exception...");

                }
                else {
                    // for all other failures, fail the sink
                    // here the failure is simply rethrown, but users can also choose to throw custom exceptions
                    throw throwable;
                }
            }
        });

        return esSinkBuilder;
    }
    public static boolean containsThrowable(Throwable throwable, Class<?> searchType) {
        if (throwable == null || searchType == null) {
            return false;
        }

        Throwable t = throwable;
        while (t != null) {
            if (searchType.isAssignableFrom(t.getClass())) {
                return true;
            } else {
                t = t.getCause();
            }
        }

        return false;
    }

    public static List<HttpHost> getESHttpHosts(String esClusterIPs) throws Exception{
        List<HttpHost> httpHosts = new ArrayList<>();
        String[] ips=esClusterIPs.split(",");
        for(String ip: ips){

            httpHosts.add(new HttpHost(ip, 9200,"http"));


        }
        return httpHosts;
    }
}
