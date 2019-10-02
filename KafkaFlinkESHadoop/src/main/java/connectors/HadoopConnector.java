package connectors;

import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.hadoop.conf.Configuration;

public class HadoopConnector {

    public static BucketingSink<String> getSink(String topicName, String hadoopURL) {
        BucketingSink<String> sink = new BucketingSink<String>("basepath");
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME", "hdadmin");
        conf.set("fs.defaultFS", hadoopURL);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        System.setProperty("HADOOP_USER_NAME", "hdamin");
        System.setProperty("hadoop.home.dir", "/");
        sink.setFSConfig(conf);
        sink.setBucketer(new DateTimeBucketer<String>("YYYYMMdd/HH"));
        sink.setBatchSize(1024 * 1024 * 128);
        sink.setBatchRolloverInterval(60 * 60 * 1000);
        return sink;
    }
}
