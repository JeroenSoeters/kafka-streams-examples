package localcluster;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by arunmohzi on 7/13/17.
 */
public class KafkaLocalServer {
    public KafkaServerStartable kafka;
    public ZooKeeperLocal zookeeper;
    private static final String KAFKA_LOG_DIR_CONF = "log.dir";
    private static final String ZOOKEEPER_DIR_CONF = "dataDir";
    
    public KafkaLocalServer(){
    }

    public KafkaLocalServer(Properties kafkaProperties, Properties zkProperties) throws IOException, InterruptedException{


        String kafkaLogPath = kafkaProperties.getProperty(KAFKA_LOG_DIR_CONF);
        if (kafkaLogPath == null || kafkaLogPath.isEmpty())
            throw new IllegalArgumentException("Kafka Log directory is not properly set");

        String zookeeperPath = zkProperties.getProperty(ZOOKEEPER_DIR_CONF);
        if (zookeeperPath == null || zookeeperPath.isEmpty())
            throw new IllegalArgumentException("Zookeeper path is not properly set");


        File kafkaLogDir = new File(kafkaLogPath);
        if(kafkaLogDir.exists()) {
            deleteFolder(kafkaLogDir);
        }

        File zookeeperDir = new File(zookeeperPath);
        if(zookeeperDir.exists()) {
            deleteFolder(zookeeperDir);
        }

        String kafka_log_dir = kafkaProperties.getProperty(KAFKA_LOG_DIR_CONF);
        if (kafka_log_dir == null || kafka_log_dir.isEmpty())
            throw new IllegalArgumentException("Kafka Log directory is not properly set");

        File kafkaDir = new File(kafka_log_dir);
        if(kafkaDir.exists()) {
            deleteFolder(kafkaDir);
        }


        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

        //start local zookeeper
        System.out.println("starting local zookeeper...");
        zookeeper = new ZooKeeperLocal(zkProperties);
        System.out.println("done");

        //start local kafka broker
        kafka = new KafkaServerStartable(kafkaConfig);
        System.out.println("starting local kafka broker...");
        kafka.startup();
        System.out.println("done");
    }


    public void stop(){
        //stop kafka broker
        System.out.println("stopping kafka...");
        kafka.shutdown();
        System.out.println("done");
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) { // some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
}