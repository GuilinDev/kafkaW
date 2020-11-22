package com.imooc.guilin.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;

public class AdminSample {

    public static final String TOPIC_NAME = "gz_topic";

    public static void main(String[] args) throws Exception {
        // check kafka connection
//        AdminClient adminClient = AdminSample.adminClient();
//        System.out.println("adminClient: " + adminClient);

        // create Topic instance
//        createTopic();

        // delete a topics
//        delTopics();

        // get topic list
//        topicLists();

        // describe topic
//        describeTopics();

        // alter config
//        alterConfig();
        // describe config
//        describeConfig();

        // increment number of partitions
//        incrPartitions(2);
    }

    /**
     * create Topic instances
     */
    public static void createTopics() {
        AdminClient adminClient = adminClient(); // 所有的操作都是基于adminClient对象
        short rs = 1; // replication factor
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, rs);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));

        System.out.println("CreateTopicsResult: " + topics.toString());
    }

    /**
     * delete topics
     */
    public static void delTopics() throws Exception {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        deleteTopicsResult.all().get();
    }

    /**
     * Get Kafka topic lists
     */
    public static void topicLists() throws Exception {
        AdminClient adminClient = adminClient();
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true); // print internal out __consumer_offset
//        ListTopicsResult listTopicsResult = adminClient.listTopics();
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> names = listTopicsResult.names().get();
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        KafkaFuture<Map<String, TopicListing>> mapKafkaFuture = listTopicsResult.namesToListings();

        // print topic names
        names.forEach(System.out::println);

        // print topic listings
        topicListings.forEach(System.out::println);
    }

    /**
     * describe topics
     * name: gz_topic,
     * desc:(name=gz_topic,
     *      internal=false,
     *      partitions=
     *          (partition=0,
     *          leader=192.168.1.252:9092 (id: 0 rack: null),
     *          replicas=192.168.1.252:9092 (id: 0 rack: null),
     *          isr=192.168.1.252:9092 (id: 0 rack: null)),
     *          authorizedOperations=[]
     */
    public static void describeTopics() throws Exception {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPIC_NAME));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
        entries.stream().forEach((entry) -> {
            System.out.println("name: " + entry.getKey() + ", desc:" + entry.getValue());
        });
    }

    /**
     * describe config
     */
    public static void describeConfig() throws Exception {
        AdminClient adminClient = adminClient();

        // ConfigResource.Type.BROKER 集群相关
//        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, TOPIC_NAME);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();

        configResourceConfigMap.entrySet().stream().forEach((entry) -> {
            System.out.println("Configresource: " + entry.getKey() + " , Config: " + entry.getValue());
        });
    }

    /**
     * modify config
     */
    public static void alterConfig() throws Exception {
        AdminClient adminClient = adminClient();
        Map<ConfigResource, Config> configMap = new HashMap<>();

        // 组织两个参数
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        Config config = new Config(Arrays.asList((new ConfigEntry("preallocate", "true"))));
        configMap.put(configResource, config);

        // alterConfigs是2.2.0之前修改config的API，比较稳定，2.3.0和之后的API为incrementalAlterConfigs，对集群支持不大好
        // 同时config也变为AlterConfigOp
        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(configMap);
        alterConfigsResult.all().get();

    }

    /**
     * increment partitions，KAFKA中partitions只能增加，无法删除
     */
    public static void incrPartitions(int partitions) throws Exception {
        AdminClient adminClient = adminClient();
        Map<String, NewPartitions> partitionsMap = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(partitions);
        partitionsMap.put(TOPIC_NAME, newPartitions);


        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(partitionsMap);
        createPartitionsResult.all().get();
    }

    /**
     * Cofig AdminClient
     * 所有的操作都是基于adminClient对象
     * @return
     */
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.252:9092"); //local server ip
        AdminClient adminClient = AdminClient.create(properties);

        return adminClient;
    }
}
