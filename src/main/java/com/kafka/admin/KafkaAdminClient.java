package com.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Properties;
import java.util.Set;

public class KafkaAdminClient {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try(AdminClient adminClient = AdminClient.create(props)){
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topics = topicsResult.names().get();
            System.out.println(topics);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
