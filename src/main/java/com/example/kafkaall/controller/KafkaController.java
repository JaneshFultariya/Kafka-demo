package com.example.kafkaall.controller;

import com.example.kafkaall.User;
import com.github.javafaker.Faker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class KafkaController {

    List<String> message = new ArrayList<>();

    List<User> userList = new ArrayList<>();
//    User userFromTopic = null;

    @Autowired
    private KafkaTemplate<String, Object> template;

    private String topic = "testpublishers";

    private String topicJson = "testTopicJson";

    @GetMapping("/publish")
    public String publisMessage(){
        Faker faker = new Faker();
        String spell = faker.nation().nationality();
        template.send(topic, spell);
        return "nationality published";
    }

    @PostMapping("/publishJson")
    public String publisMessages(@RequestBody User user){
        template.send(topicJson,  user);
        return "user published";
    }

    @GetMapping("/consumerStringMessage")
    public List<String> consumerMessage(){
        return message;
    }

    @GetMapping("/consumerJsonMessage")
    public List<User> consumerJsonMessage(){
        return userList;
    }

    @KafkaListener(groupId = "testConfig-1", topics = "testpublishers", containerFactory = "kafkaListenerContainerFactory")
    public List<String> getMessageFromTopic(String data){
        message.add(data);
        System.out.println(message);
        return message;
    }

    @KafkaListener(groupId = "testConfig-2", topics = "testTopicJson", containerFactory = "userKafkaListenerContainerFactory")
    public List<User> getJsonMessageFromTopic(User user){
        userList.add(user);
//        userFromTopic = user;
        return userList;
    }
}
