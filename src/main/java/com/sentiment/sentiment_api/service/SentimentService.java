package com.sentiment.sentiment_api.service;


import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

@Service
public class SentimentService {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final String KAFKA_TOPIC = "sentiment-requests";

    public SentimentService(StringRedisTemplate redisTemplate, KafkaTemplate<String, String> kafkaTemplate) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    public Map<String,Object> processSentimentRequest(String ticker) {
            String redisKey = "Sentiment:" + ticker.toUpperCase();

            String cachedData = redisTemplate.opsForValue().get(redisKey);

            if(cachedData !=null) {
                // cache hit

                return Map.of("status","COMPLETED",
                        "data",cachedData);
            }

            else {
                // cache Miss

                String jobId = UUID.randomUUID().toString();

                // We send a simple message to Kafka: "jobId:ticker"
                String kafkaMessage = jobId + ":" + ticker.toUpperCase();
                kafkaTemplate.send(KAFKA_TOPIC, kafkaMessage);

                return Map.of(
                        "status", "PROCESSING",
                        "jobId", jobId,
                        "message", "Request accepted. Polling endpoint will be available soon."
                );
            }
    }
}
