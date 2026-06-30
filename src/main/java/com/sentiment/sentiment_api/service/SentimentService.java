package com.sentiment.sentiment_api.service;


import com.sentiment.ai_worker.Enum.JobStatus;
import com.sentiment.sentiment_api.entity.SentimentJob;
import com.sentiment.sentiment_api.repository.SentimentJobRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor // Use this for cleaner constructor injection
public class SentimentService {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final SentimentJobRepository jobRepository;

    private static final String KAFKA_TOPIC = "sentiment-requests";

    public Map<String, Object> processSentimentRequest(String ticker) {
        String redisKey = "Sentiment:" + ticker.toUpperCase();
        String cachedData = redisTemplate.opsForValue().get(redisKey);

        if (cachedData != null) {
            return Map.of("status", "COMPLETED", "data", cachedData);
        } else {
            // 2. Create and Save the Job in PostgreSQL
            SentimentJob job = new SentimentJob();
            job.setTicker(ticker.toUpperCase());
            job.setStatus(JobStatus.QUEUED); // Assuming JobStatus is your Enum
            job.setRequestedAt(LocalDateTime.now());
            job = jobRepository.save(job); // Now the job exists in the DB!

            // 3. Send the JSON-formatted message to Kafka
            // We use JSON here because your Worker expects {"jobId": "...", "ticker": "..."}
            String kafkaMessage = String.format("{\"jobId\": \"%s\", \"ticker\": \"%s\"}",
                    job.getJobId(), ticker.toUpperCase());

            kafkaTemplate.send(KAFKA_TOPIC, job.getJobId().toString(), kafkaMessage);
            kafkaTemplate.flush();

            return Map.of(
                    "status", "PROCESSING",
                    "jobId", job.getJobId().toString(),
                    "message", "Request accepted."
            );
        }
    }
}