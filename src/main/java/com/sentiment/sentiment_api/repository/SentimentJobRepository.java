package com.sentiment.sentiment_api.repository;

import com.sentiment.sentiment_api.entity.SentimentJob;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface SentimentJobRepository extends JpaRepository<SentimentJob, UUID> {
}