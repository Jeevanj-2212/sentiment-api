package com.sentiment.sentiment_api.controller;


import com.sentiment.sentiment_api.service.SentimentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("api/v1")
public class SentimentController {

    private final SentimentService sentimentService;

    public SentimentController(SentimentService sentimentService) {
        this.sentimentService = sentimentService;
    }

    @GetMapping("/sentiment")
    public ResponseEntity<?> getSentiment(@RequestParam String ticker) {


        Map<String, Object> result = sentimentService.processSentimentRequest(ticker);


        if ("COMPLETED".equals(result.get("status"))) {
            // Cache Hit -> Return 200 OK
            return ResponseEntity.ok(result);
        } else {
            // Cache Miss -> Return 202 Accepted
            return ResponseEntity.accepted().body(result);
        }
    }


}
