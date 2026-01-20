package com.example.aggregator.enhanced;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.util.UUID;

/**
 * Control 토픽을 통한 작업 완료 상태 관리 클래스
 * Collection → Transform → Analysis → Aggregation 단계별 완료 상태를 추적
 */
public class ControlTopicManager {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final KStream<String, String> controlStream;
    private final String controlTopicName;
    
    public ControlTopicManager(KStream<String, String> controlStream, String controlTopicName) {
        this.controlStream = controlStream;
        this.controlTopicName = controlTopicName;
    }
    
    /**
     * Collection 단계 완료 메시지 발행
     */
    public void publishCollectionDone(String jobId, int expectedCount, int actualCount) {
        ObjectNode message = createControlMessage(jobId, "collection", "done", expectedCount, actualCount);
        publishControlMessage(message);
    }
    
    /**
     * Transform 단계 완료 메시지 발행
     */
    public void publishTransformDone(String jobId, int expectedCount, int actualCount) {
        System.out.println("CONTROL_TOPIC_DEBUG: Publishing transform done message to control topic - jobId=" + jobId + ", expected=" + expectedCount + ", actual=" + actualCount);
        ObjectNode message = createControlMessage(jobId, "transform", "done", expectedCount, actualCount);
        publishControlMessage(message);
    }
    
    /**
     * Analysis 단계 완료 메시지 발행
     */
    public void publishAnalysisDone(String jobId, int expectedCount, int actualCount) {
        System.out.println("CONTROL_TOPIC_DEBUG: Publishing analysis done message to control topic - jobId=" + jobId + ", expected=" + expectedCount + ", actual=" + actualCount);
        ObjectNode message = createControlMessage(jobId, "analysis", "done", expectedCount, actualCount);
        publishControlMessage(message);
    }
    
    /**
     * Aggregation 단계 완료 메시지 발행
     */
    public void publishAggregationDone(String jobId, int expectedCount, int actualCount) {
        System.out.println("CONTROL_TOPIC_DEBUG: Publishing aggregation done message to control topic - jobId=" + jobId + ", expected=" + expectedCount + ", actual=" + actualCount);
        ObjectNode message = createControlMessage(jobId, "aggregation", "done", expectedCount, actualCount);
        publishControlMessage(message);
    }
    
    /**
     * 실패 메시지 발행
     */
    public void publishFailure(String jobId, String step, String errorMessage, int expectedCount, int actualCount) {
        ObjectNode message = createControlMessage(jobId, step, "failed", expectedCount, actualCount);
        message.put("error_message", errorMessage);
        publishControlMessage(message);
    }
    
    /**
     * 타임아웃 메시지 발행
     */
    public void publishTimeout(String jobId, String step, int expectedCount, int actualCount) {
        ObjectNode message = createControlMessage(jobId, step, "timeout", expectedCount, actualCount);
        message.put("error_message", "Processing timeout exceeded");
        publishControlMessage(message);
    }
    
    /**
     * Control 메시지 생성
     */
    private ObjectNode createControlMessage(String jobId, String step, String status, int expectedCount, int actualCount) {
        ObjectNode message = MAPPER.createObjectNode();
        message.put("job_id", jobId);
        message.put("step", step);
        message.put("status", status);
        message.put("expected_count", expectedCount);
        message.put("actual_count", actualCount);
        message.put("timestamp", Instant.now().toString());
        
        // 메타데이터 추가
        ObjectNode metadata = MAPPER.createObjectNode();
        metadata.put("processing_time", 0.0);
        metadata.put("worker_id", "kafka-streams-enhanced-worker");
        metadata.put("batch_id", "batch-" + System.currentTimeMillis());
        metadata.put("server_info", "kafka-streams-enhanced-v1.0.0");
        message.set("metadata", metadata);
        
        return message;
    }
    
    /**
     * Control 메시지 발행
     */
    private void publishControlMessage(ObjectNode message) {
        try {
            String messageJson = MAPPER.writeValueAsString(message);
            String key = message.get("job_id").asText() + "|" + message.get("step").asText();
            
            System.out.println("CONTROL_TOPIC_DEBUG: Control message created and ready to publish - key=" + key + ", message=" + messageJson);
            
            // 실제 Control 토픽에 메시지 발행
            controlStream
                .filter((k, v) -> k != null && v != null)
                .mapValues(v -> messageJson)
                .to(controlTopicName, Produced.with(Serdes.String(), Serdes.String()));
            
            System.out.println("CONTROL_TOPIC_DEBUG: Control message published to topic=" + controlTopicName + ", key=" + key);
            
        } catch (Exception e) {
            System.err.println("CONTROL_TOPIC_DEBUG: Failed to publish control message: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 메시지 유효성 검증
     */
    public boolean isValidControlMessage(ObjectNode message) {
        try {
            return message.has("job_id") && 
                   message.has("step") && 
                   message.has("status") && 
                   message.has("expected_count") && 
                   message.has("actual_count") && 
                   message.has("timestamp");
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 단계별 타임아웃 설정 (밀리초)
     */
    public static class TimeoutConfig {
        public static final long COLLECTION_TIMEOUT = 60 * 60 * 1000;  // 60분
        public static final long TRANSFORM_TIMEOUT = 30 * 60 * 1000;   // 30분
        public static final long ANALYSIS_TIMEOUT = 45 * 60 * 1000;   // 45분
        public static final long AGGREGATION_TIMEOUT = 15 * 60 * 1000; // 15분
    }
    
    /**
     * 단계별 타임아웃 값 조회
     */
    public static long getTimeoutForStep(String step) {
        switch (step.toLowerCase()) {
            case "collection":
                return TimeoutConfig.COLLECTION_TIMEOUT;
            case "transform":
                return TimeoutConfig.TRANSFORM_TIMEOUT;
            case "analysis":
                return TimeoutConfig.ANALYSIS_TIMEOUT;
            case "aggregation":
                return TimeoutConfig.AGGREGATION_TIMEOUT;
            default:
                return 30 * 60 * 1000; // 기본 30분
        }
    }
}
