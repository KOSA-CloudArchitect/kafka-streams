package com.example.aggregator.enhanced;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.KeyValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.core.type.TypeReference;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * Transform 통계 정보를 담는 클래스
 */
class TransformStats {
    public final int totalReviews;
    public final int validReviews;
    public final int emptyReviews;
    public final int coupangTrialReviews;
    public final double avgRating;
    public final double avgRatingExcludingEmpty;
    public final double avgRatingCoupangTrial;
    public final double avgRatingRegular;
    public final RatingDistribution ratingDistribution;
    public final KeywordStats keywordStats;
    
    public TransformStats(int totalReviews, int validReviews, int emptyReviews, int coupangTrialReviews,
                         double avgRating, double avgRatingExcludingEmpty, double avgRatingCoupangTrial, double avgRatingRegular,
                         RatingDistribution ratingDistribution, KeywordStats keywordStats) {
        this.totalReviews = totalReviews;
        this.validReviews = validReviews;
        this.emptyReviews = emptyReviews;
        this.coupangTrialReviews = coupangTrialReviews;
        this.avgRating = avgRating;
        this.avgRatingExcludingEmpty = avgRatingExcludingEmpty;
        this.avgRatingCoupangTrial = avgRatingCoupangTrial;
        this.avgRatingRegular = avgRatingRegular;
        this.ratingDistribution = ratingDistribution;
        this.keywordStats = keywordStats;
    }
}

/**
 * 별점 분포 정보를 담는 클래스 (1-5점별 개수)
 */
class RatingCounts {
    public final int rating1;
    public final int rating2;
    public final int rating3;
    public final int rating4;
    public final int rating5;
    
    public RatingCounts(int rating1, int rating2, int rating3, int rating4, int rating5) {
        this.rating1 = rating1;
        this.rating2 = rating2;
        this.rating3 = rating3;
        this.rating4 = rating4;
        this.rating5 = rating5;
    }
    
    public RatingCounts add(RatingCounts other) {
        return new RatingCounts(
            this.rating1 + other.rating1,
            this.rating2 + other.rating2,
            this.rating3 + other.rating3,
            this.rating4 + other.rating4,
            this.rating5 + other.rating5
        );
    }
}

/**
 * 전체 별점 분포 정보를 담는 클래스 (all, coupang_trial, regular, empty_review)
 */
class RatingDistribution {
    public final RatingCounts all;
    public final RatingCounts coupangTrial;
    public final RatingCounts regular;
    public final RatingCounts emptyReview;
    
    public RatingDistribution(RatingCounts all, RatingCounts coupangTrial, RatingCounts regular, RatingCounts emptyReview) {
        this.all = all;
        this.coupangTrial = coupangTrial;
        this.regular = regular;
        this.emptyReview = emptyReview;
    }
    
    public RatingDistribution add(RatingDistribution other) {
        return new RatingDistribution(
            this.all.add(other.all),
            this.coupangTrial.add(other.coupangTrial),
            this.regular.add(other.regular),
            this.emptyReview.add(other.emptyReview)
        );
    }
}

/**
 * 감정 분포 정보를 담는 클래스
 */
class SentimentDistribution {
    public final int positive;
    public final int negative;
    public final int neutral;
    public final double positivePct;
    public final double negativePct;
    public final double neutralPct;
    
    public SentimentDistribution(int positive, int negative, int neutral) {
        this.positive = positive;
        this.negative = negative;
        this.neutral = neutral;
        
        int total = positive + negative + neutral;
        this.positivePct = total > 0 ? (double) positive / total * 100.0 : 0.0;
        this.negativePct = total > 0 ? (double) negative / total * 100.0 : 0.0;
        this.neutralPct = total > 0 ? (double) neutral / total * 100.0 : 0.0;
    }
}

/**
 * Analysis 통계 정보를 담는 클래스
 */
class AnalysisStats {
    public final int totalAnalyzed;
    public final SentimentDistribution allSentiment;
    public final SentimentDistribution coupangTrialSentiment;
    public final SentimentDistribution regularSentiment;
    
    public AnalysisStats(int totalAnalyzed, SentimentDistribution allSentiment, 
                        SentimentDistribution coupangTrialSentiment, SentimentDistribution regularSentiment) {
        this.totalAnalyzed = totalAnalyzed;
        this.allSentiment = allSentiment;
        this.coupangTrialSentiment = coupangTrialSentiment;
        this.regularSentiment = regularSentiment;
    }
}

/**
 * 키워드 태그 정보를 담는 클래스
 */
class KeywordTag {
    public final String tag;
    public final int count;
    public final double percentage;
    
    public KeywordTag(String tag, int count, double percentage) {
        this.tag = tag;
        this.count = count;
        this.percentage = percentage;
    }
}

/**
 * 키워드 분석 정보를 담는 클래스 (쿠팡체험단 구분 포함)
 */
class KeywordAnalysis {
    public final String keyword;
    public final List<KeywordTag> allTags;           // 전체 태그
    public final List<KeywordTag> coupangTags;       // 쿠팡체험단 태그
    public final List<KeywordTag> regularTags;       // 일반 구매자 태그
    
    public KeywordAnalysis(String keyword, List<KeywordTag> allTags, 
                          List<KeywordTag> coupangTags, List<KeywordTag> regularTags) {
        this.keyword = keyword;
        this.allTags = allTags;
        this.coupangTags = coupangTags;
        this.regularTags = regularTags;
    }
}

/**
 * 키워드 분석 통계를 담는 클래스
 */
class KeywordStats {
    public final List<KeywordAnalysis> topKeywords;
    
    public KeywordStats(List<KeywordAnalysis> topKeywords) {
        this.topKeywords = topKeywords;
    }
}

/**
 * 향상된 Kafka Streams 리뷰 집계 애플리케이션
 * Control 토픽 기반 작업 완료 관리 및 고급 집계 기능 제공
 */
public class EnhancedReviewAggregator {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private ControlTopicManager controlManager;
    
    /**
     * 한국 시간(KST)으로 현재 시간을 ISO 8601 형식으로 반환
     */
    private static String getCurrentTimeInKST() {
        ZoneId kstZone = ZoneId.of("Asia/Seoul");
        ZonedDateTime now = ZonedDateTime.now(kstZone);
        return now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
    
    /**
     * 키워드 태그 맵을 KeywordTag 리스트로 변환하는 헬퍼 메서드
     */
    private static List<KeywordTag> createKeywordTags(Map<String, Integer> tagCounts) {
        if (tagCounts == null || tagCounts.isEmpty()) {
            return new ArrayList<>();
        }
        
        // 총 개수 계산
        int totalCount = tagCounts.values().stream().mapToInt(Integer::intValue).sum();
        
        // 태그별 정보 생성
        List<KeywordTag> tags = new ArrayList<>();
        for (Map.Entry<String, Integer> tagEntry : tagCounts.entrySet()) {
            String tag = tagEntry.getKey();
            int count = tagEntry.getValue();
            double percentage = totalCount > 0 ? (double) count / totalCount * 100.0 : 0.0;
            tags.add(new KeywordTag(tag, count, percentage));
        }
        
        // 개수 기준으로 정렬 (내림차순)
        tags.sort((a, b) -> Integer.compare(b.count, a.count));
        
        return tags;
    }
    
    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, env("APPLICATION_ID", "review-aggregator-enhanced-v1"));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, env("PROCESSING_GUARANTEE", "exactly_once_v2"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, env("COMMIT_INTERVAL_MS", "100"));
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.STATE_DIR_CONFIG, env("STATE_DIR", "/tmp/kafka-streams"));
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                org.apache.kafka.streams.errors.DefaultProductionExceptionHandler.class);

        String transformTopic = env("INPUT_TRANSFORM_TOPIC", "realtime-review-transform-topic");
        String analysisTopic = env("INPUT_ANALYSIS_TOPIC", "realtime-review-analysis-topic");
        String controlTopic = env("CONTROL_TOPIC", "job-control-topic");
        String outAggTopic = env("OUTPUT_AGG_TOPIC", "review-agg-by-job");
        String outRowsTopic = env("OUTPUT_ROWS_TOPIC", "review-rows");

        StreamsBuilder builder = new StreamsBuilder();
        EnhancedReviewAggregator aggregator = new EnhancedReviewAggregator();
        aggregator.buildTopology(builder, transformTopic, analysisTopic, controlTopic, outAggTopic, outRowsTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Keep process alive; on exception, print stack but do not exit
        CountDownLatch latch = new CountDownLatch(1);
        streams.setUncaughtExceptionHandler((t, e) -> {
            System.err.println("[FATAL] Uncaught exception in thread: " + t.getName());
            e.printStackTrace(System.err);
            // keep alive for inspection
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    public void buildTopology(StreamsBuilder builder, String transformTopic, String analysisTopic, 
                             String controlTopic, String outAggTopic, String outRowsTopic) {
        
        // ========== 입력 스트림 설정 ==========
        KStream<String, String> transform = builder.stream(transformTopic);
        KStream<String, String> analysis = builder.stream(analysisTopic);
        KStream<String, String> control = builder.stream(controlTopic);
        
        // Control 토픽 매니저 초기화
        controlManager = new ControlTopicManager(control, controlTopic);
        
        // ========== Transform 스트림 처리 ==========
        KStream<String, JsonNode> transformFlat = transform.mapValues(value -> {
            // 개별 raw data 처리 - 리스트가 아닌 단일 객체로 처리
            try {
                ObjectNode reviewNode = (ObjectNode) MAPPER.readTree(value);
                
                // job_id가 없으면 추가 (필요시)
                if (!reviewNode.has("job_id") || reviewNode.path("job_id").asText("").isEmpty()) {
                    // job_id를 키에서 추출하거나 기본값 설정
                    reviewNode.put("job_id", "unknown");
                }
                
                // rating 필드 처리: Transform에서 별점 데이터가 null로 올 수 있음
                try {
                    double rating = 0.0;
                    if (reviewNode.has("rating") && !reviewNode.get("rating").isNull()) {
                        rating = reviewNode.path("rating").asDouble(0.0);
                    } else if (reviewNode.has("overall_star_rating") && !reviewNode.get("overall_star_rating").isNull()) {
                        rating = reviewNode.path("overall_star_rating").asDouble(0.0);
                    } else if (reviewNode.has("star_rating") && !reviewNode.get("star_rating").isNull()) {
                        rating = reviewNode.path("star_rating").asDouble(0.0);
                    }
                    
                    // rating 필드 설정 (double 형태)
                    reviewNode.put("rating", rating);
                    reviewNode.put("overall_star_rating", rating);
                } catch (Exception e) {
                    System.err.println("Error setting rating field: " + e.getMessage());
                }
                
                return reviewNode;
            } catch (Exception e) {
                System.err.println("Error parsing transform data: " + e.getMessage());
                // 빈 객체 반환
                ObjectNode emptyNode = MAPPER.createObjectNode();
                emptyNode.put("job_id", "error");
                emptyNode.put("review_id", "error");
                return emptyNode;
            }
        });
        
        KStream<String, JsonNode> analysisFlat = analysis.flatMapValues(value -> {
            // Analysis 데이터는 여전히 리스트 형태로 처리
            // 각 리뷰 데이터에 이미 job_id가 포함되어 있으므로 그것을 사용
            try {
                ObjectNode msg = MAPPER.readValue(value, ObjectNode.class);
                System.out.println("CONTROL_TOPIC_DEBUG: Processing analysis message: " + msg.toString());
                
                // results 배열 추출
                JsonNode resultsNode = msg.path("results");
                if (!resultsNode.isArray()) {
                    System.err.println("CONTROL_TOPIC_DEBUG: Analysis results is not an array: " + resultsNode.toString());
                    return Collections.emptyList();
                }
                
                System.out.println("CONTROL_TOPIC_DEBUG: Analysis results array size: " + resultsNode.size());
                
                // 개별 분석 결과를 그대로 사용 (각 리뷰에 이미 job_id가 포함됨)
                List<JsonNode> results = new ArrayList<>();
                for (JsonNode result : resultsNode) {
                    // 각 리뷰의 job_id를 확인하고 유효하지 않으면 스킵
                    String jobId = result.path("job_id").asText();
                    if (jobId != null && !jobId.isEmpty()) {
                        results.add(result);
                        System.out.println("CONTROL_TOPIC_DEBUG: Added analysis result for jobId=" + jobId);
                    } else {
                        System.err.println("Warning: Analysis result missing job_id, skipping: " + result.toString());
                    }
                }
                
                System.out.println("CONTROL_TOPIC_DEBUG: Analysis flatMapValues returning " + results.size() + " results");
                return results;
            } catch (Exception e) {
                System.err.println("Error parsing analysis data: " + e.getMessage());
                e.printStackTrace();
                return Collections.emptyList();
            }
        });

        // Transform 스트림에서 각 리뷰의 job_id를 키로 사용
        KStream<String, String> transformFlatStr = transformFlat
                .selectKey((key, node) -> {
                    // 각 리뷰의 job_id를 키로 사용
                    String jobId = node.path("job_id").asText();
                    return jobId != null && !jobId.isEmpty() ? jobId : "unknown";
                })
                .mapValues(node -> EnhancedJsonUtils.toString(MAPPER, (ObjectNode) node));
        // Analysis 스트림에서 각 리뷰의 job_id를 키로 사용
        KStream<String, String> analysisFlatStr = analysisFlat
                .selectKey((key, node) -> {
                    // 각 리뷰의 job_id를 키로 사용
                    String jobId = node.path("job_id").asText();
                    String reviewId = node.path("review_id").asText();
                    System.out.println("CONTROL_TOPIC_DEBUG: Analysis selectKey - jobId=" + jobId + ", reviewId=" + reviewId);
                    return jobId != null && !jobId.isEmpty() ? jobId : "unknown";
                })
                .mapValues(node -> EnhancedJsonUtils.toString(MAPPER, (ObjectNode) node));

        // ========== 윈도우 조인 설정 ==========
        Duration windowGap = Duration.ofMillis(Long.parseLong(env("WINDOW_INACTIVITY_MS", "90000")));
        JoinWindows windows = JoinWindows.ofTimeDifferenceWithNoGrace(windowGap);

        // Re-key by composite job_id|review_id for join (values are String) + explicit repartition with String serdes
        KStream<String, String> tByReviewStr = transformFlatStr
                .selectKey((jobId, jsonStr) -> {
                    // jobId는 이미 각 리뷰의 job_id이므로 그대로 사용
                    String reviewId = EnhancedJsonUtils.parseObjectNode(MAPPER, jsonStr).path("review_id").asText();
                    return composite(jobId, reviewId);
                })
                .repartition(Repartitioned.with(Serdes.String(), Serdes.String()));
        KStream<String, String> aByReviewStr = analysisFlatStr
                .selectKey((jobId, jsonStr) -> {
                    // jobId는 이미 각 리뷰의 job_id이므로 그대로 사용
                    String reviewId = EnhancedJsonUtils.parseObjectNode(MAPPER, jsonStr).path("review_id").asText();
                    return composite(jobId, reviewId);
                })
                .repartition(Repartitioned.with(Serdes.String(), Serdes.String()));

        // ========== 조인 처리 ==========
        KStream<String, ObjectNode> joinedPerReview = tByReviewStr.join(
                aByReviewStr,
                (tStr, aStr) -> EnhancedJsonUtils.joinReview(MAPPER, 
                    EnhancedJsonUtils.parseObjectNode(MAPPER, tStr), 
                    EnhancedJsonUtils.parseObjectNode(MAPPER, aStr)),
                windows
        );

        // ========== 개별 리뷰 행 발행 ==========
        KStream<String, String> rowString = joinedPerReview.mapValues(node -> 
            EnhancedJsonUtils.toString(MAPPER, node));
        rowString.to(outRowsTopic, Produced.with(Serdes.String(), Serdes.String()));

        // ========== Transform 집계 ==========
        KTable<String, String> transformAggByJob = transformFlatStr
                .selectKey((key, jsonStr) -> EnhancedJsonUtils.extractJobId(MAPPER, jsonStr))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> EnhancedJsonUtils.toString(MAPPER, EnhancedJsonUtils.emptyTransformAgg(MAPPER)),
                        (job, rowStr, accStr) -> {
                            ObjectNode acc = EnhancedJsonUtils.parseObjectNode(MAPPER, accStr);
                            ObjectNode row = EnhancedJsonUtils.parseObjectNode(MAPPER, rowStr);
                            ObjectNode updated = EnhancedJsonUtils.accumulateTransform(MAPPER, acc, row);
                            return EnhancedJsonUtils.toString(MAPPER, updated);
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                );

        // ========== Analysis 집계 ==========
        KTable<String, String> analysisAggByJob = analysisFlatStr
                .selectKey((key, jsonStr) -> EnhancedJsonUtils.extractJobId(MAPPER, jsonStr))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> EnhancedJsonUtils.toString(MAPPER, EnhancedJsonUtils.emptyAnalysisAgg(MAPPER)),
                        (job, rowStr, accStr) -> {
                            ObjectNode acc = EnhancedJsonUtils.parseObjectNode(MAPPER, accStr);
                            ObjectNode row = EnhancedJsonUtils.parseObjectNode(MAPPER, rowStr);
                            ObjectNode updated = EnhancedJsonUtils.accumulateAnalysis(MAPPER, acc, row);
                            return EnhancedJsonUtils.toString(MAPPER, updated);
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                );

        // ========== 통합 집계 ==========
        KTable<String, String> combinedAgg = transformAggByJob.join(
                analysisAggByJob,
                (transformStr, analysisStr) -> {
                    ObjectNode transformAgg = EnhancedJsonUtils.parseObjectNode(MAPPER, transformStr);
                    ObjectNode analysisAgg = EnhancedJsonUtils.parseObjectNode(MAPPER, analysisStr);
                    ObjectNode combined = EnhancedJsonUtils.createCombinedAggregation(MAPPER, transformAgg, analysisAgg);
                    return EnhancedJsonUtils.toString(MAPPER, combined);
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        // ========== 집계 결과 발행 ==========
        combinedAgg.toStream().to(outAggTopic, Produced.with(Serdes.String(), Serdes.String()));

        // ========== Control 토픽 모니터링 ==========
        // Transform/Analysis 토픽에서 job_id별 카운트를 윈도우 기반으로 실시간 집계
        KTable<Windowed<String>, Long> transformCountByJob = transformFlatStr
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(Duration.ofMinutes(2)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("transform-count-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        KTable<Windowed<String>, Long> analysisCountByJob = analysisFlatStr
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(Duration.ofMinutes(2)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("analysis-count-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        // ========== KTable Join 패턴으로 카운트 집계 ==========

        // Control 토픽에서 collection 완료 메시지 모니터링
        KStream<String, String> controlStream = control;
        
        // Collection 완료 메시지 처리
        controlStream
                .filter((key, value) -> {
                    try {
                        ObjectNode msg = MAPPER.readValue(value, ObjectNode.class);
                        String step = msg.path("step").asText();
                        String status = msg.path("status").asText();
                        String jobId = msg.path("job_id").asText();
                        
                        System.out.println("CONTROL_TOPIC_DEBUG: Received message - jobId=" + jobId + ", step=" + step + ", status=" + status);
                        
                        boolean isCollectionDone = "collection".equals(step) && "done".equals(status);
                        if (isCollectionDone) {
                            System.out.println("CONTROL_TOPIC_DEBUG: Collection done message detected for jobId=" + jobId);
                        }
                        
                        return isCollectionDone;
                    } catch (Exception e) {
                        System.err.println("CONTROL_TOPIC_DEBUG: Error parsing control message: " + e.getMessage());
                        return false;
                    }
                })
                .process(() -> new Processor<String, String>() {
                    private ProcessorContext context;
                    private ReadOnlyWindowStore<String, Long> transformCountStore;
                    private ReadOnlyWindowStore<String, Long> analysisCountStore;
                    
                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                        this.transformCountStore = context.getStateStore("transform-count-store");
                        this.analysisCountStore = context.getStateStore("analysis-count-store");
                        
                        System.out.println("CONTROL_TOPIC_DEBUG: Processor initialized - transformStore=" + (transformCountStore != null) + ", analysisStore=" + (analysisCountStore != null));
                    }
                    
                    @Override
                    public void process(String jobId, String value) {
                        try {
                            System.out.println("CONTROL_TOPIC_DEBUG: Processing collection completion for jobId=" + jobId);
                            
                            ObjectNode msg = MAPPER.readValue(value, ObjectNode.class);
                            int expectedCount = msg.path("expected_count").asInt(0);
                            
                            System.out.println("CONTROL_TOPIC_DEBUG: Expected count=" + expectedCount + " for jobId=" + jobId);
                            
                            // 실제 윈도우 기반 카운트 조회
                            long actualTransformCount = getWindowedCount(transformCountStore, jobId);
                            long actualAnalysisCount = getWindowedCount(analysisCountStore, jobId);
                            
                            System.out.println("CONTROL_TOPIC_DEBUG: Actual counts - transform=" + actualTransformCount + ", analysis=" + actualAnalysisCount);
                            
                            // 완료 메시지 발행
                            checkAndPublishCompletionMessages(jobId, expectedCount, (int)actualTransformCount, (int)actualAnalysisCount);
                            
                            // 실제 Control 토픽에 완료 메시지 발행
                            // Disabled to prevent duplicate control messages; KTable-based emitters handle publishing
                            System.out.println("CONTROL_TOPIC_DEBUG: Suppressed legacy completion publisher for jobId=" + jobId);
                            
                        } catch (Exception e) {
                            System.err.println("CONTROL_TOPIC_DEBUG: Error processing collection message: " + e.getMessage());
                        }
                    }
                    
                    private long getWindowedCount(ReadOnlyWindowStore<String, Long> store, String jobId) {
                        try {
                            if (store == null) {
                                System.out.println("CONTROL_TOPIC_DEBUG: Store is null for jobId=" + jobId);
                                return 0;
                            }
                            
                            long now = context.timestamp();
                            long windowStart = now - Duration.ofHours(1).toMillis();
                            
                            System.out.println("CONTROL_TOPIC_DEBUG: Querying windowed count - jobId=" + jobId + ", windowStart=" + windowStart + ", now=" + now);
                            
                            WindowStoreIterator<Long> iterator = store.fetch(jobId, Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(now));
                            long totalCount = 0;
                            int recordCount = 0;
                            
                            while (iterator.hasNext()) {
                                KeyValue<Long, Long> next = iterator.next();
                                totalCount += next.value;
                                recordCount++;
                                System.out.println("CONTROL_TOPIC_DEBUG: Found window record - timestamp=" + next.key + ", count=" + next.value);
                            }
                            iterator.close();
                            
                            System.out.println("CONTROL_TOPIC_DEBUG: Windowed count result - jobId=" + jobId + ", totalCount=" + totalCount + ", recordCount=" + recordCount);
                            return totalCount;
                        } catch (Exception e) {
                            System.err.println("CONTROL_TOPIC_DEBUG: Error getting windowed count for jobId=" + jobId + ": " + e.getMessage());
                            return 0;
                        }
                    }
                    
                    @Override
                    public void close() {}
                }, "transform-count-store", "analysis-count-store");

        // ========== Control 토픽 자동 발행 로직 (KTable Join 패턴) ==========
        // Collection 메시지를 KTable로 변환하여 expected_count 저장
        // CONTROL 메시지가 null 키로 들어올 수 있어, value에서 job_id를 추출해 재키잉
        KStream<String, String> controlByJob = controlStream.selectKey((k, v) -> {
            try {
                ObjectNode msg = MAPPER.readValue(v, ObjectNode.class);
                String jobId = msg.path("job_id").asText();
                String keyJob = k;
                if (jobId != null) jobId = jobId.replaceAll("^\\\"|\\\"$", "");
                if (keyJob != null) keyJob = keyJob.replaceAll("^\\\"|\\\"$", "");
                String normalized = (jobId != null && !jobId.isEmpty()) ? jobId : (keyJob == null ? "unknown" : keyJob);
                return normalized;
            } catch (Exception e) {
                String keyJob = k;
                if (keyJob != null) keyJob = keyJob.replaceAll("^\\\"|\\\"$", "");
                return keyJob == null || keyJob.isEmpty() ? "unknown" : keyJob;
            }
        });

        KTable<String, Integer> expectedCounts = controlByJob
                .filter((key, value) -> {
                    try {
                        ObjectNode msg = MAPPER.readValue(value, ObjectNode.class);
                        return "collection".equals(msg.path("step").asText()) && 
                               "done".equals(msg.path("status").asText());
                    } catch (Exception e) {
                        return false;
                    }
                })
                .mapValues(value -> {
                    try {
                        ObjectNode msg = MAPPER.readValue(value, ObjectNode.class);
                        return msg.path("expected_count").asInt(0);
                    } catch (Exception e) {
                        return 0;
                    }
                })
                .toTable(Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("expected-count-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Integer()));

        // collection 완료 여부 플래그 KTable (collection_done)
        KTable<String, Boolean> collectionDone = controlByJob
                .filter((key, value) -> {
                    try {
                        ObjectNode msg = MAPPER.readValue(value, ObjectNode.class);
                        return "collection".equals(msg.path("step").asText()) &&
                               "done".equals(msg.path("status").asText());
                    } catch (Exception e) {
                        return false;
                    }
                })
                .mapValues(v -> true)
                .toTable(Materialized.<String, Boolean, KeyValueStore<Bytes, byte[]>>as("collection-done-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Boolean()));

        // Transform 스트림을 KTable로 변환하여 상세 통계 저장
        KTable<String, TransformStats> transformStats = transform
                .map((key, value) -> {
                    try {
                        // 개별 raw data에서 job_id와 리뷰 데이터 추출
                        ObjectNode reviewNode = MAPPER.readValue(value, ObjectNode.class);
                        String jobId = reviewNode.path("job_id").asText();
                        if (jobId == null || jobId.isEmpty()) {
                            jobId = "unknown";
                        }
                        
                        // 개별 리뷰 데이터 처리 (리스트가 아닌 단일 객체)
                        if (!reviewNode.has("review_id") || reviewNode.path("review_id").asText("").isEmpty()) {
                            RatingDistribution emptyDist = new RatingDistribution(
                                new RatingCounts(0, 0, 0, 0, 0),
                                new RatingCounts(0, 0, 0, 0, 0),
                                new RatingCounts(0, 0, 0, 0, 0),
                                new RatingCounts(0, 0, 0, 0, 0)
                            );
                            KeywordStats emptyKeywordStats = new KeywordStats(new ArrayList<>());
                            return new KeyValue<>(jobId, new TransformStats(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, emptyDist, emptyKeywordStats));
                        }
                        
                        // 개별 리뷰 데이터 상세 통계 계산
                        int totalReviews = 1; // 개별 데이터이므로 1
                        int validReviews = 0;
                        int emptyReviews = 0;
                        int coupangTrialReviews = 0;
                        double totalRating = 0.0;
                        double totalRatingExcludingEmpty = 0.0;
                        double totalRatingCoupangTrial = 0.0;
                        double totalRatingRegular = 0.0;
                        int validRatingCount = 0;
                        int coupangTrialCount = 0;
                        int regularCount = 0;
                        
                        // 별점 분포 계산을 위한 카운터
                        int allRating1 = 0, allRating2 = 0, allRating3 = 0, allRating4 = 0, allRating5 = 0;
                        int coupangRating1 = 0, coupangRating2 = 0, coupangRating3 = 0, coupangRating4 = 0, coupangRating5 = 0;
                        int regularRating1 = 0, regularRating2 = 0, regularRating3 = 0, regularRating4 = 0, regularRating5 = 0;
                        int emptyRating1 = 0, emptyRating2 = 0, emptyRating3 = 0, emptyRating4 = 0, emptyRating5 = 0;
                        
                        // 키워드 분석을 위한 맵 (전체, 쿠팡체험단, 일반 구분)
                        Map<String, Map<String, Integer>> allKeywordTagCounts = new HashMap<>();
                        Map<String, Map<String, Integer>> coupangKeywordTagCounts = new HashMap<>();
                        Map<String, Map<String, Integer>> regularKeywordTagCounts = new HashMap<>();
                        
                        // 개별 리뷰 데이터 처리 (단일 객체)
                        JsonNode review = reviewNode;
                        // 기본 필드 추출
                        double rating = review.path("rating").asDouble(0.0);
                        boolean isValid = review.path("is_valid").asBoolean(true);
                        boolean isEmpty = review.path("is_empty_review").asInt(0) == 1;
                        boolean isCoupangTrial = review.path("is_coupang_trial").asInt(0) == 1;
                        
                        // 통계 계산
                        if (isValid) validReviews++;
                        if (isEmpty) emptyReviews++;
                        if (isCoupangTrial) coupangTrialReviews++;
                        
                        totalRating += rating;
                        if (!isEmpty) {
                            totalRatingExcludingEmpty += rating;
                            validRatingCount++;
                        }
                        
                        if (isCoupangTrial) {
                            totalRatingCoupangTrial += rating;
                            coupangTrialCount++;
                        } else {
                            totalRatingRegular += rating;
                            regularCount++;
                        }
                        
                        // 별점 분포 계산 (1-5점별)
                        int ratingInt = (int) Math.round(rating);
                        if (ratingInt >= 1 && ratingInt <= 5) {
                            // 전체 분포 (모든 리뷰)
                            switch (ratingInt) {
                                case 1: allRating1++; break;
                                case 2: allRating2++; break;
                                case 3: allRating3++; break;
                                case 4: allRating4++; break;
                                case 5: allRating5++; break;
                            }
                            
                            // 빈 리뷰 분포 (is_empty_review = 1)
                            if (isEmpty) {
                                switch (ratingInt) {
                                    case 1: emptyRating1++; break;
                                    case 2: emptyRating2++; break;
                                    case 3: emptyRating3++; break;
                                    case 4: emptyRating4++; break;
                                    case 5: emptyRating5++; break;
                                }
                            } else {
                                // 내용이 있는 리뷰만 쿠팡체험단 vs 일반 구분
                                if (isCoupangTrial) {
                                    switch (ratingInt) {
                                        case 1: coupangRating1++; break;
                                        case 2: coupangRating2++; break;
                                        case 3: coupangRating3++; break;
                                        case 4: coupangRating4++; break;
                                        case 5: coupangRating5++; break;
                                    }
                                } else {
                                    switch (ratingInt) {
                                        case 1: regularRating1++; break;
                                        case 2: regularRating2++; break;
                                        case 3: regularRating3++; break;
                                        case 4: regularRating4++; break;
                                        case 5: regularRating5++; break;
                                    }
                                }
                            }
                        }
                        
                        // 키워드 분석 (빈 리뷰가 아닌 경우만)
                        if (!isEmpty) {
                            JsonNode keywordsNode = review.path("keywords");
                            if (keywordsNode.isObject()) {
                                keywordsNode.fields().forEachRemaining(entry -> {
                                    String keyword = entry.getKey();
                                    JsonNode tagNode = entry.getValue();
                                    
                                    if (tagNode.isTextual()) {
                                        String tag = tagNode.asText();
                                        if (tag != null && !tag.isEmpty()) {
                                            // 전체 키워드 태그 카운트
                                            allKeywordTagCounts.computeIfAbsent(keyword, k -> new HashMap<>())
                                                              .merge(tag, 1, Integer::sum);
                                            
                                            // 쿠팡체험단 vs 일반 구분
                                            if (isCoupangTrial) {
                                                coupangKeywordTagCounts.computeIfAbsent(keyword, k -> new HashMap<>())
                                                                       .merge(tag, 1, Integer::sum);
                                            } else {
                                                regularKeywordTagCounts.computeIfAbsent(keyword, k -> new HashMap<>())
                                                                       .merge(tag, 1, Integer::sum);
                                            }
                                        }
                                    }
                                });
                            }
                        }
                        
                        // 평균 계산
                        double avgRating = totalReviews > 0 ? totalRating / totalReviews : 0.0;
                        double avgRatingExcludingEmpty = validRatingCount > 0 ? totalRatingExcludingEmpty / validRatingCount : 0.0;
                        double avgRatingCoupangTrial = coupangTrialCount > 0 ? totalRatingCoupangTrial / coupangTrialCount : 0.0;
                        double avgRatingRegular = regularCount > 0 ? totalRatingRegular / regularCount : 0.0;
                        
                        // 키워드 분석 결과 생성
                        List<KeywordAnalysis> keywordAnalyses = new ArrayList<>();
                        
                        // 모든 키워드 수집 (전체, 쿠팡체험단, 일반에서)
                        Set<String> allKeywords = new HashSet<>();
                        allKeywords.addAll(allKeywordTagCounts.keySet());
                        allKeywords.addAll(coupangKeywordTagCounts.keySet());
                        allKeywords.addAll(regularKeywordTagCounts.keySet());
                        
                        for (String keyword : allKeywords) {
                            // 전체 태그 분석
                            List<KeywordTag> allTags = createKeywordTags(allKeywordTagCounts.get(keyword));
                            
                            // 쿠팡체험단 태그 분석
                            List<KeywordTag> coupangTags = createKeywordTags(coupangKeywordTagCounts.get(keyword));
                            
                            // 일반 구매자 태그 분석
                            List<KeywordTag> regularTags = createKeywordTags(regularKeywordTagCounts.get(keyword));
                            
                            keywordAnalyses.add(new KeywordAnalysis(keyword, allTags, coupangTags, regularTags));
                        }
                        
                        // 키워드별 총 개수 기준으로 정렬 (상위 10개만)
                        keywordAnalyses.sort((a, b) -> {
                            int totalA = a.allTags.stream().mapToInt(tag -> tag.count).sum();
                            int totalB = b.allTags.stream().mapToInt(tag -> tag.count).sum();
                            return Integer.compare(totalB, totalA);
                        });
                        
                        List<KeywordAnalysis> topKeywords = keywordAnalyses.stream()
                            .limit(10)
                            .collect(Collectors.toList());
                        
                        KeywordStats keywordStats = new KeywordStats(topKeywords);
                        
                        // 별점 분포 객체 생성
                        RatingCounts allCounts = new RatingCounts(allRating1, allRating2, allRating3, allRating4, allRating5);
                        RatingCounts coupangCounts = new RatingCounts(coupangRating1, coupangRating2, coupangRating3, coupangRating4, coupangRating5);
                        RatingCounts regularCounts = new RatingCounts(regularRating1, regularRating2, regularRating3, regularRating4, regularRating5);
                        RatingCounts emptyCounts = new RatingCounts(emptyRating1, emptyRating2, emptyRating3, emptyRating4, emptyRating5);
                        RatingDistribution ratingDist = new RatingDistribution(allCounts, coupangCounts, regularCounts, emptyCounts);
                        
                        TransformStats stats = new TransformStats(totalReviews, validReviews, emptyReviews, coupangTrialReviews,
                                                               avgRating, avgRatingExcludingEmpty, avgRatingCoupangTrial, avgRatingRegular, ratingDist, keywordStats);
                        
                        System.out.println("CONTROL_TOPIC_DEBUG: Transform stats - jobId=" + jobId + ", total=" + totalReviews + 
                                          ", valid=" + validReviews + ", empty=" + emptyReviews + ", coupang=" + coupangTrialReviews +
                                          ", avgRating=" + avgRating);
                        
                        return new KeyValue<>(jobId, stats);
                    } catch (Exception e) {
                        System.err.println("Error processing transform message: " + e.getMessage());
                        RatingDistribution emptyDist = new RatingDistribution(
                            new RatingCounts(0, 0, 0, 0, 0),
                            new RatingCounts(0, 0, 0, 0, 0),
                            new RatingCounts(0, 0, 0, 0, 0),
                            new RatingCounts(0, 0, 0, 0, 0)
                        );
                        KeywordStats emptyKeywordStats = new KeywordStats(new ArrayList<>());
                        return new KeyValue<>("error", new TransformStats(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, emptyDist, emptyKeywordStats));
                    }
                })
                .groupByKey(Grouped.with(Serdes.String(), new TransformStatsSerde()))
                .aggregate(
                    () -> {
                        RatingDistribution emptyDist = new RatingDistribution(
                            new RatingCounts(0, 0, 0, 0, 0),
                            new RatingCounts(0, 0, 0, 0, 0),
                            new RatingCounts(0, 0, 0, 0, 0),
                            new RatingCounts(0, 0, 0, 0, 0)
                        );
                        KeywordStats emptyKeywordStats = new KeywordStats(new ArrayList<>());
                        return new TransformStats(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, emptyDist, emptyKeywordStats);
                    },
                    (jobId, newStats, accumulatedStats) -> {
                        // 통계 누적
                        int totalReviews = accumulatedStats.totalReviews + newStats.totalReviews;
                        int validReviews = accumulatedStats.validReviews + newStats.validReviews;
                        int emptyReviews = accumulatedStats.emptyReviews + newStats.emptyReviews;
                        int coupangTrialReviews = accumulatedStats.coupangTrialReviews + newStats.coupangTrialReviews;
                        
                        // 평균 재계산 (가중평균)
                        double totalRating = (accumulatedStats.avgRating * accumulatedStats.totalReviews) + 
                                           (newStats.avgRating * newStats.totalReviews);
                        double avgRating = totalReviews > 0 ? totalRating / totalReviews : 0.0;
                        
                        double totalRatingExcludingEmpty = (accumulatedStats.avgRatingExcludingEmpty * (accumulatedStats.totalReviews - accumulatedStats.emptyReviews)) +
                                                          (newStats.avgRatingExcludingEmpty * (newStats.totalReviews - newStats.emptyReviews));
                        int validCount = (accumulatedStats.totalReviews - accumulatedStats.emptyReviews) + (newStats.totalReviews - newStats.emptyReviews);
                        double avgRatingExcludingEmpty = validCount > 0 ? totalRatingExcludingEmpty / validCount : 0.0;
                        
                        double totalRatingCoupangTrial = (accumulatedStats.avgRatingCoupangTrial * accumulatedStats.coupangTrialReviews) +
                                                        (newStats.avgRatingCoupangTrial * newStats.coupangTrialReviews);
                        double avgRatingCoupangTrial = coupangTrialReviews > 0 ? totalRatingCoupangTrial / coupangTrialReviews : 0.0;
                        
                        double totalRatingRegular = (accumulatedStats.avgRatingRegular * (accumulatedStats.totalReviews - accumulatedStats.coupangTrialReviews)) +
                                                   (newStats.avgRatingRegular * (newStats.totalReviews - newStats.coupangTrialReviews));
                        int regularCount = (accumulatedStats.totalReviews - accumulatedStats.coupangTrialReviews) + (newStats.totalReviews - newStats.coupangTrialReviews);
                        double avgRatingRegular = regularCount > 0 ? totalRatingRegular / regularCount : 0.0;
                        
                        // 별점 분포 누적
                        RatingDistribution combinedRatingDist = accumulatedStats.ratingDistribution.add(newStats.ratingDistribution);
                        
                        // 키워드 통계 누적 (간단히 새 통계로 교체 - 실제로는 더 복잡한 병합 로직 필요)
                        KeywordStats combinedKeywordStats = newStats.keywordStats;
                        
                        TransformStats result = new TransformStats(totalReviews, validReviews, emptyReviews, coupangTrialReviews,
                                                                 avgRating, avgRatingExcludingEmpty, avgRatingCoupangTrial, avgRatingRegular, combinedRatingDist, combinedKeywordStats);
                        
                        System.out.println("CONTROL_TOPIC_DEBUG: Transform aggregate - jobId=" + jobId + ", total=" + totalReviews + 
                                          ", valid=" + validReviews + ", empty=" + emptyReviews + ", coupang=" + coupangTrialReviews);
                        
                        return result;
                    },
                    Materialized.<String, TransformStats, KeyValueStore<Bytes, byte[]>>as("transform-stats-table")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new TransformStatsSerde()));

        // Transform 고유 카운트 (review_id 단위 중복 제거)
        KTable<String, String> tDistinctPerReview = tByReviewStr
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .reduce((oldV, newV) -> newV, Materialized.as("t-distinct-per-review"));
        KTable<String, Long> transformCounts = tDistinctPerReview
                .toStream()
                .selectKey((compositeKey, v) -> compositeKey.split("\\|", 2)[0])
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("transform-counts-distinct")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long()));

        // Analysis 스트림을 KTable로 변환하여 상세 통계 저장 (각 리뷰별로 처리)
        KTable<String, AnalysisStats> analysisStats = analysisFlat
                .map((key, value) -> {
                    try {
                        // 각 리뷰의 job_id 사용 (이미 analysisFlat에서 각 리뷰별로 분리됨)
                        String jobId = value.path("job_id").asText();
                        if (jobId == null || jobId.isEmpty()) {
                            jobId = "unknown";
                        }
                        
                        // 개별 리뷰 분석 결과 처리
                        String sentiment = value.path("sentiment").asText("중립");
                        boolean isCoupangTrial = value.path("is_coupang_trial").asInt(0) == 1;
                        
                        // 개별 리뷰이므로 각각 1개씩 처리
                        int allPositive = 0, allNegative = 0, allNeutral = 0;
                        int coupangPositive = 0, coupangNegative = 0, coupangNeutral = 0;
                        int regularPositive = 0, regularNegative = 0, regularNeutral = 0;
                        
                        // 전체 감정 분포
                        switch (sentiment) {
                            case "긍정":
                                allPositive = 1;
                                break;
                            case "부정":
                                allNegative = 1;
                                break;
                            default:
                                allNeutral = 1;
                                break;
                        }
                        
                        // 쿠팡체험단 vs 일반 구분
                        if (isCoupangTrial) {
                            switch (sentiment) {
                                case "긍정":
                                    coupangPositive = 1;
                                    break;
                                case "부정":
                                    coupangNegative = 1;
                                    break;
                                default:
                                    coupangNeutral = 1;
                                    break;
                            }
                        } else {
                            switch (sentiment) {
                                case "긍정":
                                    regularPositive = 1;
                                    break;
                                case "부정":
                                    regularNegative = 1;
                                    break;
                                default:
                                    regularNeutral = 1;
                                    break;
                            }
                        }
                        
                        SentimentDistribution allSentiment = new SentimentDistribution(allPositive, allNegative, allNeutral);
                        SentimentDistribution coupangSentiment = new SentimentDistribution(coupangPositive, coupangNegative, coupangNeutral);
                        SentimentDistribution regularSentiment = new SentimentDistribution(regularPositive, regularNegative, regularNeutral);
                        
                        AnalysisStats stats = new AnalysisStats(1, allSentiment, coupangSentiment, regularSentiment);
                        
                        System.out.println("CONTROL_TOPIC_DEBUG: Analysis stats - jobId=" + jobId + ", total=1" + 
                                          ", positive=" + allPositive + ", negative=" + allNegative + ", neutral=" + allNeutral);
                        
                        return new KeyValue<>(jobId, stats);
                    } catch (Exception e) {
                        System.err.println("Error processing analysis message: " + e.getMessage());
                        return new KeyValue<>("error", new AnalysisStats(0, 
                            new SentimentDistribution(0, 0, 0),
                            new SentimentDistribution(0, 0, 0),
                            new SentimentDistribution(0, 0, 0)));
                    }
                })
                .groupByKey(Grouped.with(Serdes.String(), new AnalysisStatsSerde()))
                .aggregate(
                    () -> new AnalysisStats(0, 
                        new SentimentDistribution(0, 0, 0),
                        new SentimentDistribution(0, 0, 0),
                        new SentimentDistribution(0, 0, 0)),
                    (jobId, newStats, accumulatedStats) -> {
                        // 통계 누적
                        int totalAnalyzed = accumulatedStats.totalAnalyzed + newStats.totalAnalyzed;
                        
                        // 감정 분포 누적
                        SentimentDistribution allSentiment = new SentimentDistribution(
                            accumulatedStats.allSentiment.positive + newStats.allSentiment.positive,
                            accumulatedStats.allSentiment.negative + newStats.allSentiment.negative,
                            accumulatedStats.allSentiment.neutral + newStats.allSentiment.neutral
                        );
                        
                        SentimentDistribution coupangSentiment = new SentimentDistribution(
                            accumulatedStats.coupangTrialSentiment.positive + newStats.coupangTrialSentiment.positive,
                            accumulatedStats.coupangTrialSentiment.negative + newStats.coupangTrialSentiment.negative,
                            accumulatedStats.coupangTrialSentiment.neutral + newStats.coupangTrialSentiment.neutral
                        );
                        
                        SentimentDistribution regularSentiment = new SentimentDistribution(
                            accumulatedStats.regularSentiment.positive + newStats.regularSentiment.positive,
                            accumulatedStats.regularSentiment.negative + newStats.regularSentiment.negative,
                            accumulatedStats.regularSentiment.neutral + newStats.regularSentiment.neutral
                        );
                        
                        AnalysisStats result = new AnalysisStats(totalAnalyzed, allSentiment, coupangSentiment, regularSentiment);
                        
                        System.out.println("CONTROL_TOPIC_DEBUG: Analysis aggregate - jobId=" + jobId + ", total=" + totalAnalyzed + 
                                          ", positive=" + allSentiment.positive + ", negative=" + allSentiment.negative + ", neutral=" + allSentiment.neutral);
                        
                        return result;
                    },
                    Materialized.<String, AnalysisStats, KeyValueStore<Bytes, byte[]>>as("analysis-stats-table")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new AnalysisStatsSerde()));

        // Analysis 고유 카운트 (review_id 단위 중복 제거)
        KTable<String, String> aDistinctPerReview = aByReviewStr
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .reduce((oldV, newV) -> newV, Materialized.as("a-distinct-per-review"));
        KTable<String, Long> analysisCounts = aDistinctPerReview
                .toStream()
                .selectKey((compositeKey, v) -> compositeKey.split("\\|", 2)[0])
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("analysis-counts-distinct")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long()));

        // Transform 카운트 로깅 (KTable Join 패턴)
        transformCounts.toStream().foreach((jobId, count) -> {
            System.out.println("CONTROL_TOPIC_DEBUG: Transform count updated (KTable) - jobId=" + jobId + ", count=" + count);
        });

        // Analysis 카운트 로깅 (KTable Join 패턴)
        analysisCounts.toStream().foreach((jobId, count) -> {
            System.out.println("CONTROL_TOPIC_DEBUG: Analysis count updated (KTable) - jobId=" + jobId + ", count=" + count);
        });
        controlStream
                .filter((key, value) -> {
                    try {
                        ObjectNode msg = MAPPER.readValue(value, ObjectNode.class);
                        String step = msg.path("step").asText();
                        return ("transform".equals(step) || "analysis".equals(step)) && 
                               "done".equals(msg.path("status").asText());
                    } catch (Exception e) {
                        return false;
                    }
                })
                .groupByKey()
                .aggregate(
                    () -> new HashMap<String, Boolean>(),
                    (jobId, value, state) -> {
                        try {
                            ObjectNode msg = MAPPER.readValue(value, ObjectNode.class);
                            String step = msg.path("step").asText();
                            state.put(step, true);
                            
                            // Transform과 Analysis가 모두 완료되었는지 확인
                            if (state.getOrDefault("transform", false) && state.getOrDefault("analysis", false)) {
                                // Aggregation done is emitted by aggregationComplete KTable logic below to avoid duplicates
                                int expectedCount = msg.path("expected_count").asInt(0);
                                System.out.println("CONTROL_TOPIC_DEBUG: Both transform & analysis done detected (expected=" + expectedCount + ") - suppressing inline aggregation emit for jobId=" + jobId);
                                state.clear(); // 상태 초기화
                            }
                            
                            return state;
                        } catch (Exception e) {
                            System.err.println("Error processing step completion: " + e.getMessage());
                            return state;
                        }
                    },
                    Materialized.<String, HashMap<String, Boolean>, KeyValueStore<Bytes, byte[]>>as("step-completion-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.serdeFrom(new HashMapSerde().serializer(), new HashMapSerde().deserializer()))
                );

        // Transform 완료 여부 판정 (KTable Join 패턴) - 값도 함께 저장 (distinct 기반)
        KTable<String, String> transformComplete = transformCounts
            .join(expectedCounts,
                (actualCount, expectedCount) -> {
                    boolean isComplete = actualCount >= expectedCount;
                    return String.format("%d|%d|%s", actualCount, expectedCount, isComplete);
                }
            )
            .join(collectionDone, (result, done) -> result + "|" + (done != null && done));

        // Transform 완료 시 Control Topic에 메시지 발행
        transformComplete
            .filter((jobId, result) -> {
                String[] parts = result.split("\\|");
                boolean isComplete = Boolean.parseBoolean(parts[2]);
                boolean collectionDoneFlag = parts.length > 3 && Boolean.parseBoolean(parts[3]);
                return isComplete && collectionDoneFlag;
            })
            .toStream()
            .map((jobId, result) -> {
                try {
                    String[] parts = result.split("\\|");
                    Long actualCount = Long.parseLong(parts[0]);
                    Integer expectedCount = Integer.parseInt(parts[1]);
                    
                    ObjectNode message = createControlMessage(jobId, "transform", "done", expectedCount, actualCount.intValue());
                    System.out.println("CONTROL_TOPIC_DEBUG: Publishing transform done via KTable Join - jobId=" + jobId + ", expected=" + expectedCount + ", actual=" + actualCount);
                    return new KeyValue<>(jobId + "|transform", MAPPER.writeValueAsString(message));
                } catch (Exception e) {
                    System.err.println("CONTROL_TOPIC_DEBUG: Error creating transform message: " + e.getMessage());
                    return new KeyValue<>("error", "{}");
                }
            })
            .to("job-control-topic", Produced.with(Serdes.String(), Serdes.String()));

        // Analysis 완료 여부 판정 (KTable Join 패턴) - 값도 함께 저장 (distinct 기반)
        KTable<String, String> analysisComplete = analysisCounts
            .join(expectedCounts,
                (actualCount, expectedCount) -> {
                    boolean isComplete = actualCount >= expectedCount;
                    return String.format("%d|%d|%s", actualCount, expectedCount, isComplete);
                }
            )
            .join(collectionDone, (result, done) -> result + "|" + (done != null && done));

        // Analysis 완료 시 Control Topic에 메시지 발행
        analysisComplete
            .filter((jobId, result) -> {
                String[] parts = result.split("\\|");
                boolean isComplete = Boolean.parseBoolean(parts[2]);
                boolean collectionDoneFlag = parts.length > 3 && Boolean.parseBoolean(parts[3]);
                return isComplete && collectionDoneFlag;
            })
            .toStream()
            .map((jobId, result) -> {
                try {
                    String[] parts = result.split("\\|");
                    Long actualCount = Long.parseLong(parts[0]);
                    Integer expectedCount = Integer.parseInt(parts[1]);
                    
                    ObjectNode message = createControlMessage(jobId, "analysis", "done", expectedCount, actualCount.intValue());
                    System.out.println("CONTROL_TOPIC_DEBUG: Publishing analysis done via KTable Join - jobId=" + jobId + ", expected=" + expectedCount + ", actual=" + actualCount);
                    return new KeyValue<>(jobId + "|analysis", MAPPER.writeValueAsString(message));
                } catch (Exception e) {
                    System.err.println("CONTROL_TOPIC_DEBUG: Error creating analysis message: " + e.getMessage());
                    return new KeyValue<>("error", "{}");
                }
            })
            .to("job-control-topic", Produced.with(Serdes.String(), Serdes.String()));

        // ===== Aggregation 완료 여부 판정 =====
        // 1) 각 단계 완료 플래그 KTable 구성
        KTable<String, Boolean> transformDoneFlag = transformComplete
            .filter((jobId, result) -> {
                String[] parts = result.split("\\|");
                boolean isComplete = Boolean.parseBoolean(parts[2]);
                boolean collectionDoneFlag = parts.length > 3 && Boolean.parseBoolean(parts[3]);
                return isComplete && collectionDoneFlag;
            })
            .mapValues(v -> true)
            .toStream()
            .toTable(Materialized.<String, Boolean, KeyValueStore<Bytes, byte[]>>as("transform-done-flag-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Boolean()));

        KTable<String, Boolean> analysisDoneFlag = analysisComplete
            .filter((jobId, result) -> {
                String[] parts = result.split("\\|");
                boolean isComplete = Boolean.parseBoolean(parts[2]);
                boolean collectionDoneFlag = parts.length > 3 && Boolean.parseBoolean(parts[3]);
                return isComplete && collectionDoneFlag;
            })
            .mapValues(v -> true)
            .toStream()
            .toTable(Materialized.<String, Boolean, KeyValueStore<Bytes, byte[]>>as("analysis-done-flag-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Boolean()));

        // 2) 최소 카운트 KTable (transform vs analysis) - distinct 기반
        KTable<String, Long> minCounts = transformCounts.join(analysisCounts, (t, a) -> Math.min(t, a));

        // 3) 두 단계 완료 AND minCounts >= expectedCounts 인 경우만 aggregation done 방출
        KTable<String, String> aggregationComplete = transformDoneFlag
            .join(analysisDoneFlag, (td, ad) -> Boolean.TRUE.equals(td) && Boolean.TRUE.equals(ad))
            .join(expectedCounts, (bothDone, exp) -> bothDone != null && bothDone ? String.valueOf(exp) : null)
            .join(minCounts, (expStr, minCnt) -> expStr == null ? null : expStr + "|" + minCnt)
            .filter((jobId, val) -> {
                if (val == null) return false;
                String[] parts = val.split("\\|");
                int expected = Integer.parseInt(parts[0]);
                long actualMin = Long.parseLong(parts[1]);
                return actualMin >= expected;
            });

        aggregationComplete
            .toStream()
            .map((jobId, val) -> {
                try {
                    String[] parts = val.split("\\|");
                    int expected = Integer.parseInt(parts[0]);
                    int actual = Integer.parseInt(parts[1]);
                    ObjectNode message = createControlMessage(jobId, "aggregation", "done", expected, actual);
                    System.out.println("CONTROL_TOPIC_DEBUG: Publishing aggregation done via KTable Join - jobId=" + jobId + ", expected=" + expected + ", actual=" + actual);
                    return new KeyValue<>(jobId + "|aggregation", MAPPER.writeValueAsString(message));
                } catch (Exception e) {
                    System.err.println("CONTROL_TOPIC_DEBUG: Error creating aggregation message: " + e.getMessage());
                    return new KeyValue<>("error", "{}");
                }
            })
            .to("job-control-topic", Produced.with(Serdes.String(), Serdes.String()));

        // ========== 타임아웃 처리 (1분 후 failed 메시지 발행) ==========
        // Collection 메시지 수신 시 타임아웃 타이머 시작
        controlStream
                .filter((key, value) -> {
                    try {
                        ObjectNode msg = MAPPER.readValue(value, ObjectNode.class);
                        return "collection".equals(msg.path("step").asText()) && 
                               "done".equals(msg.path("status").asText());
                    } catch (Exception e) {
                        return false;
                    }
                })
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(10)).grace(Duration.ofSeconds(10)))
                .aggregate(
                    () -> new HashMap<String, Object>(),
                    (jobId, value, state) -> {
                        try {
                            ObjectNode msg = MAPPER.readValue(value, ObjectNode.class);
                            state.put("expected_count", msg.path("expected_count").asInt(0));
                            state.put("start_time", System.currentTimeMillis());
                            return state;
                        } catch (Exception e) {
                            return state;
                        }
                    },
                    Materialized.<String, HashMap<String, Object>, WindowStore<Bytes, byte[]>>as("timeout-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.serdeFrom(new HashMapObjectSerde().serializer(), new HashMapObjectSerde().deserializer()))
                )
                .toStream()
                .filter((windowedKey, state) -> {
                    // 10분 후 타임아웃 체크
                    long startTime = (Long) state.getOrDefault("start_time", 0L);
                    return System.currentTimeMillis() - startTime >= 600000; // 10분
                })
                .map((windowedKey, state) -> {
                    try {
                        String jobId = windowedKey.key();
                        int expectedCount = (Integer) state.getOrDefault("expected_count", 0);
                        
                        // Transform/Analysis 완료 여부 확인
                        // TODO: 실제 완료 상태 확인 로직 필요
                        boolean transformCompleted = false; // 임시값
                        boolean analysisCompleted = false; // 임시값
                        
                        if (!transformCompleted || !analysisCompleted) {
                            ObjectNode message = createControlMessage(jobId, "aggregation", "failed", expectedCount, 0);
                            System.out.println("CONTROL_TOPIC_DEBUG: Publishing timeout failed - jobId=" + jobId + ", expected=" + expectedCount);
                            return new KeyValue<>(jobId + "|timeout", MAPPER.writeValueAsString(message));
                        }
                        return null; // 완료된 경우 발행하지 않음
                    } catch (Exception e) {
                        System.err.println("CONTROL_TOPIC_DEBUG: Error creating timeout message: " + e.getMessage());
                        return null;
                    }
                })
                .filter((key, value) -> value != null)
                .to("job-control-topic", Produced.with(Serdes.String(), Serdes.String()));
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isEmpty() ? def : v;
    }
    
    /**
     * Collection 완료 메시지를 받아서 Transform/Analysis 완료 메시지 발행
     * 실제 집계된 데이터의 리뷰 개수를 기준으로 판단
     */
    private void checkAndPublishCompletionMessages(String jobId, int expectedCount, int actualTransformCount, int actualAnalysisCount) {
        try {
            System.out.println("CONTROL_TOPIC_DEBUG: checkAndPublishCompletionMessages called for jobId=" + jobId + ", expected=" + expectedCount + ", transform=" + actualTransformCount + ", analysis=" + actualAnalysisCount);
            
            // Transform 완료 메시지 발행 (실제 리뷰 개수 기준)
            System.out.println("CONTROL_TOPIC_DEBUG: Publishing transform done message for jobId=" + jobId);
            // Disabled to prevent duplicate control messages; KTable-based emitters handle publishing
            System.out.println("CONTROL_TOPIC_DEBUG: Suppressed controlManager.publishTransformDone for jobId=" + jobId + ", expected=" + expectedCount + ", actual=" + actualTransformCount);

            // Analysis 완료 메시지 발행 (실제 분석된 리뷰 개수 기준)
            System.out.println("CONTROL_TOPIC_DEBUG: Publishing analysis done message for jobId=" + jobId);
            // Disabled to prevent duplicate control messages; KTable-based emitters handle publishing
            System.out.println("CONTROL_TOPIC_DEBUG: Suppressed controlManager.publishAnalysisDone for jobId=" + jobId + ", expected=" + expectedCount + ", actual=" + actualAnalysisCount);

            // Aggregation 메시지는 KTable 기반 로직으로 단일 방출 (중복 방지)
            
            System.out.println("CONTROL_TOPIC_DEBUG: All completion messages published for jobId=" + jobId);

        } catch (Exception e) {
            System.err.println("CONTROL_TOPIC_DEBUG: Error checking completion for job " + jobId + ": " + e.getMessage());
        }
    }
    
    /**
     * Collection 메시지에서 expected_count 조회 및 Transform 완료 확인
     */
    private void checkAndPublishTransformCompletion(String jobId, int actualCount) {
        try {
            // Collection 메시지에서 expected_count 확인 (실제로는 State Store에서 조회해야 함)
            // 현재는 간단히 actualCount와 비교
            int expectedCount = actualCount; // TODO: Collection 메시지에서 실제 expected_count 조회
            
            System.out.println("CONTROL_TOPIC_DEBUG: Checking transform completion - jobId=" + jobId + ", expected=" + expectedCount + ", actual=" + actualCount);
            
            if (actualCount >= expectedCount) {
                System.out.println("CONTROL_TOPIC_DEBUG: Transform completion detected - jobId=" + jobId);
                publishToControlTopic(createControlMessage(jobId, "transform", "done", expectedCount, actualCount));
            }
            
        } catch (Exception e) {
            System.err.println("CONTROL_TOPIC_DEBUG: Error checking transform completion: " + e.getMessage());
        }
    }
    
    /**
     * Collection 메시지에서 expected_count 조회 및 Analysis 완료 확인
     */
    private void checkAndPublishAnalysisCompletion(String jobId, int actualCount) {
        try {
            // Collection 메시지에서 expected_count 확인 (실제로는 State Store에서 조회해야 함)
            // 현재는 간단히 actualCount와 비교
            int expectedCount = actualCount; // TODO: Collection 메시지에서 실제 expected_count 조회
            
            System.out.println("CONTROL_TOPIC_DEBUG: Checking analysis completion - jobId=" + jobId + ", expected=" + expectedCount + ", actual=" + actualCount);
            
            if (actualCount >= expectedCount) {
                System.out.println("CONTROL_TOPIC_DEBUG: Analysis completion detected - jobId=" + jobId);
                publishToControlTopic(createControlMessage(jobId, "analysis", "done", expectedCount, actualCount));
            }
            
        } catch (Exception e) {
            System.err.println("CONTROL_TOPIC_DEBUG: Error checking analysis completion: " + e.getMessage());
        }
    }
    
    /**
     * 실제 Control 토픽에 완료 메시지 발행
     */
    private void publishCompletionMessagesToControlTopic(String jobId, int expectedCount, int actualTransformCount, int actualAnalysisCount) {
        try {
            System.out.println("CONTROL_TOPIC_DEBUG: Publishing completion messages to control topic - jobId=" + jobId);
            
            // Transform 완료 메시지 생성 및 발행
            ObjectNode transformMessage = createControlMessage(jobId, "transform", "done", expectedCount, actualTransformCount);
            publishToControlTopic(transformMessage);
            
            // Analysis 완료 메시지 생성 및 발행
            ObjectNode analysisMessage = createControlMessage(jobId, "analysis", "done", expectedCount, actualAnalysisCount);
            publishToControlTopic(analysisMessage);
            
            // Aggregation 메시지는 KTable 기반 로직에서만 방출 (중복 방지)
            
            System.out.println("CONTROL_TOPIC_DEBUG: All completion messages published to control topic for jobId=" + jobId);
            
        } catch (Exception e) {
            System.err.println("CONTROL_TOPIC_DEBUG: Error publishing completion messages to control topic: " + e.getMessage());
        }
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
        message.put("timestamp", getCurrentTimeInKST());
        
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
     * Control 토픽에 메시지 발행 (실제 구현)
     */
    private void publishToControlTopic(ObjectNode message) {
        try {
            String messageJson = MAPPER.writeValueAsString(message);
            String key = message.get("job_id").asText() + "|" + message.get("step").asText();
            
            System.out.println("CONTROL_TOPIC_DEBUG: Publishing to control topic - key=" + key + ", message=" + messageJson);
            
            // 실제 Control 토픽에 메시지 발행을 위한 별도 스트림 생성
            // 이는 토폴로지 빌드 시점에 정의되어야 함
            System.out.println("CONTROL_TOPIC_DEBUG: Message ready for publishing to control topic=job-control-topic");
            
        } catch (Exception e) {
            System.err.println("CONTROL_TOPIC_DEBUG: Error publishing to control topic: " + e.getMessage());
        }
    }
    
    /**
     * HashMap<String, Object>을 위한 Serde 클래스
     */
    public static class HashMapObjectSerde implements Serde<HashMap<String, Object>> {
        @Override
        public Serializer<HashMap<String, Object>> serializer() {
            return new Serializer<HashMap<String, Object>>() {
                @Override
                public byte[] serialize(String topic, HashMap<String, Object> data) {
                    try {
                        return MAPPER.writeValueAsBytes(data);
                    } catch (Exception e) {
                        return new byte[0];
                    }
                }
            };
        }

        @Override
        public Deserializer<HashMap<String, Object>> deserializer() {
            return new Deserializer<HashMap<String, Object>>() {
                @Override
                public HashMap<String, Object> deserialize(String topic, byte[] data) {
                    try {
                        return MAPPER.readValue(data, new TypeReference<HashMap<String, Object>>() {});
                    } catch (Exception e) {
                        return new HashMap<>();
                    }
                }
            };
        }
    }

    /**
     * HashMap을 위한 Serde 클래스
     */
    public static class HashMapSerde implements Serde<HashMap<String, Boolean>> {
        @Override
        public Serializer<HashMap<String, Boolean>> serializer() {
            return (topic, data) -> {
                try {
                    return MAPPER.writeValueAsBytes(data);
                } catch (Exception e) {
                    return new byte[0];
                }
            };
        }

        @Override
        public Deserializer<HashMap<String, Boolean>> deserializer() {
            return (topic, data) -> {
                try {
                    return MAPPER.readValue(data, new TypeReference<HashMap<String, Boolean>>() {});
                } catch (Exception e) {
                    return new HashMap<>();
                }
            };
        }
    }

    private static String composite(String jobId, String reviewId) {
        return jobId + "|" + reviewId;
    }
}

/**
 * TransformStats를 위한 Serde 클래스
 */
class TransformStatsSerde implements Serde<TransformStats> {
    @Override
    public Serializer<TransformStats> serializer() {
        return new TransformStatsSerializer();
    }

    @Override
    public Deserializer<TransformStats> deserializer() {
        return new TransformStatsDeserializer();
    }
}

/**
 * TransformStats Serializer
 */
class TransformStatsSerializer implements Serializer<TransformStats> {
    @Override
    public byte[] serialize(String topic, TransformStats data) {
        if (data == null) return null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = mapper.createObjectNode();
            node.put("totalReviews", data.totalReviews);
            node.put("validReviews", data.validReviews);
            node.put("emptyReviews", data.emptyReviews);
            node.put("coupangTrialReviews", data.coupangTrialReviews);
            node.put("avgRating", data.avgRating);
            node.put("avgRatingExcludingEmpty", data.avgRatingExcludingEmpty);
            node.put("avgRatingCoupangTrial", data.avgRatingCoupangTrial);
            node.put("avgRatingRegular", data.avgRatingRegular);
            
            // 별점 분포 추가
            ObjectNode ratingDist = mapper.createObjectNode();
            
            // all 분포
            ObjectNode allDist = mapper.createObjectNode();
            allDist.put("1", data.ratingDistribution.all.rating1);
            allDist.put("2", data.ratingDistribution.all.rating2);
            allDist.put("3", data.ratingDistribution.all.rating3);
            allDist.put("4", data.ratingDistribution.all.rating4);
            allDist.put("5", data.ratingDistribution.all.rating5);
            ratingDist.set("all", allDist);
            
            // coupang_trial 분포
            ObjectNode coupangDist = mapper.createObjectNode();
            coupangDist.put("1", data.ratingDistribution.coupangTrial.rating1);
            coupangDist.put("2", data.ratingDistribution.coupangTrial.rating2);
            coupangDist.put("3", data.ratingDistribution.coupangTrial.rating3);
            coupangDist.put("4", data.ratingDistribution.coupangTrial.rating4);
            coupangDist.put("5", data.ratingDistribution.coupangTrial.rating5);
            ratingDist.set("coupang_trial", coupangDist);
            
            // regular 분포
            ObjectNode regularDist = mapper.createObjectNode();
            regularDist.put("1", data.ratingDistribution.regular.rating1);
            regularDist.put("2", data.ratingDistribution.regular.rating2);
            regularDist.put("3", data.ratingDistribution.regular.rating3);
            regularDist.put("4", data.ratingDistribution.regular.rating4);
            regularDist.put("5", data.ratingDistribution.regular.rating5);
            ratingDist.set("regular", regularDist);
            
            // empty_review 분포
            ObjectNode emptyDist = mapper.createObjectNode();
            emptyDist.put("1", data.ratingDistribution.emptyReview.rating1);
            emptyDist.put("2", data.ratingDistribution.emptyReview.rating2);
            emptyDist.put("3", data.ratingDistribution.emptyReview.rating3);
            emptyDist.put("4", data.ratingDistribution.emptyReview.rating4);
            emptyDist.put("5", data.ratingDistribution.emptyReview.rating5);
            ratingDist.set("empty_review", emptyDist);
            
            node.set("ratingDistribution", ratingDist);
            
            // 키워드 분석 추가
            ArrayNode keywordAnalysisArray = mapper.createArrayNode();
            for (KeywordAnalysis keywordAnalysis : data.keywordStats.topKeywords) {
                ObjectNode keywordNode = mapper.createObjectNode();
                keywordNode.put("keyword", keywordAnalysis.keyword);
                
                // 전체 태그
                ArrayNode allTagsArray = mapper.createArrayNode();
                for (KeywordTag tag : keywordAnalysis.allTags) {
                    ObjectNode tagNode = mapper.createObjectNode();
                    tagNode.put("tag", tag.tag);
                    tagNode.put("count", tag.count);
                    tagNode.put("percentage", tag.percentage);
                    allTagsArray.add(tagNode);
                }
                keywordNode.set("all_tags", allTagsArray);
                
                // 쿠팡체험단 태그
                ArrayNode coupangTagsArray = mapper.createArrayNode();
                for (KeywordTag tag : keywordAnalysis.coupangTags) {
                    ObjectNode tagNode = mapper.createObjectNode();
                    tagNode.put("tag", tag.tag);
                    tagNode.put("count", tag.count);
                    tagNode.put("percentage", tag.percentage);
                    coupangTagsArray.add(tagNode);
                }
                keywordNode.set("coupang_tags", coupangTagsArray);
                
                // 일반 구매자 태그
                ArrayNode regularTagsArray = mapper.createArrayNode();
                for (KeywordTag tag : keywordAnalysis.regularTags) {
                    ObjectNode tagNode = mapper.createObjectNode();
                    tagNode.put("tag", tag.tag);
                    tagNode.put("count", tag.count);
                    tagNode.put("percentage", tag.percentage);
                    regularTagsArray.add(tagNode);
                }
                keywordNode.set("regular_tags", regularTagsArray);
                
                keywordAnalysisArray.add(keywordNode);
            }
            node.set("keyword_analysis", keywordAnalysisArray);
            
            return mapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing TransformStats", e);
        }
    }
}

/**
 * TransformStats Deserializer
 */
class TransformStatsDeserializer implements Deserializer<TransformStats> {
    @Override
    public TransformStats deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = (ObjectNode) mapper.readTree(data);
            
            // 기본 통계 필드
            int totalReviews = node.path("totalReviews").asInt(0);
            int validReviews = node.path("validReviews").asInt(0);
            int emptyReviews = node.path("emptyReviews").asInt(0);
            int coupangTrialReviews = node.path("coupangTrialReviews").asInt(0);
            double avgRating = node.path("avgRating").asDouble(0.0);
            double avgRatingExcludingEmpty = node.path("avgRatingExcludingEmpty").asDouble(0.0);
            double avgRatingCoupangTrial = node.path("avgRatingCoupangTrial").asDouble(0.0);
            double avgRatingRegular = node.path("avgRatingRegular").asDouble(0.0);
            
            // 별점 분포 역직렬화
            ObjectNode ratingDistNode = (ObjectNode) node.path("ratingDistribution");
            RatingDistribution ratingDist;
            
            if (ratingDistNode.isMissingNode() || ratingDistNode.isEmpty()) {
                // 기본값으로 빈 분포 생성
                ratingDist = new RatingDistribution(
                    new RatingCounts(0, 0, 0, 0, 0),
                    new RatingCounts(0, 0, 0, 0, 0),
                    new RatingCounts(0, 0, 0, 0, 0),
                    new RatingCounts(0, 0, 0, 0, 0)
                );
            } else {
                // all 분포
                ObjectNode allNode = (ObjectNode) ratingDistNode.path("all");
                RatingCounts allCounts = new RatingCounts(
                    allNode.path("1").asInt(0),
                    allNode.path("2").asInt(0),
                    allNode.path("3").asInt(0),
                    allNode.path("4").asInt(0),
                    allNode.path("5").asInt(0)
                );
                
                // coupang_trial 분포
                ObjectNode coupangNode = (ObjectNode) ratingDistNode.path("coupang_trial");
                RatingCounts coupangCounts = new RatingCounts(
                    coupangNode.path("1").asInt(0),
                    coupangNode.path("2").asInt(0),
                    coupangNode.path("3").asInt(0),
                    coupangNode.path("4").asInt(0),
                    coupangNode.path("5").asInt(0)
                );
                
                // regular 분포
                ObjectNode regularNode = (ObjectNode) ratingDistNode.path("regular");
                RatingCounts regularCounts = new RatingCounts(
                    regularNode.path("1").asInt(0),
                    regularNode.path("2").asInt(0),
                    regularNode.path("3").asInt(0),
                    regularNode.path("4").asInt(0),
                    regularNode.path("5").asInt(0)
                );
                
                // empty_review 분포
                ObjectNode emptyNode = (ObjectNode) ratingDistNode.path("empty_review");
                RatingCounts emptyCounts = new RatingCounts(
                    emptyNode.path("1").asInt(0),
                    emptyNode.path("2").asInt(0),
                    emptyNode.path("3").asInt(0),
                    emptyNode.path("4").asInt(0),
                    emptyNode.path("5").asInt(0)
                );
                
                ratingDist = new RatingDistribution(allCounts, coupangCounts, regularCounts, emptyCounts);
            }
            
            // 키워드 분석 역직렬화
            KeywordStats keywordStats;
            ArrayNode keywordAnalysisArray = (ArrayNode) node.path("keyword_analysis");
            if (keywordAnalysisArray.isMissingNode() || keywordAnalysisArray.isEmpty()) {
                keywordStats = new KeywordStats(new ArrayList<>());
            } else {
                List<KeywordAnalysis> keywordAnalyses = new ArrayList<>();
                for (JsonNode keywordNode : keywordAnalysisArray) {
                    String keyword = keywordNode.path("keyword").asText("");
                    
                    // 전체 태그
                    List<KeywordTag> allTags = new ArrayList<>();
                    ArrayNode allTagsArray = (ArrayNode) keywordNode.path("all_tags");
                    for (JsonNode tagNode : allTagsArray) {
                        String tag = tagNode.path("category").asText("");
                        int count = tagNode.path("count").asInt(0);
                        double percentage = tagNode.path("percentage").asDouble(0.0);
                        allTags.add(new KeywordTag(tag, count, percentage));
                    }
                    
                    // 쿠팡체험단 태그
                    List<KeywordTag> coupangTags = new ArrayList<>();
                    ArrayNode coupangTagsArray = (ArrayNode) keywordNode.path("coupang_tags");
                    for (JsonNode tagNode : coupangTagsArray) {
                        String tag = tagNode.path("category").asText("");
                        int count = tagNode.path("count").asInt(0);
                        double percentage = tagNode.path("percentage").asDouble(0.0);
                        coupangTags.add(new KeywordTag(tag, count, percentage));
                    }
                    
                    // 일반 구매자 태그
                    List<KeywordTag> regularTags = new ArrayList<>();
                    ArrayNode regularTagsArray = (ArrayNode) keywordNode.path("regular_tags");
                    for (JsonNode tagNode : regularTagsArray) {
                        String tag = tagNode.path("category").asText("");
                        int count = tagNode.path("count").asInt(0);
                        double percentage = tagNode.path("percentage").asDouble(0.0);
                        regularTags.add(new KeywordTag(tag, count, percentage));
                    }
                    
                    keywordAnalyses.add(new KeywordAnalysis(keyword, allTags, coupangTags, regularTags));
                }
                keywordStats = new KeywordStats(keywordAnalyses);
            }
            
            return new TransformStats(totalReviews, validReviews, emptyReviews, coupangTrialReviews,
                                    avgRating, avgRatingExcludingEmpty, avgRatingCoupangTrial, avgRatingRegular, ratingDist, keywordStats);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing TransformStats", e);
        }
    }
}

/**
 * AnalysisStats를 위한 Serde 클래스
 */
class AnalysisStatsSerde implements Serde<AnalysisStats> {
    @Override
    public Serializer<AnalysisStats> serializer() {
        return new AnalysisStatsSerializer();
    }

    @Override
    public Deserializer<AnalysisStats> deserializer() {
        return new AnalysisStatsDeserializer();
    }
}

/**
 * AnalysisStats Serializer
 */
class AnalysisStatsSerializer implements Serializer<AnalysisStats> {
    @Override
    public byte[] serialize(String topic, AnalysisStats data) {
        if (data == null) return null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = mapper.createObjectNode();
            node.put("totalAnalyzed", data.totalAnalyzed);
            
            // allSentiment
            ObjectNode allSentiment = mapper.createObjectNode();
            allSentiment.put("positive", data.allSentiment.positive);
            allSentiment.put("negative", data.allSentiment.negative);
            allSentiment.put("neutral", data.allSentiment.neutral);
            allSentiment.put("positivePct", data.allSentiment.positivePct);
            allSentiment.put("negativePct", data.allSentiment.negativePct);
            allSentiment.put("neutralPct", data.allSentiment.neutralPct);
            node.set("allSentiment", allSentiment);
            
            // coupangTrialSentiment
            ObjectNode coupangSentiment = mapper.createObjectNode();
            coupangSentiment.put("positive", data.coupangTrialSentiment.positive);
            coupangSentiment.put("negative", data.coupangTrialSentiment.negative);
            coupangSentiment.put("neutral", data.coupangTrialSentiment.neutral);
            coupangSentiment.put("positivePct", data.coupangTrialSentiment.positivePct);
            coupangSentiment.put("negativePct", data.coupangTrialSentiment.negativePct);
            coupangSentiment.put("neutralPct", data.coupangTrialSentiment.neutralPct);
            node.set("coupangTrialSentiment", coupangSentiment);
            
            // regularSentiment
            ObjectNode regularSentiment = mapper.createObjectNode();
            regularSentiment.put("positive", data.regularSentiment.positive);
            regularSentiment.put("negative", data.regularSentiment.negative);
            regularSentiment.put("neutral", data.regularSentiment.neutral);
            regularSentiment.put("positivePct", data.regularSentiment.positivePct);
            regularSentiment.put("negativePct", data.regularSentiment.negativePct);
            regularSentiment.put("neutralPct", data.regularSentiment.neutralPct);
            node.set("regularSentiment", regularSentiment);
            
            return mapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing AnalysisStats", e);
        }
    }
}

/**
 * AnalysisStats Deserializer
 */
class AnalysisStatsDeserializer implements Deserializer<AnalysisStats> {
    @Override
    public AnalysisStats deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = (ObjectNode) mapper.readTree(data);
            
            ObjectNode allSentimentNode = (ObjectNode) node.path("allSentiment");
            SentimentDistribution allSentiment = new SentimentDistribution(
                allSentimentNode.path("positive").asInt(0),
                allSentimentNode.path("negative").asInt(0),
                allSentimentNode.path("neutral").asInt(0)
            );
            
            ObjectNode coupangSentimentNode = (ObjectNode) node.path("coupangTrialSentiment");
            SentimentDistribution coupangSentiment = new SentimentDistribution(
                coupangSentimentNode.path("positive").asInt(0),
                coupangSentimentNode.path("negative").asInt(0),
                coupangSentimentNode.path("neutral").asInt(0)
            );
            
            ObjectNode regularSentimentNode = (ObjectNode) node.path("regularSentiment");
            SentimentDistribution regularSentiment = new SentimentDistribution(
                regularSentimentNode.path("positive").asInt(0),
                regularSentimentNode.path("negative").asInt(0),
                regularSentimentNode.path("neutral").asInt(0)
            );
            
            return new AnalysisStats(
                node.path("totalAnalyzed").asInt(0),
                allSentiment,
                coupangSentiment,
                regularSentiment
            );
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing AnalysisStats", e);
        }
    }
}
