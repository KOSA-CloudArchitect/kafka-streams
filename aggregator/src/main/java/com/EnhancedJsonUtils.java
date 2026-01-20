package com.example.aggregator.enhanced;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 향상된 JSON 처리 유틸리티 클래스
 * 기존 JsonUtils 기능을 확장하여 Control 토픽 관리 및 고급 집계 기능 제공
 */
public class EnhancedJsonUtils {
    private EnhancedJsonUtils() {}
    
    /**
     * 한국 시간(KST)으로 현재 시간을 ISO 8601 형식으로 반환
     */
    private static String getCurrentTimeInKST() {
        ZoneId kstZone = ZoneId.of("Asia/Seoul");
        ZonedDateTime now = ZonedDateTime.now(kstZone);
        return now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
    
    // ========== 기존 JsonUtils 기능 유지 ==========
    
    public static List<JsonNode> safeParseArray(ObjectMapper mapper, String json, String arrayField, boolean withJobId) {
        try {
            JsonNode root = mapper.readTree(json);
            JsonNode arr = root.path(arrayField);
            if (!arr.isArray()) return Collections.emptyList();
            String jobId = withJobId ? root.path("job_id").asText("") : "";
            List<JsonNode> out = new ArrayList<>();
            for (JsonNode node : arr) {
                if (withJobId && node instanceof ObjectNode) {
                    ObjectNode on = (ObjectNode) node;
                    on.put("job_id", jobId);
                }
                out.add(node);
            }
            return out;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }
    
    public static ObjectNode joinReview(ObjectMapper mapper, JsonNode t, JsonNode a) {
        ObjectNode out = mapper.createObjectNode();
        out.put("job_id", t.path("job_id").asText(""));
        out.put("review_id", t.path("review_id").asText(""));
        out.set("keywords", t.path("keywords"));
        out.set("rating", t.path("rating"));
        out.set("clean_text", t.path("clean_text"));
        out.set("summary", a.path("summary"));
        out.set("sentiment", a.path("sentiment"));
        
        // 추가 필드들 매핑
        out.set("product_id", t.path("product_id"));
        out.set("title", t.path("title"));
        out.set("category", t.path("category"));
        out.set("review_count", t.path("review_count"));
        out.set("sales_price", t.path("sales_price"));
        out.set("final_price", t.path("final_price"));
        out.set("review_date", t.path("review_date"));
        out.set("review_text", t.path("review_text"));
        out.set("review_help_count", t.path("review_help_count"));
        out.set("crawled_at", t.path("crawled_at"));
        out.set("is_coupang_trial", t.path("is_coupang_trial"));
        out.set("is_empty_review", t.path("is_empty_review"));
        out.set("is_valid_rating", t.path("is_valid_rating"));
        out.set("is_valid_date", t.path("is_valid_date"));
        out.set("has_content", t.path("has_content"));
        out.set("is_valid", t.path("is_valid"));
        out.set("invalid_reason", t.path("invalid_reason"));
        out.set("year", t.path("year"));
        out.set("month", t.path("month"));
        out.set("day", t.path("day"));
        out.set("quarter", t.path("quarter"));
        out.set("yyyymm", t.path("yyyymm"));
        out.set("yyyymmdd", t.path("yyyymmdd"));
        out.set("weekday", t.path("weekday"));
        
        return out;
    }
    
    public static String toString(ObjectMapper mapper, ObjectNode node) {
        try { 
            return mapper.writeValueAsString(node); 
        } catch (JsonProcessingException e) { 
            return "{}"; 
        }
    }
    
    public static ObjectNode parseObjectNode(ObjectMapper mapper, String json) {
        try { 
            return (ObjectNode) mapper.readTree(json); 
        } catch (Exception e) { 
            return mapper.createObjectNode(); 
        }
    }
    
    public static String extractJobId(ObjectMapper mapper, String json) {
        try { 
            return mapper.readTree(json).path("job_id").asText(""); 
        } catch (Exception e) { 
            return ""; 
        }
    }
    
    // ========== 새로운 향상된 기능 ==========
    
    /**
     * 개별 raw data를 처리하는 메서드 (리스트가 아닌 단일 객체)
     * Transform 토픽에서 개별 리뷰 데이터가 올 때 사용
     */
    public static JsonNode parseSingleReview(ObjectMapper mapper, String json) {
        try {
            JsonNode root = mapper.readTree(json);
            
            // 개별 리뷰 데이터인지 확인
            if (root.has("review_id") && root.has("rating")) {
                return root;
            } else {
                // 리스트 형태인 경우 첫 번째 요소 반환
                if (root.has("reviews") && root.path("reviews").isArray()) {
                    JsonNode reviews = root.path("reviews");
                    if (reviews.size() > 0) {
                        ObjectNode firstReview = (ObjectNode) reviews.get(0);
                        firstReview.put("job_id", root.path("job_id").asText(""));
                        return firstReview;
                    }
                }
            }
            
            // 빈 객체 반환
            ObjectNode emptyNode = mapper.createObjectNode();
            emptyNode.put("job_id", "unknown");
            emptyNode.put("review_id", "unknown");
            return emptyNode;
        } catch (Exception e) {
            ObjectNode errorNode = mapper.createObjectNode();
            errorNode.put("job_id", "error");
            errorNode.put("review_id", "error");
            return errorNode;
        }
    }
    
    /**
     * 개별 분석 결과를 처리하는 메서드 (리스트가 아닌 단일 객체)
     * Analysis 토픽에서 개별 분석 결과가 올 때 사용
     */
    public static JsonNode parseSingleAnalysis(ObjectMapper mapper, String json) {
        try {
            JsonNode root = mapper.readTree(json);
            
            // 개별 분석 결과인지 확인
            if (root.has("review_id") && root.has("sentiment")) {
                return root;
            } else {
                // 리스트 형태인 경우 첫 번째 요소 반환
                if (root.has("results") && root.path("results").isArray()) {
                    JsonNode results = root.path("results");
                    if (results.size() > 0) {
                        ObjectNode firstResult = (ObjectNode) results.get(0);
                        firstResult.put("job_id", root.path("job_id").asText(""));
                        return firstResult;
                    }
                }
            }
            
            // 빈 객체 반환
            ObjectNode emptyNode = mapper.createObjectNode();
            emptyNode.put("job_id", "unknown");
            emptyNode.put("review_id", "unknown");
            return emptyNode;
        } catch (Exception e) {
            ObjectNode errorNode = mapper.createObjectNode();
            errorNode.put("job_id", "error");
            errorNode.put("review_id", "error");
            return errorNode;
        }
    }
    
    /**
     * Transform 데이터 집계용 빈 집계 객체 생성
     */
    public static ObjectNode emptyTransformAgg(ObjectMapper mapper) {
        ObjectNode agg = mapper.createObjectNode();
        
        // job_id 초기화
        agg.put("job_id", "");
        
        // 기본 통계
        ObjectNode stats = mapper.createObjectNode();
        stats.put("total_reviews", 0);
        stats.put("valid_reviews", 0);
        stats.put("empty_reviews", 0);
        stats.put("coupang_trial_reviews", 0);
        stats.put("avg_rating", 0.0);
        stats.put("avg_rating_excluding_empty", 0.0);
        stats.put("avg_rating_coupang_trial", 0.0);
        stats.put("avg_rating_regular", 0.0);
        agg.set("transform_stats", stats);
        
        // 별점 분포
        ObjectNode ratingDist = mapper.createObjectNode();
        ObjectNode allDist = mapper.createObjectNode();
        ObjectNode coupangDist = mapper.createObjectNode();
        ObjectNode regularDist = mapper.createObjectNode();
        
        for (int i = 1; i <= 5; i++) {
            allDist.put(String.valueOf(i), 0);
            coupangDist.put(String.valueOf(i), 0);
            regularDist.put(String.valueOf(i), 0);
        }
        
        ratingDist.set("all", allDist);
        ratingDist.set("coupang_trial", coupangDist);
        ratingDist.set("regular", regularDist);
        agg.set("rating_distribution", ratingDist);
        
        // 키워드 분석
        agg.set("keyword_analysis", mapper.createArrayNode());
        
        return agg;
    }
    
    /**
     * Analysis 데이터 집계용 빈 집계 객체 생성
     */
    public static ObjectNode emptyAnalysisAgg(ObjectMapper mapper) {
        ObjectNode agg = mapper.createObjectNode();
        
        // job_id 초기화
        agg.put("job_id", "");
        
        ObjectNode stats = mapper.createObjectNode();
        stats.put("total_analyzed", 0);
        
        ObjectNode sentimentDist = mapper.createObjectNode();
        ObjectNode allSentiment = mapper.createObjectNode();
        ObjectNode coupangSentiment = mapper.createObjectNode();
        ObjectNode regularSentiment = mapper.createObjectNode();
        
        // 감정별 카운트
        allSentiment.put("긍정", 0);
        allSentiment.put("부정", 0);
        allSentiment.put("중립", 0);
        allSentiment.put("긍정_pct", 0.0);
        allSentiment.put("부정_pct", 0.0);
        allSentiment.put("중립_pct", 0.0);
        
        coupangSentiment.put("긍정", 0);
        coupangSentiment.put("부정", 0);
        coupangSentiment.put("중립", 0);
        coupangSentiment.put("긍정_pct", 0.0);
        coupangSentiment.put("부정_pct", 0.0);
        coupangSentiment.put("중립_pct", 0.0);
        
        regularSentiment.put("긍정", 0);
        regularSentiment.put("부정", 0);
        regularSentiment.put("중립", 0);
        regularSentiment.put("긍정_pct", 0.0);
        regularSentiment.put("부정_pct", 0.0);
        regularSentiment.put("중립_pct", 0.0);
        
        sentimentDist.set("all", allSentiment);
        sentimentDist.set("coupang_trial", coupangSentiment);
        sentimentDist.set("regular", regularSentiment);
        
        stats.set("sentiment_distribution", sentimentDist);
        agg.set("analysis_stats", stats);
        
        return agg;
    }
    
    /**
     * Transform 데이터 집계 처리
     */
    public static ObjectNode accumulateTransform(ObjectMapper mapper, ObjectNode acc, ObjectNode row) {
        // job_id 설정 (첫 번째 레코드에서)
        if (acc.path("job_id").asText("").isEmpty()) {
            acc.put("job_id", row.path("job_id").asText(""));
        }
        
        ObjectNode stats = (ObjectNode) acc.path("transform_stats");
        ObjectNode ratingDist = (ObjectNode) acc.path("rating_distribution");
        ArrayNode keywordAnalysis = (ArrayNode) acc.path("keyword_analysis");
        
        // 기본 카운트 증가
        int totalReviews = stats.path("total_reviews").asInt(0) + 1;
        stats.put("total_reviews", totalReviews);
        
        // 유효성 검사
        boolean isValid = row.path("is_valid").asBoolean(true);
        boolean isEmpty = row.path("is_empty_review").asInt(0) == 1;
        boolean isCoupangTrial = row.path("is_coupang_trial").asInt(0) == 1;
        
        if (isValid) {
            stats.put("valid_reviews", stats.path("valid_reviews").asInt(0) + 1);
        }
        
        if (isEmpty) {
            stats.put("empty_reviews", stats.path("empty_reviews").asInt(0) + 1);
        }
        
        if (isCoupangTrial) {
            stats.put("coupang_trial_reviews", stats.path("coupang_trial_reviews").asInt(0) + 1);
        }
        
        // 별점 처리
        double rating = row.path("rating").asDouble(0.0);
        if (rating > 0) {
            int ratingInt = (int) Math.round(rating);
            if (ratingInt >= 1 && ratingInt <= 5) {
                // 전체 분포 업데이트
                ObjectNode allDist = (ObjectNode) ratingDist.path("all");
                allDist.put(String.valueOf(ratingInt), allDist.path(String.valueOf(ratingInt)).asInt(0) + 1);
                
                // 쿠팡체험단/일반 사용자 분포 업데이트
                if (isCoupangTrial) {
                    ObjectNode coupangDist = (ObjectNode) ratingDist.path("coupang_trial");
                    coupangDist.put(String.valueOf(ratingInt), coupangDist.path(String.valueOf(ratingInt)).asInt(0) + 1);
                } else {
                    ObjectNode regularDist = (ObjectNode) ratingDist.path("regular");
                    regularDist.put(String.valueOf(ratingInt), regularDist.path(String.valueOf(ratingInt)).asInt(0) + 1);
                }
            }
        }
        
        // 평균 별점 계산
        updateAverageRatings(stats, ratingDist);
        
        // 키워드 분석
        if (row.has("keywords") && row.path("keywords").isObject()) {
            updateKeywordAnalysis(mapper, keywordAnalysis, (ObjectNode) row.path("keywords"), isCoupangTrial);
        }
        
        return acc;
    }
    
    /**
     * Analysis 데이터 집계 처리
     */
    public static ObjectNode accumulateAnalysis(ObjectMapper mapper, ObjectNode acc, ObjectNode row) {
        // job_id 설정 (첫 번째 레코드에서)
        if (acc.path("job_id").asText("").isEmpty()) {
            acc.put("job_id", row.path("job_id").asText(""));
        }
        
        ObjectNode stats = (ObjectNode) acc.path("analysis_stats");
        ObjectNode sentimentDist = (ObjectNode) stats.path("sentiment_distribution");
        
        // 총 분석 수 증가
        stats.put("total_analyzed", stats.path("total_analyzed").asInt(0) + 1);
        
        // 감정 분석 결과 처리
        String sentiment = row.path("sentiment").asText("");
        boolean isCoupangTrial = row.path("is_coupang_trial").asInt(0) == 1;
        
        if (!sentiment.isEmpty()) {
            // 전체 감정 분포 업데이트
            ObjectNode allSentiment = (ObjectNode) sentimentDist.path("all");
            updateSentimentCount(allSentiment, sentiment);
            
            // 쿠팡체험단/일반 사용자 감정 분포 업데이트
            if (isCoupangTrial) {
                ObjectNode coupangSentiment = (ObjectNode) sentimentDist.path("coupang_trial");
                updateSentimentCount(coupangSentiment, sentiment);
            } else {
                ObjectNode regularSentiment = (ObjectNode) sentimentDist.path("regular");
                updateSentimentCount(regularSentiment, sentiment);
            }
        }
        
        // 감정별 비율 계산
        updateSentimentPercentages(sentimentDist);
        
        return acc;
    }
    
    /**
     * 평균 별점 계산 및 업데이트
     */
    private static void updateAverageRatings(ObjectNode stats, ObjectNode ratingDist) {
        ObjectNode allDist = (ObjectNode) ratingDist.path("all");
        ObjectNode coupangDist = (ObjectNode) ratingDist.path("coupang_trial");
        ObjectNode regularDist = (ObjectNode) ratingDist.path("regular");
        
        // 전체 평균 별점
        double allAvg = calculateAverageRating(allDist);
        stats.put("avg_rating", allAvg);
        
        // 빈 리뷰 제외 평균 별점 (유효한 리뷰만)
        int validReviews = stats.path("valid_reviews").asInt(0);
        if (validReviews > 0) {
            stats.put("avg_rating_excluding_empty", allAvg);
        }
        
        // 쿠팡체험단 평균 별점
        int coupangCount = stats.path("coupang_trial_reviews").asInt(0);
        if (coupangCount > 0) {
            double coupangAvg = calculateAverageRating(coupangDist);
            stats.put("avg_rating_coupang_trial", coupangAvg);
        }
        
        // 일반 사용자 평균 별점
        int regularCount = stats.path("total_reviews").asInt(0) - coupangCount;
        if (regularCount > 0) {
            double regularAvg = calculateAverageRating(regularDist);
            stats.put("avg_rating_regular", regularAvg);
        }
    }
    
    /**
     * 별점 분포에서 평균 계산
     */
    private static double calculateAverageRating(ObjectNode ratingDist) {
        int totalCount = 0;
        double totalSum = 0.0;
        
        for (int i = 1; i <= 5; i++) {
            int count = ratingDist.path(String.valueOf(i)).asInt(0);
            totalCount += count;
            totalSum += count * i;
        }
        
        return totalCount > 0 ? totalSum / totalCount : 0.0;
    }
    
    /**
     * 감정별 카운트 업데이트
     */
    private static void updateSentimentCount(ObjectNode sentimentNode, String sentiment) {
        switch (sentiment) {
            case "긍정":
                sentimentNode.put("긍정", sentimentNode.path("긍정").asInt(0) + 1);
                break;
            case "부정":
                sentimentNode.put("부정", sentimentNode.path("부정").asInt(0) + 1);
                break;
            case "중립":
                sentimentNode.put("중립", sentimentNode.path("중립").asInt(0) + 1);
                break;
        }
    }
    
    /**
     * 감정별 비율 계산
     */
    private static void updateSentimentPercentages(ObjectNode sentimentDist) {
        String[] categories = {"all", "coupang_trial", "regular"};
        
        for (String category : categories) {
            ObjectNode sentimentNode = (ObjectNode) sentimentDist.path(category);
            int pos = sentimentNode.path("긍정").asInt(0);
            int neg = sentimentNode.path("부정").asInt(0);
            int neu = sentimentNode.path("중립").asInt(0);
            int total = pos + neg + neu;
            
            if (total > 0) {
                sentimentNode.put("긍정_pct", pos * 100.0 / total);
                sentimentNode.put("부정_pct", neg * 100.0 / total);
                sentimentNode.put("중립_pct", neu * 100.0 / total);
            }
        }
    }
    
    /**
     * 키워드 분석 업데이트
     */
    private static void updateKeywordAnalysis(ObjectMapper mapper, ArrayNode keywordAnalysis, ObjectNode keywords, boolean isCoupangTrial) {
        Iterator<String> fieldNames = keywords.fieldNames();
        
        while (fieldNames.hasNext()) {
            String keywordName = fieldNames.next();
            String tagValue = keywords.path(keywordName).asText("");
            
            if (tagValue.isEmpty()) continue;
            
            // 기존 키워드 찾기
            ObjectNode existingKeyword = null;
            for (JsonNode node : keywordAnalysis) {
                if (node.path("name").asText("").equals(keywordName)) {
                    existingKeyword = (ObjectNode) node;
                    break;
                }
            }
            
            // 키워드가 없으면 새로 생성
            if (existingKeyword == null) {
                existingKeyword = mapper.createObjectNode();
                existingKeyword.put("name", keywordName);
                existingKeyword.set("items", mapper.createArrayNode());
                keywordAnalysis.add(existingKeyword);
            }
            
            // 태그 아이템 찾기
            ArrayNode items = (ArrayNode) existingKeyword.path("items");
            ObjectNode existingItem = null;
            for (JsonNode item : items) {
                if (item.path("category").asText("").equals(tagValue)) {
                    existingItem = (ObjectNode) item;
                    break;
                }
            }
            
            // 태그 아이템이 없으면 새로 생성
            if (existingItem == null) {
                existingItem = mapper.createObjectNode();
                existingItem.put("tag", tagValue);
                existingItem.put("count", 0);
                existingItem.put("percentage", 0.0);
                items.add(existingItem);
            }
            
            // 카운트 증가
            existingItem.put("count", existingItem.path("count").asInt(0) + 1);
            
            // 비율 계산 (전체 키워드별 총 카운트 필요)
            int totalCount = 0;
            for (JsonNode item : items) {
                totalCount += item.path("count").asInt(0);
            }
            if (totalCount > 0) {
                existingItem.put("percentage", existingItem.path("count").asInt(0) * 100.0 / totalCount);
            }
        }
    }
    
    /**
     * 통합 집계 결과 생성
     */
    public static ObjectNode createCombinedAggregation(ObjectMapper mapper, ObjectNode transformAgg, ObjectNode analysisAgg) {
        ObjectNode combined = mapper.createObjectNode();
        
        // 기본 정보
        String jobId = transformAgg.path("job_id").asText("");
        combined.put("_id", jobId);  // MongoDB용 _id 필드 추가
        combined.put("job_id", jobId);
        combined.put("timestamp", getCurrentTimeInKST());
        
        // Transform 집계 결과
        combined.set("transform_stats", transformAgg.path("transform_stats"));
        combined.set("rating_distribution", transformAgg.path("rating_distribution"));
        combined.set("keyword_analysis", transformAgg.path("keyword_analysis"));
        
        // Analysis 집계 결과
        combined.set("analysis_stats", analysisAgg.path("analysis_stats"));
        
        // 통합 통계
        ObjectNode combinedStats = mapper.createObjectNode();
        ObjectNode transformStats = (ObjectNode) transformAgg.path("transform_stats");
        ObjectNode analysisStats = (ObjectNode) analysisAgg.path("analysis_stats");
        
        int totalReviews = transformStats.path("total_reviews").asInt(0);
        int analyzedReviews = analysisStats.path("total_analyzed").asInt(0);
        
        combinedStats.put("total_reviews", totalReviews);
        combinedStats.put("analyzed_reviews", analyzedReviews);
        combinedStats.put("analysis_coverage", totalReviews > 0 ? analyzedReviews * 100.0 / totalReviews : 0.0);
        combinedStats.put("avg_rating", transformStats.path("avg_rating").asDouble(0.0));
        
        // 주요 감정 분석
        ObjectNode sentimentDist = (ObjectNode) analysisStats.path("sentiment_distribution").path("all");
        String dominantSentiment = getDominantSentiment(sentimentDist);
        combinedStats.put("dominant_sentiment", dominantSentiment);
        
        // 감정-별점 상관관계 (간단한 계산)
        double avgRating = transformStats.path("avg_rating").asDouble(0.0);
        double positiveRatio = sentimentDist.path("긍정_pct").asDouble(0.0);
        combinedStats.put("sentiment_rating_correlation", calculateSimpleCorrelation(avgRating, positiveRatio));
        
        combined.set("combined_stats", combinedStats);
        
        return combined;
    }
    
    /**
     * 주요 감정 분석
     */
    private static String getDominantSentiment(ObjectNode sentimentDist) {
        double pos = sentimentDist.path("긍정_pct").asDouble(0.0);
        double neg = sentimentDist.path("부정_pct").asDouble(0.0);
        double neu = sentimentDist.path("중립_pct").asDouble(0.0);
        
        if (pos >= neg && pos >= neu) return "긍정";
        if (neg >= pos && neg >= neu) return "부정";
        return "중립";
    }
    
    /**
     * 간단한 상관관계 계산
     */
    private static double calculateSimpleCorrelation(double avgRating, double positiveRatio) {
        // 별점이 높을수록 긍정 비율이 높다는 가정
        // 실제로는 더 복잡한 통계 계산이 필요
        return avgRating * positiveRatio / 100.0;
    }
    
    private static double parseDouble(String s) {
        try { 
            return Double.parseDouble(s); 
        } catch (Exception e) { 
            return 0.0; 
        }
    }
}
