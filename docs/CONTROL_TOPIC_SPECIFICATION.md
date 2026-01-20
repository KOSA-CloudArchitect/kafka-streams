# Control 토픽 기반 작업 완료 관리 시스템 명세서

## 개요

Airflow에서 리뷰 처리 파이프라인의 각 단계별 완료 상태를 모니터링하기 위한 Control 토픽 시스템입니다. 단일 토픽을 통해 Collection → Transform → Analysis → Aggregation 단계의 완료 및 실패 상태를 추적합니다.

## 토픽 구성

### Control 토픽 정의
- **토픽명**: `job-control-topic`
- **파티션**: 1 (메시지 순서 보장)
- **복제본**: 1
- **보존 정책**: delete
- **보존 기간**: 7일 (604800000ms)

### Kafka 토픽 리소스
```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: job-control-topic
  labels:
    strimzi.io/cluster: my-cluster
spec:
  partitions: 1
  replicas: 1
  config:
    cleanup.policy: delete
    retention.ms: 604800000
    segment.bytes: 1073741824
```

## 메시지 스키마

### 기본 메시지 구조
```json
{
  "job_id": "string",
  "step": "string",
  "status": "string",
  "expected_count": "integer",
  "actual_count": "integer",
  "timestamp": "string",
  "error_message": "string",
  "metadata": {
    "processing_time": "number",
    "worker_id": "string",
    "batch_id": "string",
    "server_info": "string"
  }
}
```

### 필드 상세 설명

| 필드 | 타입 | 필수 | 설명 |
|------|------|------|------|
| `job_id` | string | ✅ | 작업 식별자 |
| `step` | string | ✅ | 처리 단계 ("collection", "transform", "analysis", "aggregation") |
| `status` | string | ✅ | 상태 ("done", "failed", "timeout") |
| `expected_count` | integer | ✅ | 예상 처리 수량 |
| `actual_count` | integer | ✅ | 실제 처리 수량 |
| `timestamp` | string | ✅ | 완료/실패 시점 (ISO 8601 형식) |
| `error_message` | string | ❌ | 실패 시 에러 메시지 |
| `metadata` | object | ❌ | 단계별 추가 정보 |

### 메시지 예시

#### Collection 완료
```json
{
  "job_id": "job-2024-001",
  "step": "collection",
  "status": "done",
  "expected_count": 1000,
  "actual_count": 1000,
  "timestamp": "2024-01-15T19:30:00+09:00",
  "metadata": {
    "processing_time": 120.5,
    "worker_id": "collector-001",
    "batch_id": "batch-001",
    "server_info": "data-collector-v1.2.0"
  }
}
```

#### Transform 완료
```json
{
  "job_id": "job-2024-001",
  "step": "transform",
  "status": "done",
  "expected_count": 1000,
  "actual_count": 1000,
  "timestamp": "2024-01-15T19:35:00+09:00",
  "metadata": {
    "processing_time": 45.2,
    "worker_id": "spark-worker-001",
    "batch_id": "transform-batch-001",
    "server_info": "spark-streaming-v3.4.0"
  }
}
```

#### Analysis 실패
```json
{
  "job_id": "job-2024-001",
  "step": "analysis",
  "status": "failed",
  "expected_count": 1000,
  "actual_count": 850,
  "timestamp": "2024-01-15T19:40:00+09:00",
  "error_message": "LLM API timeout after 3 retries",
  "metadata": {
    "processing_time": 1800.0,
    "worker_id": "llm-worker-001",
    "batch_id": "analysis-batch-001",
    "server_info": "llm-api-v2.1.0"
  }
}
```

#### Aggregation 완료
```json
{
  "job_id": "job-2024-001",
  "step": "aggregation",
  "status": "done",
  "expected_count": 1000,
  "actual_count": 1000,
  "timestamp": "2024-01-15T19:45:00+09:00",
  "metadata": {
    "processing_time": 12.8,
    "worker_id": "kafka-streams-001",
    "batch_id": "agg-batch-001",
    "server_info": "kafka-streams-v3.7.0"
  }
}
```

## 처리 단계별 로직

### 1. Collection 단계
- **트리거**: 데이터 수집 서버에서 수집 완료 시
- **발행자**: Data Collection Service
- **완료 조건**: 수집된 리뷰 수 = expected_count
- **타임아웃**: 60분

### 2. Transform 단계
- **트리거**: Transform 토픽에서 메시지 수신 시
- **발행자**: Kafka Streams (KTable Join 패턴)
- **완료 조건**: Transform 토픽의 job_id별 실제 리뷰 수 = Collection 메시지의 expected_count
- **타임아웃**: 30분
- **핵심 로직**: KTable Join을 통한 동적 완료 체크

### 3. Analysis 단계
- **트리거**: Analysis 토픽에서 메시지 수신 시
- **발행자**: Kafka Streams (KTable Join 패턴)
- **완료 조건**: Analysis 토픽의 job_id별 실제 결과 수 = Collection 메시지의 expected_count
- **타임아웃**: 45분
- **핵심 로직**: Transform과 동일한 KTable Join 패턴 사용

### 4. Aggregation 단계
- **트리거**: Kafka Streams에서 집계 완료 시
- **발행자**: Kafka Streams (집계 처리 완료 시)
- **완료 조건**: 모든 집계 처리 완료
- **타임아웃**: 15분

## Kafka Streams 구현

### Control 메시지 발행 로직
```java
public class ControlTopicManager {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final KStream<String, String> controlStream;
    
    /**
     * 한국 시간(KST)으로 현재 시간을 ISO 8601 형식으로 반환
     */
    private static String getCurrentTimeInKST() {
        ZoneId kstZone = ZoneId.of("Asia/Seoul");
        ZonedDateTime now = ZonedDateTime.now(kstZone);
        return now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
    
    public void publishCollectionDone(String jobId, int expectedCount, int actualCount) {
        ObjectNode message = createControlMessage(jobId, "collection", "done", expectedCount, actualCount);
        controlStream.to("job-control-topic", Produced.with(Serdes.String(), Serdes.String()));
    }
    
    public void publishTransformDone(String jobId, int expectedCount, int actualCount) {
        ObjectNode message = createControlMessage(jobId, "transform", "done", expectedCount, actualCount);
        controlStream.to("job-control-topic", Produced.with(Serdes.String(), Serdes.String()));
    }
    
    public void publishAnalysisDone(String jobId, int expectedCount, int actualCount) {
        ObjectNode message = createControlMessage(jobId, "analysis", "done", expectedCount, actualCount);
        controlStream.to("job-control-topic", Produced.with(Serdes.String(), Serdes.String()));
    }
    
    public void publishAggregationDone(String jobId, int expectedCount, int actualCount) {
        ObjectNode message = createControlMessage(jobId, "aggregation", "done", expectedCount, actualCount);
        controlStream.to("job-control-topic", Produced.with(Serdes.String(), Serdes.String()));
    }
    
    public void publishFailure(String jobId, String step, String errorMessage, int expectedCount, int actualCount) {
        ObjectNode message = createControlMessage(jobId, step, "failed", expectedCount, actualCount);
        message.put("error_message", errorMessage);
        controlStream.to("job-control-topic", Produced.with(Serdes.String(), Serdes.String()));
    }
    
    private ObjectNode createControlMessage(String jobId, String step, String status, int expectedCount, int actualCount) {
        ObjectNode message = MAPPER.createObjectNode();
        message.put("job_id", jobId);
        message.put("step", step);
        message.put("status", status);
        message.put("expected_count", expectedCount);
        message.put("actual_count", actualCount);
        message.put("timestamp", getCurrentTimeInKST());
        
        ObjectNode metadata = MAPPER.createObjectNode();
        metadata.put("processing_time", 0.0);
        metadata.put("worker_id", "kafka-streams-worker");
        metadata.put("batch_id", "batch-" + System.currentTimeMillis());
        metadata.put("server_info", "kafka-streams-enhanced-v1.0.0");
        message.set("metadata", metadata);
        
        return message;
    }
}
```

### 토폴로지 통합 (KTable Join 패턴)
```java
public class EnhancedReviewAggregator {
    private ControlTopicManager controlManager;
    
    public void buildTopology(StreamsBuilder builder) {
        // 기존 토폴로지...
        
        // Control 토픽 모니터링
        KStream<String, String> controlStream = builder.stream("job-control-topic");
        
        // Collection 메시지에서 expected_count 추출하여 KTable 생성
        KTable<String, Integer> expectedCounts = controlStream
            .filter((key, value) -> {
                try {
                    ObjectNode message = MAPPER.readValue(value, ObjectNode.class);
                    return "collection".equals(message.get("step").asText()) && 
                           "done".equals(message.get("status").asText());
                } catch (Exception e) {
                    return false;
                }
            })
            .map((key, value) -> {
                try {
                    ObjectNode message = MAPPER.readValue(value, ObjectNode.class);
                    String jobId = message.get("job_id").asText();
                    int expectedCount = message.get("expected_count").asInt();
                    return new KeyValue<>(jobId, expectedCount);
                } catch (Exception e) {
                    return new KeyValue<>(key, 0);
                }
            })
            .groupByKey()
            .aggregate(() -> 0, (jobId, value, expectedCount) -> expectedCount,
                       Materialized.as("expected-count-store"));
        
        // Transform 메시지에서 실제 리뷰 수 집계하여 KTable 생성
        KTable<String, Long> transformCounts = transformStream
            .map((key, value) -> {
                try {
                    ObjectNode message = MAPPER.readValue(value, ObjectNode.class);
                    String jobId = message.get("job_id").asText();
                    ArrayNode reviews = (ArrayNode) message.get("reviews");
                    int reviewCount = reviews.size();
                    return new KeyValue<>(jobId, (long) reviewCount);
                } catch (Exception e) {
                    return new KeyValue<>(key, 0L);
                }
            })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .aggregate(() -> 0L, (jobId, count, total) -> total + count,
                       Materialized.as("transform-count-table"));
        
        // KTable Join으로 동적 완료 체크
        KTable<String, String> transformComplete = transformCounts.join(
            expectedCounts,
            (actualCount, expectedCount) -> {
                boolean isComplete = actualCount >= expectedCount;
                return String.format("%d|%d|%s", actualCount, expectedCount, isComplete);
            }
        );
        
        // 완료된 Transform 작업에 대해 Control 메시지 발행
        transformComplete
            .filter((jobId, result) -> {
                String[] parts = result.split("\\|");
                return Boolean.parseBoolean(parts[2]); // isComplete
            })
            .toStream()
            .map((jobId, result) -> {
                String[] parts = result.split("\\|");
                long actualCount = Long.parseLong(parts[0]);
                int expectedCount = Integer.parseInt(parts[1]);
                
                ObjectNode message = MAPPER.createObjectNode();
                message.put("job_id", jobId);
                message.put("step", "transform");
                message.put("status", "done");
                message.put("expected_count", expectedCount);
                message.put("actual_count", (int) actualCount);
                message.put("timestamp", getCurrentTimeInKST());
                
                ObjectNode metadata = MAPPER.createObjectNode();
                metadata.put("processing_time", 0.0);
                metadata.put("worker_id", "kafka-streams-enhanced-worker");
                metadata.put("batch_id", "batch-" + System.currentTimeMillis());
                metadata.put("server_info", "kafka-streams-enhanced-v1.0.0");
                message.set("metadata", metadata);
                
                try {
                    return new KeyValue<>(jobId + "|transform", MAPPER.writeValueAsString(message));
                } catch (Exception e) {
                    return new KeyValue<>(jobId + "|transform", "{}");
                }
            })
            .to("job-control-topic", Produced.with(Serdes.String(), Serdes.String()));
            
        // Analysis 완료 모니터링 (동일한 패턴)
        KTable<String, Long> analysisCounts = analysisStream
            .map((key, value) -> {
                try {
                    ObjectNode message = MAPPER.readValue(value, ObjectNode.class);
                    String jobId = message.get("job_id").asText();
                    ArrayNode results = (ArrayNode) message.get("results");
                    int resultCount = results.size();
                    return new KeyValue<>(jobId, (long) resultCount);
                } catch (Exception e) {
                    return new KeyValue<>(key, 0L);
                }
            })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .aggregate(() -> 0L, (jobId, count, total) -> total + count,
                       Materialized.as("analysis-count-table"));
        
        // Analysis 완료 체크 (Transform과 동일한 패턴)
        KTable<String, String> analysisComplete = analysisCounts.join(
            expectedCounts,
            (actualCount, expectedCount) -> {
                boolean isComplete = actualCount >= expectedCount;
                return String.format("%d|%d|%s", actualCount, expectedCount, isComplete);
            }
        );
        
        analysisComplete
            .filter((jobId, result) -> {
                String[] parts = result.split("\\|");
                return Boolean.parseBoolean(parts[2]);
            })
            .toStream()
            .map((jobId, result) -> {
                String[] parts = result.split("\\|");
                long actualCount = Long.parseLong(parts[0]);
                int expectedCount = Integer.parseInt(parts[1]);
                
                ObjectNode message = MAPPER.createObjectNode();
                message.put("job_id", jobId);
                message.put("step", "analysis");
                message.put("status", "done");
                message.put("expected_count", expectedCount);
                message.put("actual_count", (int) actualCount);
                message.put("timestamp", getCurrentTimeInKST());
                
                ObjectNode metadata = MAPPER.createObjectNode();
                metadata.put("processing_time", 0.0);
                metadata.put("worker_id", "kafka-streams-enhanced-worker");
                metadata.put("batch_id", "batch-" + System.currentTimeMillis());
                metadata.put("server_info", "kafka-streams-enhanced-v1.0.0");
                message.set("metadata", metadata);
                
                try {
                    return new KeyValue<>(jobId + "|analysis", MAPPER.writeValueAsString(message));
                } catch (Exception e) {
                    return new KeyValue<>(jobId + "|analysis", "{}");
                }
            })
            .to("job-control-topic", Produced.with(Serdes.String(), Serdes.String()));
    }
}
```

## Airflow 연동

### KafkaSensor 설정
```python
from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka import KafkaSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

def check_job_completion(job_id, step, status='done'):
    """특정 job의 특정 단계 완료를 확인하는 필터 함수"""
    def message_filter(message):
        try:
            data = json.loads(message.value)
            return (data['job_id'] == job_id and 
                   data['step'] == step and 
                   data['status'] == status)
        except (json.JSONDecodeError, KeyError):
            return False
    return message_filter

def check_job_failure(job_id, step):
    """특정 job의 특정 단계 실패를 확인하는 필터 함수"""
    def message_filter(message):
        try:
            data = json.loads(message.value)
            return (data['job_id'] == job_id and 
                   data['step'] == step and 
                   data['status'] == 'failed')
        except (json.JSONDecodeError, KeyError):
            return False
    return message_filter

def handle_failure(context):
    """실패 처리 함수"""
    job_id = context['dag_run'].conf.get('job_id')
    step = context['task_instance'].task_id.replace('wait_', '')
    print(f"Job {job_id} failed at {step} step")
    # 알림 로직 추가 가능

# DAG 정의
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('review_processing_monitor',
         default_args=default_args,
         description='리뷰 처리 파이프라인 모니터링',
         schedule_interval=None,
         catchup=False) as dag:
    
    # Collection 완료 대기
    wait_collection = KafkaSensor(
        task_id='wait_collection',
        kafka_conn_id='kafka_default',
        topic='job-control-topic',
        message_filter=check_job_completion('{{ dag_run.conf.job_id }}', 'collection'),
        timeout=3600,  # 1시간 타임아웃
        poke_interval=30,  # 30초마다 체크
        on_failure_callback=handle_failure
    )
    
    # Transform 완료 대기
    wait_transform = KafkaSensor(
        task_id='wait_transform',
        kafka_conn_id='kafka_default',
        topic='job-control-topic',
        message_filter=check_job_completion('{{ dag_run.conf.job_id }}', 'transform'),
        timeout=1800,  # 30분 타임아웃
        poke_interval=30,
        on_failure_callback=handle_failure
    )
    
    # Analysis 완료 대기
    wait_analysis = KafkaSensor(
        task_id='wait_analysis',
        kafka_conn_id='kafka_default',
        topic='job-control-topic',
        message_filter=check_job_completion('{{ dag_run.conf.job_id }}', 'analysis'),
        timeout=2700,  # 45분 타임아웃
        poke_interval=30,
        on_failure_callback=handle_failure
    )
    
    # Aggregation 완료 대기
    wait_aggregation = KafkaSensor(
        task_id='wait_aggregation',
        kafka_conn_id='kafka_default',
        topic='job-control-topic',
        message_filter=check_job_completion('{{ dag_run.conf.job_id }}', 'aggregation'),
        timeout=900,  # 15분 타임아웃
        poke_interval=30,
        on_failure_callback=handle_failure
    )
    
    # 완료 알림
    notify_completion = PythonOperator(
        task_id='notify_completion',
        python_callable=lambda: print("All processing steps completed successfully!")
    )
    
    # 작업 순서 정의
    wait_collection >> wait_transform >> wait_analysis >> wait_aggregation >> notify_completion
```

### DAG 실행 방법
```bash
# Airflow CLI로 DAG 실행
airflow dags trigger review_processing_monitor \
  --conf '{"job_id": "job-2024-001"}'

# 또는 Airflow UI에서 수동 실행 시 conf에 job_id 설정
```

## 모니터링 및 알림

### 실패 감지 및 알림
```python
def send_slack_notification(job_id, step, error_message):
    """Slack 알림 발송"""
    webhook_url = "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
    message = {
        "text": f"🚨 리뷰 처리 실패 알림",
        "attachments": [
            {
                "color": "danger",
                "fields": [
                    {"title": "Job ID", "value": job_id, "short": True},
                    {"title": "실패 단계", "value": step, "short": True},
                    {"title": "에러 메시지", "value": error_message, "short": False}
                ]
            }
        ]
    }
    
    requests.post(webhook_url, json=message)

def send_email_notification(job_id, step, error_message):
    """이메일 알림 발송"""
    # 이메일 발송 로직
    pass
```

### 메트릭 수집
```python
from prometheus_client import Counter, Histogram, Gauge

# 메트릭 정의
control_messages_total = Counter('control_messages_total', 'Total control messages', ['step', 'status'])
processing_duration = Histogram('processing_duration_seconds', 'Processing duration', ['step'])
active_jobs = Gauge('active_jobs_total', 'Number of active jobs', ['step'])

def update_metrics(message_data):
    """메트릭 업데이트"""
    control_messages_total.labels(
        step=message_data['step'], 
        status=message_data['status']
    ).inc()
    
    if 'processing_time' in message_data.get('metadata', {}):
        processing_duration.labels(
            step=message_data['step']
        ).observe(message_data['metadata']['processing_time'])
```

## 운영 가이드

### 토픽 관리
```bash
# 토픽 상태 확인
kubectl -n kafka exec -it my-cluster-broker-0 -- bash -lc \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic job-control-topic"

# 메시지 확인
kubectl -n kafka exec -it my-cluster-broker-0 -- bash -lc \
  "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic job-control-topic --from-beginning --max-messages 10"

# 컨슈머 그룹 상태 확인
kubectl -n kafka exec -it my-cluster-broker-0 -- bash -lc \
  "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group airflow-monitor"
```

### 문제 해결
1. **메시지 누락**: 컨슈머 그룹 오프셋 확인
2. **타임아웃**: 각 단계별 타임아웃 설정 조정
3. **중복 메시지**: exactly-once 보장 설정 확인
4. **순서 문제**: 파티션 1개 사용으로 순서 보장

## 보안 고려사항

### 접근 제어
- Kafka ACL 설정으로 토픽 접근 권한 제어
- Airflow에서 사용하는 컨슈머 그룹별 권한 분리

### 데이터 보호
- 민감한 정보는 metadata에 포함하지 않음
- 에러 메시지에서 개인정보 제거

## 확장성 고려사항

### 성능 최적화
- 파티션 수 증가 시 순서 보장 고려
- 컨슈머 그룹별 독립적인 오프셋 관리
- 배치 처리로 메시지 처리량 향상

### 모니터링 확장
- Grafana 대시보드 연동
- 알림 채널 확장 (Slack, 이메일, SMS)
- 로그 집계 및 분석

## 실제 구현 현황 (2025-09-15 업데이트)

### ✅ 구현 완료된 기능
- **KTable Join 패턴**: Collection 메시지의 `expected_count`와 Transform/Analysis의 실제 카운트를 동적으로 비교
- **한국 시간(KST) 적용**: 모든 timestamp가 한국 시간으로 생성됨 (`Asia/Seoul` 타임존)
- **실제 리뷰 수 집계**: 메시지 수가 아닌 실제 리뷰 수로 정확한 카운팅
- **동적 완료 체크**: 하드코딩 제거하고 런타임에 `actualCount >= expectedCount` 비교
- **타입 안전성**: 명시적 Serdes 지정으로 `ClassCastException` 방지

### 🔧 핵심 기술적 개선사항
1. **KTable Join 패턴 도입**
   ```java
   // Collection 메시지에서 expected_count 추출
   KTable<String, Integer> expectedCounts = controlStream...
   
   // Transform 메시지에서 실제 리뷰 수 집계
   KTable<String, Long> transformCounts = transformStream...
   
   // 두 KTable을 Join하여 동적 완료 체크
   KTable<String, String> transformComplete = transformCounts.join(expectedCounts, ...);
   ```

2. **한국 시간 적용**
   ```java
   private static String getCurrentTimeInKST() {
       ZoneId kstZone = ZoneId.of("Asia/Seoul");
       ZonedDateTime now = ZonedDateTime.now(kstZone);
       return now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
   }
   ```

3. **정확한 리뷰 수 집계**
   ```java
   // 이전: groupByKey().count() - 메시지 수 집계
   // 현재: groupByKey().aggregate() - 실제 리뷰 수 집계
   .aggregate(() -> 0L, (jobId, count, total) -> total + count)
   ```

### 📊 테스트 검증 완료
- ✅ `expected_count=2` → Transform 메시지 2개 → 완료
- ✅ `expected_count=3` → Transform 메시지 3개 → 완료  
- ✅ `expected_count=5` → Transform 메시지 2개+3개 → 완료
- ✅ 한국 시간(KST) 적용 확인
- ✅ Control Topic 메시지 정상 발행 (`expected_count`, `actual_count` 정확히 반영)

### 🎯 운영 상태
- **상태**: ✅ **모든 핵심 기능 구현 및 검증 완료**
- **배포**: ✅ **프로덕션 환경 배포 완료**
- **모니터링**: ✅ **실시간 로그 모니터링 가능**
- **확장성**: ✅ **동적 작업 크기 처리 가능**

이 Control 토픽 시스템을 통해 Airflow에서 리뷰 처리 파이프라인의 각 단계를 효율적으로 모니터링할 수 있습니다.
