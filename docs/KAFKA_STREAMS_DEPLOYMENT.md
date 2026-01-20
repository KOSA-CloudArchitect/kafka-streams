# Kafka Streams 리뷰 집계 애플리케이션 배포 기록 (review-aggregator)

## 개요
- 목적: Spark → LLM 분석 결과를 Kafka Streams로 집계하여
  - per-review 행(`review-rows`)과
  - job_id 기준 집계(`review-agg-by-job`)
  를 생성
- 구성요소:
  - Kafka 입력 토픽: `realtime-review-transform-topic`, `realtime-review-analysis-topic`
  - Kafka 출력 토픽: `review-rows`, `review-agg-by-job`
  - 애플리케이션 ID: `review-aggregator-v3` (최종)
  - 컨테이너 이미지: `hahxowns/review-aggregator:0.1.2`

## 최종 토폴로지 개요
1) 변환 스트림: transform-topic의 `{job_id, reviews:[...]}`를 per-review로 평탄화 → String JSON으로 변환
2) 분석 스트림: analysis-topic의 `{job_id, results:[...]}`를 per-review로 평탄화 → String JSON으로 변환
3) 키 재설정: `key = job_id|review_id`
4) 명시적 repartition: `Repartitioned.with(String, String)` (value를 String으로 보장)
5) 윈도우 조인: 변환-분석 per-review를 윈도우 조인 → per-review JSON 생성
6) 출력:
   - per-review 행: `review-rows`
   - job 집계: per-review 행을 job_id로 groupBy/aggregate → `review-agg-by-job`

## 배포 작업 요약
- 코드 경로: `analysis/aggregator` (Java 11, Kafka Streams 3.7.0)
- 도커: `analysis/aggregator/Dockerfile` (Temurin JRE 21)
- 쿠버네티스 매니페스트: `kubernetes/namespaces/core/base/review-aggregator-deployment.yaml`

## 시도한 작업과 문제, 원인, 해결

### 1) 초기 스캐폴딩 및 빌드
- 작업:
  - Maven 프로젝트 생성, 기본 토폴로지 작성, JDK 21 → JDK 11로 타깃 변경(로컬 JDK 11 사용)
  - 빌드 산출물: `review-aggregator-0.1.0-jar-with-dependencies.jar`
- 문제: 컨테이너 JRE 11 vs 클래스 파일 21 불일치로 `UnsupportedClassVersionError` 발생
- 원인: Dockerfile이 JRE 11인데, 한 시점 클래스 파일이 21로 빌드됨
- 해결:
  - Dockerfile을 `eclipse-temurin:21-jre`로 변경
  - 버전 증가(0.1.1) 후 재빌드/재배포

### 2) CrashLoopBackOff (ExitCode 0) 이슈
- 증상: 컨테이너가 즉시 정상 종료되어 CrashLoopBackOff
- 원인: 메인 스레드 종료 (Streams 종료 후 프로세스가 끝남)
- 해결:
  - `CountDownLatch`로 메인 스레드 대기하도록 수정
  - UncaughtExceptionHandler에서 스택트레이스 출력만 하고 프로세스 유지

### 3) `ClassCastException` (ObjectNode → StringSerializer)
- 증상 로그 (요지):
  - `ClassCastException while producing data to topic ... KSTREAM-KEY-SELECT-0000000004-repartition`
  - value serializer는 `StringSerializer`인데 실제 값 타입이 `ObjectNode`
- 원인:
  - selectKey/repartition 경로에서 value가 ObjectNode 상태로 남아 있었음
  - repartition 시 기본 Serde가 String이므로 직렬화 충돌
- 1차 해결 시도:
  - selectKey 이전에 `mapValues`로 JSON String 변환
  - 조인 전후 sink/aggregate에 String Serde 사용
- 잔여 문제:
  - 여전히 동일 오류 발생 → 실제 배포 이미지에 수정 코드가 반영되지 않은 상태(노드 캐시 이미지 재사용)
- 최종 해결:
  - 코드에서 selectKey 이후에 명시적 `repartition(Repartitioned.with(Serdes.String(), Serdes.String()))` 추가 (두 스트림 모두)
  - 이미지 버전 명시 상향 및 풀 정책 변경
    - `pom.xml` 0.1.2, Dockerfile COPY 대상도 0.1.2로 맞춤
    - Deployment: `image: hahxowns/review-aggregator:0.1.2`, `imagePullPolicy: Always`
  - 내부 토픽/오프셋 영향 배제 위해 APPLICATION_ID 변경: `review-aggregator-v3`

### 4) 분석 토픽 READY 의심 및 확인
- 관찰: `realtime-review-analysis-topic` READY 비표시로 의심
- 확인:
  - KafkaTopic CR/브로커 내 토픽 존재 확인 → 브로커에 정상 존재(파티션/리더/ISR OK)
- 결론: 토픽 준비 문제 아님. 핵심은 Streams value 직렬화 경로 및 배포 이미지 반영 문제였음

### 5) 최종 배포 성공
- 조치 이후 상태:
  - `review-aggregator` 파드 Running
  - 내부 repartition/changelog 토픽은 `review-aggregator-v3-...` 접두로 새로 생성
  - 오류 로그(ObjNode→String) 더 이상 재현되지 않음

## 최종 설정 스냅샷
- 애플리케이션 ID: `review-aggregator-v3`
- 이미지: `docker.io/hahxowns/review-aggregator:0.1.2`
- imagePullPolicy: `Always`
- Streams 주요 코드 포인트(`analysis/aggregator/src/main/java/com/example/aggregator/ReviewAggregator.java`):
  - selectKey 이전 value를 String으로 변환
  - selectKey 이후 `repartition(Repartitioned.with(Serdes.String(), Serdes.String()))` 적용(두 스트림)
  - 조인 시 String→ObjectNode 파싱 후 로직 처리
  - sink/aggregate 모두 String Serde 사용

## 운영 팁
- 이미지 캐시 회피: 테스트/릴리스 시 태그를 반드시 올리고 `imagePullPolicy: Always` 권장
- 내부 토픽 리셋: 애플리케이션 ID 변경으로 안전하게 신규 내부 토픽 생성
- 디버깅: Streams 에러는 sink/repartition 노드명을 통해 어느 경로에서 직렬화/역직렬화 문제가 나는지 빠르게 추적 가능
- 입력 토픽 포맷: 변환/분석 입력 모두 JSON String 가정. 스키마 변경 시 backward 호환 고려 또는 DLQ 운영 권장

## 재현/배포 명령 모음
```bash
# 빌드/푸시
cd analysis/aggregator
mvn -DskipTests package
docker build -t hahxowns/review-aggregator:0.1.2 .
docker push hahxowns/review-aggregator:0.1.2

# 배포
kubectl apply -k kubernetes/namespaces/core/base
kubectl -n core rollout status deploy/review-aggregator
kubectl -n core logs -f deploy/review-aggregator | sed -n '1,200p'

# 출력 토픽 확인 (브로커 파드)
kubectl -n kafka exec -it my-cluster-broker-0 -- bash -lc \
  "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic review-rows --from-beginning --max-messages 3"

kubectl -n kafka exec -it my-cluster-broker-0 -- bash -lc \
  "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic review-agg-by-job --from-beginning --max-messages 3"
```
