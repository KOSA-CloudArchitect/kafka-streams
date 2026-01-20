#!/bin/bash

# Enhanced Review Aggregator 빌드 및 배포 스크립트
# 버전: 0.2.0
# 애플리케이션 ID: review-aggregator-enhanced-v1

set -e  # 오류 발생 시 스크립트 종료

echo "=== Enhanced Review Aggregator 빌드 및 배포 시작 ==="

# 1. Maven 빌드
echo "1. Maven 빌드 시작..."
cd aggregator

# 기존 빌드 결과 정리
echo "   - 기존 빌드 결과 정리..."
mvn clean

# 테스트 스킵하고 패키지 빌드
echo "   - Maven 패키지 빌드 중..."
mvn -DskipTests package

# 빌드 결과 확인
if [ ! -f "target/review-aggregator-0.2.0-jar-with-dependencies.jar" ]; then
    echo "   ❌ 빌드 실패: JAR 파일이 생성되지 않았습니다."
    exit 1
fi

echo "   ✅ Maven 빌드 완료: review-aggregator-0.2.0-jar-with-dependencies.jar"

# 2. Docker 이미지 빌드
echo "2. Docker 이미지 빌드 시작..."
cd ..

# Docker 이미지 빌드
echo "   - Docker 이미지 빌드 중..."
docker build -t hahxowns/review-aggregator-enhanced:0.2.0 aggregator/

# 빌드 결과 확인
if [ $? -ne 0 ]; then
    echo "   ❌ Docker 빌드 실패"
    exit 1
fi

echo "   ✅ Docker 이미지 빌드 완료: hahxowns/review-aggregator-enhanced:0.2.0"

# 3. Docker 이미지 푸시
echo "3. Docker 이미지 푸시 시작..."
echo "   - Docker Hub에 이미지 푸시 중..."
docker push hahxowns/review-aggregator-enhanced:0.2.0

if [ $? -ne 0 ]; then
    echo "   ❌ Docker 푸시 실패"
    exit 1
fi

echo "   ✅ Docker 이미지 푸시 완료"

# 4. Kubernetes 배포
echo "4. Kubernetes 배포 시작..."

# 현재 컨텍스트 확인
echo "   - 현재 Kubernetes 컨텍스트 확인..."
kubectl config current-context

# 네임스페이스 확인
echo "   - core 네임스페이스 확인..."
if ! kubectl get namespace core > /dev/null 2>&1; then
    echo "   ❌ core 네임스페이스가 존재하지 않습니다."
    exit 1
fi

# 기존 배포 확인 및 업데이트
echo "   - 기존 배포 확인..."
if kubectl get deployment review-aggregator-enhanced -n core > /dev/null 2>&1; then
    echo "   - 기존 배포 업데이트 중..."
    kubectl set image deployment/review-aggregator-enhanced app=hahxowns/review-aggregator-enhanced:0.2.0 -n core
    kubectl rollout status deployment/review-aggregator-enhanced -n core --timeout=300s
else
    echo "   - 새로운 배포 생성 중..."
    # 배포 매니페스트가 있다면 적용
    if [ -f "kubernetes/namespaces/core/base/review-aggregator-enhanced-deployment.yaml" ]; then
        kubectl apply -f kubernetes/namespaces/core/base/review-aggregator-enhanced-deployment.yaml
        kubectl rollout status deployment/review-aggregator-enhanced -n core --timeout=300s
    else
        echo "   ⚠️  배포 매니페스트 파일을 찾을 수 없습니다."
        echo "   수동으로 배포를 진행해주세요."
    fi
fi

if [ $? -ne 0 ]; then
    echo "   ❌ Kubernetes 배포 실패"
    exit 1
fi

echo "   ✅ Kubernetes 배포 완료"

# 5. 배포 상태 확인
echo "5. 배포 상태 확인..."

# 파드 상태 확인
echo "   - 파드 상태 확인..."
kubectl get pods -n core -l app=review-aggregator-enhanced

# 로그 확인 (최근 50줄)
echo "   - 최근 로그 확인..."
kubectl logs -n core -l app=review-aggregator-enhanced --tail=50

echo ""
echo "=== 배포 완료 ==="
echo "애플리케이션 ID: review-aggregator-enhanced-v1"
echo "Docker 이미지: hahxowns/review-aggregator-enhanced:0.2.0"
echo "네임스페이스: core"
echo ""
echo "추가 명령어:"
echo "  - 로그 실시간 확인: kubectl logs -n core -l app=review-aggregator-enhanced -f"
echo "  - 파드 상태 확인: kubectl get pods -n core -l app=review-aggregator-enhanced"
echo "  - 배포 롤백: kubectl rollout undo deployment/review-aggregator-enhanced -n core"
