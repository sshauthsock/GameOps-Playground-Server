# 멀티 스테이지 빌드를 사용하여 경량 이미지 생성
# Stage 1: 빌드 스테이지
FROM gcc:latest AS builder

# 작업 디렉토리 설정
WORKDIR /app

# 소스 코드 복사
COPY src/main.cpp .

# C++ 코드 컴파일
# -std=c++17: C++17 표준 사용
# -O2: 최적화 레벨 2
# -pthread: 스레드 지원
# -static: 완전 정적 링크로 호환성 문제 해결
RUN g++ -std=c++17 -O2 -pthread -static -o server main.cpp

# Stage 2: 실행 스테이지 (경량 이미지)
# gcc:latest와 호환되는 최신 Debian 사용
FROM debian:bookworm-slim

# 작업 디렉토리 설정
WORKDIR /app

# 빌드 스테이지에서 컴파일된 바이너리 복사
COPY --from=builder /app/server .

# 포트 노출 (Railway의 $PORT 환경 변수 사용)
# Railway는 $PORT 환경 변수를 자동으로 제공합니다
EXPOSE 7777

# 서버 실행 (항상 7777 포트 사용)
# Railway의 PORT 환경 변수를 무시하고 고정 포트 사용
CMD ["./server", "7777"]

