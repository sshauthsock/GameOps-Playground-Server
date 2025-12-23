# 멀티 스테이지 빌드를 사용하여 경량 이미지 생성
# Stage 1: 빌드 스테이지
FROM gcc:latest AS builder

# Boost 라이브러리 설치
RUN apt-get update && apt-get install -y \
    libboost-system-dev \
    libboost-thread-dev \
    && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 설정
WORKDIR /app

# 소스 코드 복사
COPY src/main.cpp .
COPY src/websocket_server.cpp .
COPY src/websocket_server.h .

# C++ 코드 컴파일
# -std=c++17: C++17 표준 사용
# -O2: 최적화 레벨 2
# -pthread: 스레드 지원
# -static: 완전 정적 링크로 호환성 문제 해결
# Boost 라이브러리 링크 추가
RUN g++ -std=c++17 -O2 -pthread -static -o server main.cpp websocket_server.cpp -lboost_system -lboost_thread

# Stage 2: 실행 스테이지 (경량 이미지)
# gcc:latest와 호환되는 최신 Debian 사용
FROM debian:bookworm-slim

# 작업 디렉토리 설정
WORKDIR /app

# 빌드 스테이지에서 컴파일된 바이너리 복사
COPY --from=builder /app/server .

# 포트 노출 (TCP 포트 7777, WebSocket 포트 7778)
EXPOSE 7777 7778

# 서버 실행
# Railway/Render: $PORT 환경 변수 사용 (TCP=$PORT, WebSocket=$PORT+1)
# 로컬/Docker Compose: 명령줄 인자 사용 (./server 7777 7778)
# 환경 변수가 없으면 기본값 7777, 7778 사용
CMD ["./server"]

