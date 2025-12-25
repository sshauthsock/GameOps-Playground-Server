# GameOps Playground Server

C++로 구현된 멀티플레이어 게임 서버입니다. TCP와 WebSocket을 모두 지원하여 Unity WebGL 클라이언트와 네이티브 클라이언트 모두에서 연결할 수 있습니다.

## 🚀 주요 특징

- **이중 프로토콜 지원**: TCP 소켓과 WebSocket 동시 지원
- **Railway 클라우드 배포**: HTTP 서비스 모드로 자동 배포
- **턴 기반 게임 로직**: 방 관리, 플레이어 관리, 턴 시스템
- **안정성**: 버퍼 오버플로우 방지, 동시성 제어, 메시지 검증

## 📚 문서

### 📖 프로젝트 개요
- **[프로젝트 상세 설명](./docs/PROJECT_OVERVIEW.md)** - 프로젝트 전체 개요, 아키텍처, 기능 설명
- **[시각적 아키텍처 다이어그램](./docs/PROJECT_DIAGRAMS.md)** - 시스템 구조와 흐름을 시각적으로 표현

### 🔧 기술 문서
- **[WebSocket 구현 여정](./docs/WEBSOCKET_IMPLEMENTATION_JOURNEY.md)** - WebSocket 구현 과정과 문제 해결
- **[서버 WebSocket 구현 가이드](./docs/SERVER_WEBSOCKET_IMPLEMENTATION_GUIDE.md)** - 기술 구현 상세 가이드
- **[Railway HTTP 서비스 설정](./docs/RAILWAY_HTTP_SERVICE_SETUP.md)** - Railway 배포 가이드
- **[Unity WebGL 연결 가이드](./docs/UNITY_WEBGL_CONNECTION.md)** - 클라이언트 연결 방법

## 프로젝트 구조

```
GameOps-Playground-Server/
├── src/              # 소스 코드
│   ├── main.cpp      # 메인 서버 로직, 게임 로직, TCP 서버
│   ├── websocket_server.h
│   └── websocket_server.cpp
├── include/          # 헤더 파일
├── tests/            # 테스트 코드
├── docs/             # 문서
├── scripts/          # 빌드/배포 스크립트
├── build/            # 빌드 출력 (gitignore)
├── Dockerfile        # Docker 이미지 빌드
├── docker-compose.yml # Docker Compose 설정
└── .dockerignore     # Docker 빌드 제외 파일
```

## 빌드 및 실행

### Docker Compose 사용 (권장)
```bash
docker-compose up --build
```

서버가 다음 포트에서 실행됩니다:
- TCP 서버: `localhost:7777`
- WebSocket 서버: `localhost:7778`

### 직접 빌드
```bash
mkdir -p build
cd build
cmake ..
make
./server 7777 7778
```

## 개발 환경

- **언어**: C++17 이상
- **라이브러리**: Boost.Beast, Boost.System, Boost.Thread
- **컨테이너**: Docker & Docker Compose
- **배포**: Railway

## 🎮 게임 기능

- 방 생성, 참가, 나가기
- 플레이어 로그인 및 관리
- 준비 상태 관리
- 턴 기반 게임플레이
- 플레이어 액션 (이동, 발사, 조준 등)

## 📡 메시지 프로토콜

메시지는 4바이트 헤더(길이 + ID)와 가변 길이 페이로드로 구성됩니다.

주요 메시지 타입:
- **인증**: 100 (로그인 요청), 101 (로그인 응답)
- **방 관리**: 290-316 (방 목록, 생성, 참가, 나가기)
- **게임 준비**: 320-331 (준비, 게임 시작)
- **게임 액션**: 200, 410, 420, 500, 600 (발사, 이동, 조준 등)
- **턴 관리**: 430 (턴 시작), 440 (턴 종료)

자세한 내용은 [프로젝트 개요 문서](./docs/PROJECT_OVERVIEW.md)를 참조하세요.

## 🔗 연결 정보

### Railway 배포 서버
- **WebSocket URL**: `wss://gameops-playground-server-production.up.railway.app`
- **프로토콜**: WSS (WebSocket Secure)

### 로컬 개발 서버
- **TCP**: `tcp://localhost:7777`
- **WebSocket**: `ws://localhost:7778`

## 📝 라이선스

이 프로젝트는 GameOps Playground 프로젝트의 일부입니다.

