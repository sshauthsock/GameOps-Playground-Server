# GameOps Playground Server

GameOps Playground Server 프로젝트입니다.

## 프로젝트 구조

```
GameOps-Playground-Server/
├── src/              # 소스 코드
│   └── main.cpp      # 메인 진입점
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

### Docker Compose 사용
```bash
docker-compose up --build
```

### 직접 빌드
```bash
mkdir -p build
cd build
cmake ..
make
```

## 개발 환경

- C++17 이상
- Docker & Docker Compose

