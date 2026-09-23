# devin-2api (Rust)

> **한국어** | [English](README.en.md) | [中文](README.zh-CN.md)

devin-2api는 Devin 계정([app.devin.ai](https://app.devin.ai/))에서 사용할 수 있는 모델을 OpenAI / Anthropic 호환 엔드포인트 뒤에 노출하는 비공식 프로토콜 어댑터입니다. 표준 클라이언트(Codex, Claude Code, 임의 SDK)가 익숙한 API로 이 모델들을 호출할 수 있게 해줍니다. Rust로 작성된 단일 정적 바이너리로 배포됩니다.

> **면책**: 이 프로젝트는 Cognition과 무관하며 보증받지 않았습니다. 자신의 Devin 세션 토큰으로 내부 RPC 표면에 인증합니다. 개인 계정 용도로 사용하며 Devin 서비스 약관 준수 책임은 사용자에게 있습니다.

## 기능

- **하나의 업스트림, 세 개의 API 표면** — `POST /v1/responses`(OpenAI Responses, Codex 계열 클라이언트용 WebSocket 전송과 다중 턴 세션 포함), `POST /v1/chat/completions`(OpenAI Chat), `POST /v1/messages`(Anthropic Messages)
- **스트리밍과 비스트리밍** 응답(typed SSE / JSON)
- **라운드트립되는 reasoning** — thinking signature가 보존되어 턴 간 재생됩니다: Responses는 `encrypted_content` reasoning item, Anthropic은 `redacted_thinking`, Chat은 `reasoning_content`
- **충실한 도구 호출** — custom/freeform 도구 호출(예: `apply_patch`)이 원문 그대로 왕복; 도구 이름과 `tool_choice`는 로컬에서 검증; call↔result 재페어링은 업스트림이 강제하는 규칙과 일치
- **탄력적인 업스트림 스트림** — 만료된 토큰은 자격 증명 소스에서 다시 읽고, 콘텐츠 이전의 업스트림 실패(전송 단절, 무응답 정지, 빈 응답)는 투명하게 재시도하며, 초기 실패는 커밋된 `200` 뒤의 SSE 오류가 아닌 실제 HTTP 오류로 표면화
- **레이트 리밋 게이트** — 업스트림 `resource_exhausted`가 로컬 쿨다운 래치를 트립: 대기 중인 요청은 잠시 기다린 뒤 `429` + `Retry-After`로 빠르게 실패하고, 드립 방식 프로브가 복구를 감지하며, 래치 상태는 재시작 간 유지(`logs/gate-state.json`). 선택적 `max_rpm` 토큰 버킷이 래치 트립 전에 아웃바운드 압력을 조절
- **정규화된 오류 계약** — 업스트림 오류 코드가 적절한 HTTP 상태와 프로토콜별 오류 타입으로 매핑; 레이트 리밋은 `429` + `Retry-After`; 모든 요청은 디버그 디렉터리를 가리키는 `X-Request-Id`/`debug_ref`를 가짐
- **`/v1/models` capability 플래그** — 컨텍스트 윈도우, 도구/thinking/이미지 지원이 업스트림 모델 카탈로그에서 표면화
- **`/panel` 관리 패널** — 요청 브라우저, 사용량/비용 집계, 쿼터 추적, 프로세스 메트릭, 요청별 디버그 디렉터리, 대부분 필드의 핫 리로드를 지원하는 마스킹된 설정 뷰
- **배포가 쉬움** — 단일 정적 바이너리, [GHCR](https://github.com/min9lin9/devin2api/pkgs/container/devin2api) 컨테이너 이미지

## 빠른 시작

### 1. Devin 토큰 준비

devin-2api는 Devin 세션 토큰(`devin-session-token$...`)으로 인증합니다. `config.yaml`의 `devin.token`을 비워 두면 다음 순서로 자동 탐색합니다:

1. `DEVIN_TOKEN` 또는 `WINDSURF_API_KEY` 환경 변수
2. Devin CLI 자격 증명 파일 — macOS/Linux는 `~/.local/share/devin/credentials.toml`, Windows는 `%APPDATA%\devin\credentials.toml`(다음 `%LOCALAPPDATA%\devin\credentials.toml`). Windows CLI는 단독 배포되지 않고 [Windsurf 데스크톱 앱](https://devin.ai/download)에 포함됩니다. 설치 후 `& "C:\Program Files\Windsurf\resources\app\extensions\windsurf\devin\bin\devin.exe" auth login`이 위 파일을 생성합니다.

macOS에서는 Devin 앱의 로컬 상태에서 토큰을 추출할 수도 있습니다:

```bash
sqlite3 ~/Library/"Application Support"/Devin/User/globalStorage/state.vscdb \
  "SELECT json_extract(value, '$.apiKey') FROM ItemTable WHERE key='windsurfAuthStatus';"
```

토큰은 만료됩니다. 업스트림이 `unauthenticated`를 반환하면 어댑터가 같은 소스 체인을 다시 읽습니다 — Devin CLI가 `credentials.toml`을 갱신하면 프록시가 재시작 없이 자가 치유됩니다.

### 2. 설정

```bash
cp config.example.yaml config.yaml
```

`config.yaml`을 편집해 토큰을 채웁니다(`config.example.yaml` 기준으로 `devin.token`만 채우면 됩니다 — base URL과 모델은 예시로 미리 채워져 있습니다).

### 3. 실행

미리 빌드된 바이너리([Releases](https://github.com/min9lin9/devin2api/releases), 검증용 `checksums.txt` 첨부). 에셋 이름은 `devin-2api-{darwin,linux}-{amd64,arm64}`이며 Windows는 같은 이름의 `.zip` 번들(exe + `config.example.yaml` + LICENSE)입니다:

```bash
# Linux 기준; macOS는 devin-2api-darwin-arm64 또는 -darwin-amd64
curl -fLO https://github.com/min9lin9/devin2api/releases/latest/download/devin-2api-linux-amd64
chmod +x devin-2api-linux-amd64
./devin-2api-linux-amd64 -config config.yaml
```

Windows에서는 `devin-2api-windows-amd64.zip`을 풀고 `config.yaml`을 편집한 뒤(토큰은 비워 둬도 됩니다 — 1단계 2번이 Windsurf 번들 `devin.exe`가 만든 자격 증명 파일을 커버) 콘솔에서 `devin-2api.exe -config config.yaml`을 실행합니다. Ctrl+C는 같은 graceful drain을 트리거하지만, 창 닫기와 `taskkill /F`는 그렇지 않습니다 — Windows는 콘솔 프로세스에 대한 graceful kill을 제공하지 않습니다.

소스에서 빌드(생성된 proto 바인딩이 `crates/devin-proto`에 커밋되어 있어 추가 도구 불필요):

```bash
cargo build --locked --release --bin devin-2api
./target/release/devin-2api -config config.yaml
```

Docker(이미지는 [GHCR](https://github.com/min9lin9/devin2api/pkgs/container/devin2api)에 게시):

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/config.yaml:/app/config.yaml" \
  ghcr.io/min9lin9/devin2api --config /app/config.yaml
```

서비스로 실행(선택):

| 플랫폼  | 수퍼바이저                              | 레이아웃                                                                                                     | 설치/업그레이드              |
| ------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ---------------------------- |
| macOS   | launchd agent                           | bin `~/.local/bin` · config+state `~/Library/Application Support/devin-2api`                                 | `scripts/deploy.sh`          |
| Linux   | `systemd --user`                        | bin `~/.local/bin` · config `~/.config/devin-2api` · state `~/.local/state/devin-2api`                       | `scripts/deploy-linux.sh`    |
| Windows | 없음 — 콘솔, 또는 NSSM / Task Scheduler | exe `%LOCALAPPDATA%\Programs\devin-2api` · config `%APPDATA%\devin-2api` · state `%LOCALAPPDATA%\devin-2api` | `scripts/deploy-windows.ps1` |

바이너리는 플랫폼 관례에 따라 경로를 해석합니다: config는 `-config` 플래그 → `DEVIN2API_CONFIG` → `./config.yaml` → 위 플랫폼 기본값; state는 `-state-dir` → `DEVIN2API_STATE_DIR` → 플랫폼 기본값. 두 배포 스크립트 모두 한 번에 설치/업그레이드하고(`--release latest`는 미리 빌드된 바이너리를 받음), `/healthz`가 새 버전을 보고하는지 확인한 뒤 `GET /v1/models`로 업스트림 인증이 실제로 동작하는지 검증합니다. 저장소를 홈으로 취급하므로(`config.yaml`을 플랫폼 config 디렉터리로 동기화하고 state 디렉터리를 가리키는 `logs` 심볼릭 링크를 저장소에 유지) 먼저 clone한 뒤 실행하세요:

```bash
git clone https://github.com/min9lin9/devin2api && cd devin2api
bash scripts/deploy-linux.sh --release latest    # macOS: scripts/deploy.sh
```

첫 실행 시 `config.yaml`이 `config.example.yaml`에서 생성되고 임의의 `auth.api_key`/`dashboard.password`가 채워지며 Devin 토큰 입력을 요청받습니다(비워 두면 자동 탐색으로 폴백). 미리 값을 지정하려면 `cp config.example.yaml config.yaml` 후 편집하세요. `--check`는 설치됨/실행 중/최신 버전을 보고하고, `--uninstall`은 서비스와 바이너리를 제거하되 config와 logs는 유지합니다.

Linux에서 로그인 세션이 끝나도 서비스가 살아 있어야 한다면 `loginctl enable-linger $USER`를 실행하세요.

### 4. 확인

```bash
curl http://localhost:8080/healthz
# {"status":"ok","version":"...","uptime_seconds":12,"debug_logging":false}
```

## 사용법

> **참고**: `/v1/*` 엔드포인트는 선택적 API 키 인증을 지원합니다. `config.yaml`의 `auth.api_key`를 설정하면 클라이언트가 `Authorization: Bearer <api_key>` 또는 `X-Api-Key: <api_key>`를 보내야 합니다. 비워 두면 엔드포인트가 열린 상태가 됩니다 — 키도 설정하지 않고 루프백 밖에 바인드하면 네트워크 전체에 Devin 쿼터를 나눠주는 셈입니다.

엔드포인트:

- `POST /v1/responses` — OpenAI Responses(같은 경로의 `GET`은 WebSocket 전송 협상)
- `POST /v1/chat/completions` — OpenAI Chat Completions
- `POST /v1/messages` — Anthropic Messages
- `GET /v1/models`, `GET /v1/models/{model}` — capability 플래그가 포함된 업스트림 모델 카탈로그
- `GET /panel` — 관리 패널(요청 브라우저, 사용량, 쿼터, 프로세스 통계); `dashboard.password`로 보호

프록시는 **무상태**입니다: 모든 HTTP 요청이 전체 대화를 가져야 합니다(`previous_response_id`는 받지만 무시됩니다 — 서버 측 응답 저장소가 없습니다). WebSocket 전송에서는 연결별로 다중 턴 세션이 유지되고 증분 입력이 전체 트랜스크립트로 투명하게 확장됩니다.

OpenAI Responses API 클라이언트로 `http://localhost:8080/v1/responses`를 호출하세요.

비스트리밍:

```bash
curl http://localhost:8080/v1/responses \
  -H "Content-Type: application/json" \
  -d '{
    "model": "glm-5-2",
    "input": "Hello"
  }'
```

스트리밍(SSE):

```bash
curl -N http://localhost:8080/v1/responses \
  -H "Content-Type: application/json" \
  -d '{
    "model": "glm-5-2",
    "input": "Hello",
    "stream": true
  }'
```

요청 본문은 OpenAI Responses API(`input`, `instructions`, `tools`, `stream`, …)를 따릅니다. Anthropic Messages 클라이언트는 대신 `/v1/messages`를 호출합니다:

```bash
curl http://localhost:8080/v1/messages \
  -H "Content-Type: application/json" \
  -H "anthropic-version: 2023-06-01" \
  -d '{
    "model": "glm-5-2",
    "max_tokens": 256,
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

## 설정

설정은 시작 시 한 번 로드되는 YAML 파일입니다. 알 수 없는 필드는 거부됩니다. 전체 주석이 달린 참조는 `config.example.yaml`을 보세요.

| 필드                                             | 설명                                                                                                                                                        | 필수 / 기본값                                                                                           |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `server.listen`                                  | HTTP 리슨 주소                                                                                                                                              | 필수                                                                                                    |
| `server.max_concurrency`                         | 동시 `/v1/*` 요청 상한                                                                                                                                      | `1024`                                                                                                  |
| `devin.base_url`                                 | Devin Connect 서비스 base URL                                                                                                                               | `devin.token` 설정 시 필수(코드 기본값 없음; `config.example.yaml`은 `https://server.codeium.com` 사용) |
| `devin.token`                                    | Devin 세션 토큰(`devin-session-token$...`); 비우면 env / 자격 증명 파일에서 탐색                                                                            | 아니오 — 토큰을 찾을 수 없으면 엔드포인트가 업스트림 인증 실패를 반환                                   |
| `devin.model`                                    | Devin 채팅 모델 UID(예: `glm-5-2`)                                                                                                                          | `devin.token` 설정 시 필수(코드 기본값 없음)                                                            |
| `devin.aliases`                                  | 클라이언트 모델명 → 업스트림 UID 매핑(`swe-2: swe-2-max`); 매칭 순서: 정확 → 대소문자 무시 → `"*"` catch-all; alias는 `alias_of`와 함께 `/v1/models`에 표시 | 없음                                                                                                    |
| `devin.client_name`/`client_version`/`client_os` | 업스트림 메타데이터로 보내는 클라이언트 식별자                                                                                                              | `chisel` / `3000.2.17` / `mac`                                                                          |
| `devin.proxy`                                    | 업스트림 프록시 URL(`http(s)://`, `socks5(h)://`); 비우면 직접 연결 / env 변수                                                                              | 없음                                                                                                    |
| `devin.force_http1`                              | 업스트림으로 요청별 TCP 연결(HTTP/2 스트림 직렬화 회피)                                                                                                     | `true`                                                                                                  |
| `devin.max_rpm`                                  | 업스트림 메시지 레이트 리밋(msgs/min, 토큰 버킷); `<=0` 무제한 — 429 쿨다운 래치는 어느 쪽이든 적용                                                         | `0`(무제한; `config.example.yaml`은 `80`으로 배포)                                                      |
| `devin.gate_max_hold_seconds`                    | 쿨다운 래치 안에서 빠른 실패 `429` + `Retry-After` 전 최대 대기 시간                                                                                        | `15`                                                                                                    |
| `devin.gate_drip_interval_seconds`               | 래치 안의 프로브 방출 간격 — 제한 중 업스트림 도달 속도와 언래치 감지를 조절                                                                                | `8`                                                                                                     |
| `devin.gate_default_latch_seconds`               | 업스트림 `resource_exhausted`가 reset 시각을 선언하지 않을 때의 폴백 래치 시간                                                                              | `60`                                                                                                    |
| `devin.gate_window_offset_seconds`               | 로컬 분 안에서 업스트림 분 버킷 경계의 추정 위치(몇 초에 해당하는지)                                                                                        | `0`(로컬 `:00`; 관측된 경계는 로컬 `:59` 부근)                                                          |
| `devin.gate_window_guard_seconds`                | 추정 버킷 경계 양쪽의 데드 존 — 그 안의 요청은 다음 윈도우까지 대기                                                                                         | `2`                                                                                                     |
| `debug.enabled`                                  | `logs/` 아래 요청별 디버그 로그 기록                                                                                                                        | `false`                                                                                                 |
| `debug.retention_days`                           | 요청 로그 디렉터리 보관 일수; `<=0`이면 시간 기반 정리 비활성화                                                                                             | `14`                                                                                                    |
| `debug.max_total_mb`                             | `logs/` 전체 크기 상한; 가장 오래된 디렉터리부터 제거                                                                                                       | `1024`                                                                                                  |
| `debug.payload_hours`                            | 큰 스테이지 파일(03/04/06/attachments)이 제거되기까지의 시간, meta/error 증거는 유지                                                                        | `24`                                                                                                    |
| `debug.keep_error_dirs`                          | 크기 제거에서 보호되는 최신 실패 디렉터리 수(`error.json` 포함)                                                                                             | `32`                                                                                                    |
| `debug.quota_interval_minutes`                   | `logs/quota.jsonl`로의 쿼터 스냅샷 간격; `<=0`이면 비활성화                                                                                                 | `5`                                                                                                     |
| `debug.pprof_listen`                             | Rust 런타임 진단 리스너 주소(예: `127.0.0.1:6060`); 무인증 — 루프백 전용. 이름은 Go 원본과의 호환을 위해 유지                                               | 비어 있음(비활성화)                                                                                     |
| `dashboard.password`                             | `/panel` 관리자 비밀번호; 비우면 로그인 불필요                                                                                                              | 없음                                                                                                    |
| `auth.api_key`                                   | `/v1/*` 엔드포인트용 API 키; 비우면 인증 비활성화. 클라이언트는 `Authorization: Bearer <key>` 또는 `X-Api-Key: <key>`로 전송 가능                           | 없음(오픈)                                                                                              |

참고:

- 토큰은 로그에 기록되지 않습니다(`<redacted>`로 마스킹);
- `devin.token`이 비어 있어도 요청은 업스트림으로 나가며, 업스트림 인증 실패가 정규화된 오류로 반환됩니다 — 탐색 소스 어디든 토큰이 나타나면 다음 요청이 성공하며 재시작이 필요 없습니다;
- `config.yaml`은 gitignore되어 있습니다 — 그래도 실제 토큰을 git에 넣지 마세요.

## Go 구현과의 호환성

Go 구현([WncFht/devin2api](https://github.com/WncFht/devin2api))과 **설정 파일, 상태 디렉터리, 로그 형식을 그대로 공유**합니다. 마이그레이션 규칙, 단일 writer 규칙, 롤백 절차, 승인된 다섯 가지 동작 차이는 [docs/compatibility.md](docs/compatibility.md)를 보세요. 요약:

- 기존 `config.yaml`, `credentials.toml`, `logs/` 히스토리를 그대로 읽습니다 — 마이그레이션 불필요.
- **하나의 state 디렉터리에는 한 번에 하나의 writer만** — 두 데몬을 같은 state 디렉터리로 동시에 돌리지 마세요.
- 롤백은 별도로 백업해 둔 state 사본을 복원하는 방식입니다(절차는 [docs/deployment.md](docs/deployment.md)).

## 측정된 성능

동일 호스트(4코어 Ryzen 5 5600G, 루프백 스텁, 페어링된 30초 샘플, bootstrap CI)에서 Go 구현과 비교한 결과입니다. 전체 수치와 방법론은 [docs/perf.md](docs/perf.md)를 보세요.

- **SSE 스트리밍 처리량은 Go보다 낮습니다**: Chat SSE 셀에서 0.45–1.03×(동시성과 디버그 로깅이 높을수록 격차 확대; c1 debug-on은 Rust 우세). Responses/Messages SSE도 같은 경로를 공유합니다.
- **버퍼드 JSON과 WebSocket은 더 빠릅니다**: chat JSON 1.28×, WebSocket 턴 1.47×.
- **메모리는 훨씬 적게 사용합니다**: 모든 셀에서 피크 RSS가 Go의 0.22–0.75×.
- 신뢰성 게이트는 모두 통과: 100k 스텁 요청에서 예상치 못한 실패/중복/누락 종료 이벤트/리크된 퍼밋 0건, 취소 p99 15ms.

SSE 경로에서 더 빠르다고 주장하지 않습니다 — 측정값이 그렇지 않습니다.

## 플랫폼 지원 상태

릴리스 매트릭스는 6개 타깃입니다: Linux amd64/arm64(정적 musl), macOS amd64/arm64, Windows amd64/arm64(zip). 현재 상태:

- **검증됨(이 호스트에서 네이티브 빌드 + 스모크)**: `x86_64-unknown-linux-musl` — 정적 링크, 인터프리터 없음.
- **CI에서만 검증**: 나머지 5개 타깃은 `.github/workflows/release.yml`의 네이티브 호스티드 러너에서 빌드됩니다. Windows arm64는 amd64 러너에서 크로스 빌드되며 에뮬레이트된 스모크는 없습니다.
- Go 전용 진단(pprof/fgprof)은 Rust 진단으로 교체되었습니다 — [docs/perf.md](docs/perf.md)의 플랫폼별 capability 매트릭스를 보세요.

## 문서

- **배포, 롤백, 오프라인 스모크**: [docs/deployment.md](docs/deployment.md)
- **Go↔Rust 호환성, 승인된 예외, 마이그레이션**: [docs/compatibility.md](docs/compatibility.md)
- **측정된 성능 + Rust 런타임 진단/프로파일링**: [docs/perf.md](docs/perf.md)
- **업스트림 프로토콜 역공학 참조**: [docs/protocol.md](docs/protocol.md)
- **오류 참조와 트러블슈팅**: [docs/troubleshooting.md](docs/troubleshooting.md)
- **모든 바이너리/플래그의 커맨드 레퍼런스**: [docs/commands.md](docs/commands.md)
- **툴체인, 코드젠, CI/릴리스**: [docs/toolchain.md](docs/toolchain.md)
- **라이선스**: [MIT](LICENSE) · 서드파티 크레이트 라이선스: [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)

## 자주 묻는 질문

**devin2api가 뭔가요?**
Devin 계정의 모델을 OpenAI/Anthropic 호환 API 뒤에 노출하는 로컬 프록시입니다. Codex, Claude Code, 임의 SDK가 익숙한 엔드포인트로 Devin 모델을 호출할 수 있습니다.

**Go 구현과 무엇이 다른가요?**
동작 차이는 [호환성 문서](docs/compatibility.md)의 승인된 예외 다섯 건뿐입니다. Go 런타임 의존성이 없는 단일 정적 바이너리입니다.

**어떻게 설치하나요?**
[Releases](https://github.com/min9lin9/devin2api/releases)에서 플랫폼 바이너리를 받아 `config.yaml`과 함께 실행하거나, `cargo build --locked --release`로 빌드합니다. 토큰은 로컬 Devin/Windsurf 설치에서 자동 탐색됩니다.

**이름이 왜 두 가지인가요?**
저장소/프로젝트명은 `devin2api`, 바이너리/배포 아티팩트명은 `devin-2api`입니다.

**Cognition/Devin과 관계가 있나요?**
없습니다. 비공식 프로젝트이며 보증받지 않았습니다. Devin 서비스 약관 준수 책임은 사용자에게 있습니다.

## 감사

이 프로젝트는 [leookun/devin-2api](https://github.com/leookun/devin-2api)를 기반으로 한 Go 구현 [WncFht/devin2api](https://github.com/WncFht/devin2api)을 Rust로 재구현한 것입니다 — 원작자들의 작업에 감사합니다. 업스트림 프로토콜 스키마(`proto/`)는 Devin CLI 바이너리에서 추출되었으며 `proto/SHA256SUMS`에 출처 해시가 기록되어 있습니다.
