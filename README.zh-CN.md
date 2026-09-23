# devin-2api (Rust)

> [한국어](README.md) | [English](README.en.md) | **中文**

devin-2api 是一个非官方协议适配器，把你 Devin 账号（[app.devin.ai](https://app.devin.ai/)）可用的模型包装在 OpenAI / Anthropic 兼容接口后面——让标准客户端（Codex、Claude Code、任意 SDK）通过熟悉的 API 调用它们。

以 Rust 编写，交付为单一静态二进制。

> **声明**：本项目与 Cognition 无任何关联、未获其背书。它使用你自己的 Devin 会话 token 调用内部 RPC 接口，仅供个人账号自用；请自行遵守 Devin 的服务条款。

## 特性

- **一个上游，三个 API 面**——`POST /v1/responses`（OpenAI Responses，含 Codex 式客户端的 WebSocket transport 与多轮会话）、`POST /v1/chat/completions`（OpenAI Chat）、`POST /v1/messages`（Anthropic Messages）
- **支持流式与一次性响应**（typed SSE / JSON）
- **思考签名跨轮回放**——按各 provider 原生形态保存并回传：Responses 面落成 `encrypted_content` reasoning item，Anthropic 面落成 `redacted_thinking`，Chat 面落成 `reasoning_content`
- **忠实的工具调用**——custom/freeform 工具调用（如 `apply_patch`）原文往返；工具名与 `tool_choice` 本地校验；按上游强制的 call↔result 交错序重新配对
- **上游流韧性**——token 过期自动从凭据来源重读；产出内容前的上游失败（传输断裂、静默卡死、空回复）透明重试；早期失败返回真实 HTTP 错误，而不是已提交 200 后的 SSE error
- **限流闸门**——上游 `resource_exhausted` 触发本地冷却闩：排队请求短暂等待后快速失败 `429` + `Retry-After`，不再捶打已被限流的上游；闩内按滴灌节奏放探针探测恢复；闩状态落盘 `logs/gate-state.json`，重启后未过期自动恢复。可选 `max_rpm` 令牌桶在触闩前先行整形出站压力
- **归一化错误契约**——上游错误码映射为正确的 HTTP 状态与各协议错误类型；限流归一为 `429` + `Retry-After`；每个请求带 `X-Request-Id`/`debug_ref` 直指调试目录
- **`/v1/models` 能力位透出**——上下文窗口、工具/thinking/图片支持等来自上游模型目录
- **`/panel` 管理面板**——请求浏览、用量/成本聚合、配额追踪、进程指标、按请求调试目录，以及多数字段可热加载的脱敏配置视图
- **部署简单**——单一静态二进制，[GHCR](https://github.com/min9lin9/devin2api/pkgs/container/devin2api) 容器镜像

## 快速开始

### 1. 提供 Devin token

devin-2api 用你的 Devin 会话 token（`devin-session-token$...`）向 Devin 认证。`config.yaml` 里 `devin.token` 留空时按以下顺序自动发现：

1. `DEVIN_TOKEN` 或 `WINDSURF_API_KEY` 环境变量；
2. Devin CLI 凭据文件——macOS/Linux 为 `~/.local/share/devin/credentials.toml`；Windows 为 `%APPDATA%\devin\credentials.toml`（其次 `%LOCALAPPDATA%\devin\credentials.toml`）。Windows CLI 不单独分发，随 [Windsurf 桌面应用](https://devin.ai/download)附带——安装后 `& "C:\Program Files\Windsurf\resources\app\extensions\windsurf\devin\bin\devin.exe" auth login` 即生成上述文件。

macOS 上也可以从 Devin 应用的本地状态提取 token：

```bash
sqlite3 ~/Library/"Application Support"/Devin/User/globalStorage/state.vscdb \
  "SELECT json_extract(value, '$.apiKey') FROM ItemTable WHERE key='windsurfAuthStatus';"
```

token 会过期。上游返回 `unauthenticated` 时，适配器会重读同一来源链——Devin CLI 刷新 `credentials.toml` 后代理无需重启即可自愈。

### 2. 配置

```bash
cp config.example.yaml config.yaml
```

编辑 `config.yaml` 填入 token（从 `config.example.yaml` 出发只需填 `devin.token`——base URL 和模型已预填为示例）。

### 3. 运行

预编译二进制（见 [Releases](https://github.com/min9lin9/devin2api/releases)，附 `checksums.txt` 供校验）。资产命名为 `devin-2api-{darwin,linux}-{amd64,arm64}`；Windows 以同名 `.zip` 分发（exe + `config.example.yaml` + LICENSE）：

```bash
# Linux 示例；macOS 用 devin-2api-darwin-arm64 或 -darwin-amd64
curl -fLO https://github.com/min9lin9/devin2api/releases/latest/download/devin-2api-linux-amd64
chmod +x devin-2api-linux-amd64
./devin-2api-linux-amd64 -config config.yaml
```

Windows：解压 `devin-2api-windows-amd64.zip`，编辑 `config.yaml`（token 可留空——第 1 步第 2 条覆盖了 Windsurf 附带 `devin.exe` 生成的凭据文件），然后在控制台运行 `devin-2api.exe -config config.yaml`。Ctrl+C 触发同样的优雅排空；关窗和 `taskkill /F` 不会——Windows 对控制台进程没有优雅终止手段。

从源码构建（生成的 proto 绑定已提交在 `crates/devin-proto`，无需额外工具链）：

```bash
cargo build --locked --release --bin devin-2api
./target/release/devin-2api -config config.yaml
```

Docker（镜像发布在 [GHCR](https://github.com/min9lin9/devin2api/pkgs/container/devin2api)）：

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/config.yaml:/app/config.yaml" \
  ghcr.io/min9lin9/devin2api --config /app/config.yaml
```

作为服务运行（可选）：

| 平台    | 托管方式                             | 布局                                                                                                         | 安装/升级                    |
| ------- | ------------------------------------ | ------------------------------------------------------------------------------------------------------------ | ---------------------------- |
| macOS   | launchd agent                        | bin `~/.local/bin` · config+state `~/Library/Application Support/devin-2api`                                 | `scripts/deploy.sh`          |
| Linux   | `systemd --user`                     | bin `~/.local/bin` · config `~/.config/devin-2api` · state `~/.local/state/devin-2api`                       | `scripts/deploy-linux.sh`    |
| Windows | 无——控制台，或 NSSM / Task Scheduler | exe `%LOCALAPPDATA%\Programs\devin-2api` · config `%APPDATA%\devin-2api` · state `%LOCALAPPDATA%\devin-2api` | `scripts/deploy-windows.ps1` |

二进制按平台惯例解析路径：config 走 `-config` flag → `DEVIN2API_CONFIG` → `./config.yaml` → 上表平台默认；state 走 `-state-dir` → `DEVIN2API_STATE_DIR` → 平台默认。两个部署脚本都是一条命令完成安装或升级（`--release latest` 拉预编译二进制），先验证 `/healthz` 报告新版本，再探 `GET /v1/models` 确认上游鉴权真的可用。脚本把仓库当 home——把 `config.yaml` 同步进平台 config 目录、在仓库里保留指向 state 目录的 `logs` 符号链接——所以先 clone 再跑：

```bash
git clone https://github.com/min9lin9/devin2api && cd devin2api
bash scripts/deploy-linux.sh --release latest    # macOS: scripts/deploy.sh
```

首次运行时 `config.yaml` 从 `config.example.yaml` 生成并填入随机 `auth.api_key`/`dashboard.password`，同时提示输入 Devin token（留空则回落自动发现）；要预设值就先 `cp config.example.yaml config.yaml` 再编辑。`--check` 报告已安装/运行中/最新版本；`--uninstall` 移除服务与二进制但保留 config 和 logs。

Linux 上若需要服务在登录会话结束后继续运行，执行 `loginctl enable-linger $USER`。

### 4. 验证

```bash
curl http://localhost:8080/healthz
# {"status":"ok","version":"...","uptime_seconds":12,"debug_logging":false}
```

## 用法

> **注意**：`/v1/*` 端点支持可选 API key 认证。在 `config.yaml` 设置 `auth.api_key` 后，客户端需发送 `Authorization: Bearer <api_key>` 或 `X-Api-Key: <api_key>`。留空则端点开放——如果还要绑定到回环以外的地址而不设 key，等于把你的 Devin 配额开放给整个网络。

端点：

- `POST /v1/responses` — OpenAI Responses（同路径 `GET` 协商 WebSocket transport）
- `POST /v1/chat/completions` — OpenAI Chat Completions
- `POST /v1/messages` — Anthropic Messages
- `GET /v1/models`、`GET /v1/models/{model}` — 带能力位的上游模型目录
- `GET /panel` — 管理面板（请求浏览、用量、配额、进程统计）；`dashboard.password` 保护

代理是**无状态**的：每个 HTTP 请求必须携带完整会话（`previous_response_id` 被接受但忽略——没有服务端响应存储）。WebSocket transport 下按连接维护多轮会话，增量输入被透明展开为完整 transcript。

用 OpenAI Responses API 客户端调用 `http://localhost:8080/v1/responses`。

非流式：

```bash
curl http://localhost:8080/v1/responses \
  -H "Content-Type: application/json" \
  -d '{
    "model": "glm-5-2",
    "input": "Hello"
  }'
```

流式（SSE）：

```bash
curl -N http://localhost:8080/v1/responses \
  -H "Content-Type: application/json" \
  -d '{
    "model": "glm-5-2",
    "input": "Hello",
    "stream": true
  }'
```

请求体遵循 OpenAI Responses API（`input`、`instructions`、`tools`、`stream` 等）。Anthropic Messages 客户端改调 `/v1/messages`：

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

## 配置

配置是启动时加载一次的 YAML 文件，未知字段会被拒绝。完整带注释的参考见 `config.example.yaml`。

| 字段                                             | 说明                                                                                                                                        | 必填 / 默认值                                                                                  |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `server.listen`                                  | HTTP 监听地址                                                                                                                               | 必填                                                                                           |
| `server.max_concurrency`                         | `/v1/*` 并发上限                                                                                                                            | `1024`                                                                                         |
| `devin.base_url`                                 | Devin Connect 服务 base URL                                                                                                                 | 设置 `devin.token` 后必填（代码无默认；`config.example.yaml` 用 `https://server.codeium.com`） |
| `devin.token`                                    | Devin 会话 token（`devin-session-token$...`）；留空 = 从 env / 凭据文件自动发现                                                             | 否——发现不到 token 时端点返回归一化的上游鉴权失败                                              |
| `devin.model`                                    | Devin 聊天模型 UID（如 `glm-5-2`）                                                                                                          | 设置 `devin.token` 后必填（代码无默认）                                                        |
| `devin.aliases`                                  | 客户端模型名 → 上游 UID 映射（`swe-2: swe-2-max`）；匹配顺序：精确 → 大小写不敏感 → `"*"` 兜底；alias 以 `alias_of` 标记出现在 `/v1/models` | 无                                                                                             |
| `devin.client_name`/`client_version`/`client_os` | 发往上游 metadata 的客户端身份                                                                                                              | `chisel` / `3000.2.17` / `mac`                                                                 |
| `devin.proxy`                                    | 上游代理地址（`http(s)://`、`socks5(h)://`）；留空 = 直连 / 环境变量                                                                        | 无                                                                                             |
| `devin.force_http1`                              | 每请求独立 TCP 连接（避免 HTTP/2 单连接多 stream 串行）                                                                                     | `true`                                                                                         |
| `devin.max_rpm`                                  | 上游消息速率上限（条/分钟，令牌桶）；`<=0` 不限速——429 冷却闩无论如何都生效                                                                 | `0`（不限；`config.example.yaml` 出厂 `80`）                                                   |
| `devin.gate_max_hold_seconds`                    | 冷却闩内令牌排队允许的最长等待秒数，超出快速失败 `429` + `Retry-After`                                                                      | `15`                                                                                           |
| `devin.gate_drip_interval_seconds`               | 闩内放行探针的间隔秒数——决定限流期间打到上游的速率与解闩探测频率                                                                            | `8`                                                                                            |
| `devin.gate_default_latch_seconds`               | 上游 `resource_exhausted` 未声明 reset 时刻时的兜底闩时长                                                                                   | `60`                                                                                           |
| `devin.gate_window_offset_seconds`               | 上游分钟桶界在本地分钟内的估计位置（第几秒）                                                                                                | `0`（本地 `:00`；实测桶界在本地 `:59` 附近）                                                   |
| `devin.gate_window_guard_seconds`                | 估计桶界两侧的停发死区秒数——死区内请求睡到下一窗口开放                                                                                      | `2`                                                                                            |
| `debug.enabled`                                  | 在 state 目录的 `logs/` 下写请求级调试日志                                                                                                  | `false`                                                                                        |
| `debug.retention_days`                           | 请求日志目录保留天数；`<=0` 不按时间清理                                                                                                    | `14`                                                                                           |
| `debug.max_total_mb`                             | `logs/` 总量上限（MB），超限从最旧目录开始删                                                                                                | `1024`                                                                                         |
| `debug.payload_hours`                            | 大体积阶段文件（03/04/06/attachments）的保留小时数；超时剥离负载、保留 meta/error 证据                                                      | `24`                                                                                           |
| `debug.keep_error_dirs`                          | 容量淘汰时保护的最新失败目录数（含 `error.json`）                                                                                           | `32`                                                                                           |
| `debug.quota_interval_minutes`                   | 配额快照采样间隔（分钟），写入 `logs/quota.jsonl`；`<=0` 不采样                                                                             | `5`                                                                                            |
| `debug.pprof_listen`                             | Rust 运行时诊断监听地址（如 `127.0.0.1:6060`）；无鉴权——只绑回环。字段名为兼容 Go 原版保留                                                  | 空（不启用）                                                                                   |
| `dashboard.password`                             | `/panel` 管理密码；留空 = 无需登录                                                                                                          | 无                                                                                             |
| `auth.api_key`                                   | `/v1/*` 端点访问密钥；留空不校验。客户端可发 `Authorization: Bearer <key>` 或 `X-Api-Key: <key>`                                            | 无（开放）                                                                                     |

注意：

- token 不会写进日志（以 `<redacted>` 脱敏）；
- `devin.token` 留空时请求照常发往上层并返回归一化的上游鉴权失败——任一发现来源出现 token 后下一个请求即成功，无需重启；
- `config.yaml` 已 gitignore——但无论如何别把真实 token 提交进 git。

## 与 Go 实现的兼容性

与 Go 实现（[WncFht/devin2api](https://github.com/WncFht/devin2api)）**共享配置文件、状态目录和日志格式**。迁移规则、单 writer 状态约定、回滚流程和五项已批准的行为差异见 [docs/compatibility.md](docs/compatibility.md)。摘要：

- 现有 `config.yaml`、`credentials.toml`、`logs/` 历史原样可读——无需迁移。
- **同一 state 目录同一时刻只允许一个 writer**——不要让两个守护进程同时写同一个 state 目录。
- 回滚通过恢复单独备份的 state 副本完成（流程见 [docs/deployment.md](docs/deployment.md)）。

## 实测性能

与 Go 实现的同机对比（4 核 Ryzen 5 5600G、回环 stub、配对 30 秒采样、bootstrap 置信区间）。完整数据与方法见 [docs/perf.md](docs/perf.md)。

- **SSE 流式吞吐低于 Go**：Chat SSE 各 cell 为 0.45–1.03×（并发与 debug 日志越高差距越大；c1 debug-on 为 Rust 占优）。Responses/Messages SSE 走同一条路径。
- **缓冲 JSON 与 WebSocket 更快**：chat JSON 1.28×，WebSocket 回合 1.47×。
- **内存占用显著更低**：所有 cell 的峰值 RSS 为 Go 的 0.22–0.75×。
- 可靠性门槛全部通过：10 万 stub 请求零意外失败/重复/缺失终止事件/泄漏 permit，取消 p99 15ms。

不声称在 SSE 路径上更快——实测数据不支持。

## 平台支持状态

发布矩阵共六个目标：Linux amd64/arm64（静态 musl）、macOS amd64/arm64、Windows amd64/arm64（zip）。当前状态：

- **已验证（本机原生构建 + 冒烟）**：`x86_64-unknown-linux-musl`——静态链接、无解释器。
- **仅 CI 验证**：其余五个目标在 `.github/workflows/release.yml` 的原生托管 runner 上构建。Windows arm64 在 amd64 runner 上交叉构建，无模拟冒烟。
- Go 专用诊断（pprof/fgprof）已替换为 Rust 诊断——各平台能力矩阵见 [docs/perf.md](docs/perf.md)。

## 文档

- **部署、回滚、离线冒烟**：[docs/deployment.md](docs/deployment.md)
- **Go↔Rust 兼容性、已批准例外、迁移**：[docs/compatibility.md](docs/compatibility.md)
- **实测性能 + Rust 运行时诊断/剖析**：[docs/perf.md](docs/perf.md)
- **上游协议逆向参考**：[docs/protocol.md](docs/protocol.md)
- **错误速查与排障**：[docs/troubleshooting.md](docs/troubleshooting.md)
- **全部二进制/flag 的命令参考**：[docs/commands.md](docs/commands.md)
- **工具链、代码生成、CI/发布**：[docs/toolchain.md](docs/toolchain.md)
- **许可证**：[MIT](LICENSE) · 第三方 crate 许可证：[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)

## 常见问题

**devin2api 是什么？**
一个本地代理，把 Devin 账号可用的模型暴露在 OpenAI/Anthropic 兼容 API 之后。Codex、Claude Code 和任意 SDK 都能用熟悉的端点调用 Devin 模型。

**与 Go 实现有什么区别？**
行为差异只有[兼容性文档](docs/compatibility.md)中列出的五项已批准例外。交付为无 Go 运行时依赖的单一静态二进制。

**如何安装？**
从 [Releases](https://github.com/min9lin9/devin2api/releases) 下载平台二进制并配合 `config.yaml` 运行，或用 `cargo build --locked --release` 构建。token 会从本地 Devin/Windsurf 安装自动发现。

**为什么有两个名字？**
仓库/项目名是 `devin2api`；二进制和发布产物名是 `devin-2api`。

**与 Cognition/Devin 有关系吗？**
没有。非官方项目，未获背书。遵守 Devin 服务条款的责任在用户。

## 致谢

本项目是 Go 实现 [WncFht/devin2api](https://github.com/WncFht/devin2api) 的 Rust 重实现，后者基于 [leookun/devin-2api](https://github.com/leookun/devin-2api)——感谢原作者们的工作。上游协议 schema（`proto/`）提取自 Devin CLI 二进制，出处哈希记录在 `proto/SHA256SUMS`。
