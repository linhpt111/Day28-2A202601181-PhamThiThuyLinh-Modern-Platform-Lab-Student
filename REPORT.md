# Báo cáo thực hành — Day 28 Track 2 (Platform Integration & Production Readiness)

- **Người thực hiện:** Phạm Thị Thùy Linh (`linhpt111`) — làm **cá nhân**
- **Ngày chạy:** 2026-09-03 → 2026-09-04 (giờ máy local, UTC+7)
- **Máy chạy:** Windows 11, 22 vCPU, ~629 GB đĩa trống, Docker Desktop 29.1.3 / Compose v2.40.3
- **Profile preflight:** `local-standard` → chạy được **toàn bộ hệ thống** (base + `--profile full`)
- **Trạng thái:** bản nháp (`tạm`) — phần GPU/vLLM và LangSmith chưa có credential, xem [§7](#7-những-phần-chưa-verify-được--cần-quyềntài-khoản)

> Tất cả số liệu dưới đây lấy từ log lệnh thật trong phiên chạy này. Các file
> evidence JSON nằm ở `evidence/` (12 file). Không có secret, `.env`, database,
> cache hay model weights nào được commit.

---

## 1. Bốn hàm cần hoàn thiện (Bước 5)

Chỉ sửa đúng một file: `src/lab28_platform/integration_tasks.py`. Không sửa/xóa test nào,
không che `NotImplementedError`.

| Hàm | IP | Cách làm | Kết quả test |
|---|---|---|---|
| `event_headers` | IP01 + IP10 | Luôn trả `idempotency-key` dạng `bytes`; chỉ thêm `traceparent` khi có trace (bỏ hẳn key thay vì gửi chuỗi rỗng). Không hard-code giá trị nào. | `1 passed, 3 deselected` |
| `dedupe_latest` | IP03 | Duyệt input **đúng một lần**, giữ 1 event/`idempotency_key`, so sánh tuple `(occurred_at, event_id)` để tie-break không phụ thuộc thứ tự Kafka, trả về sắp xếp theo `idempotency_key`. | `1 passed, 3 deselected` + `22 passed` (`tests/test_delta_merge_idempotency.py`) |
| `feast_online_request` | IP04 | `entities={"asker_id":[asker_id]}`, `features=list(FEATURE_REFS)` (lấy từ `contracts.py`, không viết lại danh sách), `full_feature_names=False`. | `1 passed, 3 deselected` |
| `readiness_status` | IP07 + IP08 | Thứ tự ưu tiên: có `mandatory` fail → `not_ready`; chỉ non-mandatory fail → `degraded`; còn lại → `ready`. | `1 passed, 3 deselected` |

**Bằng chứng — checkpoint tổng sau Bước 5:**

```text
uv run pytest starter-tests -q          → 4 passed
uv run pytest starter-tests tests -q    → 87 passed
uv run ruff check .                     → All checks passed!
scripts/verify_matrix.py                → OK  245 checks passed
scripts/check_portability.py            → OK  host-path and shell independent
scripts/validate_manifests.py           → Kubernetes and GitOps manifest contracts passed
```

## 2. Docker config + khởi động hệ thống (Bước 6–8)

```text
docker compose --env-file ports.template config --quiet                  → exit 0
docker compose --env-file ports.template --profile full config --quiet   → exit 0
```

Base profile: 12 service `running`/`healthy`. Full profile thêm `airflow` +
`spark-connect` → **14 service**, tất cả `healthy`.

```text
lab28 topics  → data.raw, data.processed, model.events, data.raw.dlq = created
lab28 index   → points_upserted 13, points_total 13
lab28 release → lab28-rag-release v2, alias=champion, run_id f430cb76...
lab28 seed    → documents 13 accepted / 0 rejected; feedback 12 accepted / 0 rejected
```

## 3. Mười điểm kết nối (Definition of Done)

`evidence/integration-report.json`: **score 83**, 6 verified / 5 passing / 4 unverified-from-serving-process.
Bốn điểm `unverified` là do bản chất thiết kế (không probe được từ tiến trình serving) và
đã được chứng minh bằng evidence thu ngoài tiến trình, đúng như CLI yêu cầu.

| IP | Boundary | Trạng thái | Bằng chứng cụ thể |
|---|---|---|---|
| IP01 | HTTP → Kafka | ✅ | `ip01-kafka-consume.json`: 2 message trên `data.raw`, key = `entity_id`, header `traceparent=00-b4534e10…-01` + `idempotency-key` + `schema_version` |
| IP02 | Kafka → Airflow 3 | ✅ | `ip02-airflow-run.json`: run `ev-37596084` state **success** (57s); 4/4 task success; 4 asset event: `lab28://delta/{documents,feedback}`, `lab28://qdrant/lab28_documents`, `lab28://feast/asker_activity` |
| IP03 | Airflow/Spark → Delta | ✅ | `ip03-delta-history.json`: feedback v19 (35 rows), documents v11 (52 rows); history 19×`MERGE` + `CREATE TABLE`; time-travel v0→v19 (0 → 35 rows) |
| IP04 | Delta → Feast | ✅ | `ip04-feast-online.json`: HTTP 200, cả 4 feature `PRESENT` cho `ev-asker-37596084` (avg_rating 5.0, feedback_count 1, negative_ratio 0.0, delta_version 20) |
| IP05 | Delta docs → Qdrant | ✅ | `ip05-qdrant-search.json`: 52 point, embedding `paraphrase-multilingual-MiniLM-L12-v2@faf4aa42…`, point ID deterministic (uuid5) |
| IP06 | Eval → MLflow Registry | ✅ | `ip06-mlflow-release.json`: `lab28-rag-release v2 is champion`, run_id `f430cb76…`; J3 chứng minh promote + rollback alias |
| IP07 | RAG → vLLM thật | ⚠️ **UNVERIFIED** | `ip07-vllm-identity.json`: `unreachable: ConnectError`, `is_real_vllm=false`. Không có GPU/endpoint — xem [§7](#7-những-phần-chưa-verify-được--cần-quyềntài-khoản). **Không fake server.** |
| IP08 | Client → Envoy gateway | ✅ | `ip08-gateway.json`: `200 OK` (x-request-id `67d07dd5…`) và `429 Too Many Requests` (x-request-id `929df1b5…`) — rate limit 10 token/1s hoạt động đúng |
| IP09 | → Prometheus/Grafana | ✅ | `ip09-prometheus-targets.json`: 9/10 target `up` (chỉ `lab28-vllm-optional` down = đúng dự kiến), rule group `lab28-slo` (2 rule); `ip09-grafana-dashboards.json`: dashboard `Lab 28 Platform Overview` |
| IP10 | → OTLP trace | ✅ | `ip10-trace.json`: trace `44cbe2cbfba3f43a05f175a98f89e7fb`, **20 span, 3 service**, `missing_required: []` — đủ 6 span bắt buộc: `lab28.gateway.request` → `lab28.api.ingest` → `lab28.kafka.produce` → `lab28.kafka.consume` → `lab28.airflow.dag` → `lab28.spark.delta_merge` |

## 4. Năm luồng kiểm thử (J1–J5) + gate còn lại

```text
pytest integration-tests -m "not gpu and not langsmith" -q
  → 56 passed, 16 deselected (6m16s)
```

Chi tiết từng luồng:

| Luồng | Kết quả | Ý nghĩa |
|---|---|---|
| J1 golden path | **12 passed, 3 skipped** | API → Kafka → Airflow → Delta → Feast/Qdrant → response, cùng một trace ID |
| J2 idempotent replay | **9 passed** (chạy lại 3 lần đều 9/9) | Gửi lại cùng một fact không tạo row trùng; Delta version có thể tăng nhưng row vẫn là 1 |
| J3 promotion/rollback | ✅ trong `15 passed` | Đổi champion sang version mới rồi rollback alias, không sửa code |
| J4 degraded recovery | ✅ trong `15 passed` | Thành phần không bắt buộc chết → `degraded` → hồi phục |
| J5 trace/metrics continuity | ✅ trong `20 passed` | trace ID + golden signals giữ xuyên luồng |
| gateway rate limit / prometheus targets / trace span coverage | ✅ trong `20 passed` | 3 gate cuối |

## 5. Readiness — `ready` / `degraded` / `not_ready`

Đây là chỗ dễ nhầm nhất nên tách rõ:

```text
GET http://localhost:8000/ready  → "degraded"
GET http://localhost:8080/ready  → "degraded"   (qua Envoy, IP08 forward đúng)
GET http://localhost:8080/health → {"status":"alive","service":"lab28-api"}
```

- `/health` = **liveness**, không chạm dependency → luôn 200 khi process còn sống.
- `/ready` = **readiness**, fan-out kiểm tra 5 dependency thật.
- Hệ thống báo `degraded` vì `vllm` **không reachable** nhưng trong container
  `LAB28_VLLM_REQUIRE_REAL=false` ⇒ vLLM là **non-mandatory** ⇒ theo đúng
  `readiness_status()` mình implement: non-mandatory fail → `degraded`, không phải `not_ready`.
- Lưu ý đã kiểm chứng: chạy `uv run lab28 ready` **từ host** cho ra `not_ready`
  vì host không có biến `LAB28_VLLM_REQUIRE_REAL=false` (mặc định trong code là `True`
  ⇒ vLLM thành mandatory). Đây là khác biệt **cấu hình môi trường host vs container**,
  không phải bug logic — cùng một hàm, khác input `mandatory`.

## 6. Load profile & phân tích bottleneck

```text
uv run python load-tests/run_profile.py --requests 200 --workers 8
{
  "requests": 200, "workers": 8,
  "status_counts": { "200": 200 },
  "latency_ms": { "p50": 827.6, "p95": 1409.4, "p99": 2169.3 }
}
```

**Bottleneck:** P50 ~828 ms cho một endpoint health-check là rất cao. Nguyên nhân:
`/ready` **không cache** — mỗi request fan-out kiểm tra live 5 dependency
(Kafka metadata, MLflow registry + tải artifact alias, Qdrant count, Feast health, vLLM probe).
Với 8 worker song song, các probe này xếp hàng ⇒ đuôi p99 hơn 2s.

Đề xuất production (chưa implement, chỉ phân tích): cache kết quả probe với TTL
ngắn (1–5s) + probe nền bất đồng bộ, để `/ready` trả về snapshot thay vì gọi
đồng bộ; giữ `/health` tuyệt đối rẻ cho load balancer. Nếu không, chính
readiness probe sẽ trở thành nguồn tải lên các dependency khi scale pod lên.

Rate limit của gateway cũng đo được: 20 request GET `/ready` song song → 6×429 + 14×200,
đúng token bucket `max_tokens=10, tokens_per_fill=10, fill_interval=1s`.

## 7. Những phần chưa verify được — cần quyền/tài khoản

Ghi rõ theo yêu cầu `SUBMISSION.md` (báo `UNVERIFIED`, **không giả lập**):

| Hạng mục | Trạng thái | Cần gì để hoàn tất |
|---|---|---|
| **IP07 — vLLM thật** | `UNVERIFIED` | Máy này không có GPU NVIDIA. Cần **tài khoản Kaggle** (T4, còn quota) theo `KAGGLE_GPU_EXTENSION.md`, hoặc **endpoint vLLM + model ID do giảng viên cấp**. Sau đó set `LAB28_VLLM_BASE_URL` / `LAB28_VLLM_MODEL_ID` (không commit URL/token). Gate yêu cầu chứng minh `/version`, `/v1/models`, và metric prefix `vllm:` — server chỉ giả OpenAI API sẽ **không** pass. 3–7 test bị `skip` ở mỗi suite là do gate này. |
| **LangSmith tracing** | `UNVERIFIED` | Thiếu `LANGSMITH_API_KEY` ⇒ 1 test `skip`. Cần API key của lớp. (IP10 vẫn đã chứng minh đầy đủ bằng OTLP/Jaeger.) |
| **Kubernetes/Argo CD chạy thật** | Manifest ✅ / cluster ❌ | `scripts/validate_manifests.py` pass (contract K8s + GitOps hợp lệ), nhưng chưa apply lên cluster thật vì không có cluster/kubeconfig. Cần cluster (kind/minikube/lớp cấp) để demo drift + self-heal + desired-state rollback trực tiếp. |

Ngoài ra, hai vấn đề **môi trường** đã gặp và cách xử lý (không bypass gì):

1. **`pytest.exe` bị chặn** — `An Application Control policy has blocked this file (os error 4551)`.
   Xử lý: chạy `python -m pytest` (cùng interpreter, cùng test, không sửa test).
   Có thể cần whitelist trong chính sách Application Control của máy nếu muốn dùng `uv run pytest`.
2. **Thư mục temp cũ bị khóa** — `PermissionError [WinError 5]` trên
   `%LOCALAPPDATA%\Temp\pytest-of-phamt` (di sản từ phiên trước, `icacls` cũng bị
   Access denied). Xử lý: truyền `--basetemp` sang thư mục khác. Nếu muốn dọn hẳn
   thì cần quyền admin để xóa/đổi ACL thư mục đó.

## 8. Phát hiện đáng lưu ý: Envoy gateway trả 405 không đúng (intermittent)

Đây là phát hiện thật trong lúc chạy, **chưa sửa** (không tự ý đổi `gateway/envoy.yaml`
vì đó là hạ tầng đề bài cấp, và đổi nó sẽ làm sai lệch bằng chứng IP08):

**Triệu chứng:** request non-GET qua Envoy `:8080` **thỉnh thoảng** bị trả

```text
HTTP/1.1 405 Method Not Allowed
allow: OPTIONS,GET
server: envoy
x-envoy-upstream-service-time: 0
x-envoy-decorator-operation: lab28.gateway.request

# HTTP 405 Method Not Allowed: POST; use OPTIONS or GET
```

**Đã khoanh vùng được:**

- Chỉ xảy ra **qua gateway**; POST trực tiếp vào API `:8000` chạy 20/20 = 202, không lỗi lần nào.
- `x-envoy-upstream-service-time: 0` ⇒ Envoy **tự trả lời**, request không tới FastAPI.
- Không liên quan rate limit (429 là mã khác, và xảy ra cả khi bucket còn token).
- Không phải 405 của FastAPI: FastAPI trả JSON `{"detail":"Method Not Allowed"}` với `allow: GET`,
  còn cái này là plain-text `# HTTP …` với `allow: OPTIONS,GET`.
- Tái hiện được bằng `curl -X DELETE` (100%) và `curl -X POST` (~40–50% khi gửi liên tiếp).
- Nặng nhất khi hai request đi **back-to-back trên cùng connection keep-alive**
  (đúng pattern của J2: gửi trùng 2 lần liên tiếp để chứng minh idempotency) →
  lúc đầu J2 fail 5/5 lần vì lý do này.
- Sau khi retry, J2 pass **9/9 ba lần liên tiếp** và J1 pass **12/12** ⇒ logic nền tảng
  đúng, đây là flake tầng hạ tầng (`envoyproxy/envoy:v1.39.1` + Docker Desktop/Windows).

**Ảnh hưởng thực tế:** `lab28 seed --via-gateway` bị 405/429 lẫn lộn nên exit 1, dù
README kỳ vọng "0 rejected". Mình đã seed qua `lab28 seed` (đi thẳng API — vẫn là
đường ingestion thật, vẫn qua validation + idempotency key + traceparent) để dữ liệu
vào đủ, rồi thu evidence IP08 riêng bằng burst GET có kiểm soát.

**Đề nghị:** nếu lỗi này còn ở môi trường của lớp, nên thử pin Envoy sang bản
stable khác hoặc bật access log ở gateway để xác nhận request có tới upstream không.

## 9. Sự cố đã tạo: dấu hiệu → quan sát → nguyên nhân → khôi phục

Scenario **"Feast down"** theo `runbooks/failure-injection.md`. Chỉ thao tác service
trong project `lab28-platform`, **không dùng `down -v`** (sẽ xóa state).
Evidence: `evidence/incident-feast-outage.json`.

**Dấu hiệu dự đoán trước khi inject:** Feast là dependency **non-mandatory**, nên
`readiness_status()` phải trả `degraded` chứ **không** phải `not_ready`; ingestion phải
vẫn trả 202 vì đường ghi chỉ cần Kafka; event gửi trong lúc Feast chết phải nằm lại
Kafka và được nạp sau khi hồi phục.

| Thời điểm (UTC) | Hành động | Quan sát |
|---|---|---|
| 17:11:58 | Baseline | `/ready` = `degraded` (chỉ vllm false); Delta feedback **v21 / 37 rows**; Qdrant 54 points |
| 17:12:27 | `docker compose stop feast` | container stopped |
| 17:12:34 | Quan sát | `/ready` = **`degraded`**; `feast ready=false → unreachable: ConnectError`; các thành phần khác vẫn true ✅ đúng dự đoán |
| 17:12:52 | POST `/api/v1/feedback` **trong lúc sự cố** | **HTTP 202 accepted**, `event_id d37674ae…`, `asker_id inc-b8d6c2f3` ✅ ingestion không bị chặn |
| 17:13:01 | `docker compose start feast` | starting |
| 17:13:25 | Feast healthy (~24 s) | `/ready` quay lại `degraded` chỉ còn vllm false ✅ |
| 17:14:36 | DAG run `inc-b8d6c2f3` | **success**, 56 s — nạp event bị buffer trong lúc sự cố |
| 17:14:59 | Kiểm chứng | Delta **v22 / 38 rows**; **đúng 1 row** cho `inc-b8d6c2f3`; Feast online trả `PRESENT` cả 4 feature (`avg_rating 4.0, feedback_count 1, negative_ratio 0.0, delta_version 22`) |

**Nguyên nhân:** chủ động inject (`docker compose stop feast`) — không phải lỗi thật.

**Khôi phục:** start lại service → healthy 24 s → chạy một DAG run để drain Kafka.

**Chứng minh không mất dữ liệu:** event gửi *trong lúc* Feast chết vẫn nằm an toàn trên
`data.raw` (offset chỉ commit **sau** khi MERGE thành công), và sau khi hồi phục nó vào
Delta **đúng 1 row** — không mất, cũng không nhân đôi. Row count 37 → 38 (+1 đúng bằng
số event đã gửi), version 21 → 22 (một commit MERGE mới).

**Bài học:** phân biệt mandatory/non-mandatory là thứ quyết định sự cố này chỉ làm
*giảm chất lượng* thay vì *sập dịch vụ*. Nếu Feast bị đánh dấu mandatory, pod đã bị
loại khỏi rotation và toàn bộ request sẽ hỏng, dù retrieval + LLM vẫn chạy được.

## 10. Checklist demo — trạng thái

- [x] Sơ đồ kiến trúc, người phụ trách, 10 điểm kết nối
- [x] Luồng chạy đúng có run ID (`ev-37596084`), trace ID (`44cbe2cb…`), Delta version (feedback v19 / documents v11), MLflow version (v2 champion)
- [x] Kafka gửi lại nhưng Delta không có row trùng (J2 9/9, chạy lại 3 lần)
- [x] Sự cố + khôi phục + chứng minh không mất dữ liệu (J4 degraded → recovery)
- [x] Golden signals trên Grafana + một trace Jaeger xuyên hệ thống (20 span/3 service)
- [x] MLflow promote rồi rollback alias mà không sửa code (J3)
- [x] Giải thích `ready` / `degraded` / `not_ready` ([§5](#5-readiness--ready--degraded--not_ready))
- [x] Manifest K8s/GitOps hợp lệ (`validate_manifests.py` pass) — chưa apply lên cluster thật
- [x] Người làm giải thích được lựa chọn kỹ thuật của từng phần ([ANSWERS.md](ANSWERS.md))
- [x] Không commit secret / token / DB / cache / model weights
- [ ] IP07 với vLLM thật — **chờ GPU/endpoint** ([§7](#7-những-phần-chưa-verify-được--cần-quyềntài-khoản))

## 11. Cách chạy lại (reproduce)

```text
uv sync --frozen --python 3.11 --extra dev --extra integration --no-editable
uv run lab28 preflight
docker compose --env-file ports.template --profile full up -d --build --wait

uv run lab28 topics
uv run lab28 index --source file
uv run lab28 release
uv run lab28 seed
uv run lab28 evidence

python -m pytest starter-tests tests -q
python -m pytest integration-tests -m "not gpu and not langsmith" -q
uv run python load-tests/run_profile.py --requests 200 --workers 8
```

Lưu ý môi trường Windows: đặt `PYTHONUTF8=1` trước khi chạy `lab28 release`
(MLflow in emoji, console cp1252 sẽ `UnicodeEncodeError`), và dùng
`python -m pytest` nếu `pytest.exe` bị Application Control chặn.
