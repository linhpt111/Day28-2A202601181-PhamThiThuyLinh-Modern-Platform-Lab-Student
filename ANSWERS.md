# ANSWERS.md — Day 28 Track 2

Số liệu và bằng chứng chi tiết ở [`REPORT.md`](REPORT.md), file evidence ở `evidence/`.

---

## 1. Trade-offs — những lựa chọn kỹ thuật và lý do

### 1.1 `dedupe_latest`: dedupe ở tầng nào?

Có ba chỗ có thể chống trùng: (a) Kafka producer idempotence, (b) trong batch trước khi
MERGE, (c) chỉ dựa vào `MERGE` của Delta.

Mình chọn **(b) + (c) cùng lúc**, và đó là lý do hàm này tồn tại:

- Kafka producer idempotence chỉ chống trùng do **retry của producer**, không chống được
  việc cùng một fact được submit hai lần bởi client (J2 mô phỏng đúng ca này).
- Nếu chỉ dựa vào `MERGE`, một batch chứa 2 bản của cùng `idempotency_key` sẽ làm
  Spark báo lỗi "multiple source rows matched the same target row" — MERGE cần source
  đã unique theo merge key. Nên phải collapse **trước** khi merge.
- Tie-break bằng tuple `(occurred_at, event_id)` thay vì chỉ `occurred_at`: hai delivery
  của cùng một fact có thể có timestamp giống nhau đến từng micro giây; nếu tie-break
  không deterministic thì kết quả phụ thuộc **thứ tự Kafka trả message**, và cùng một
  batch replay lại có thể cho ra row khác ⇒ mất tính reproducible của evidence.
- Sort theo `idempotency_key` ở output: để hai lần chạy trên cùng input cho ra **đúng
  cùng một thứ tự**, giúp so sánh/diff evidence được.

Đánh đổi: phải giữ toàn bộ batch trong memory (dict theo key). Với `MAX_MESSAGES = 500`
thì không sao; nếu batch lên hàng triệu event thì phải đổi sang streaming aggregation
hoặc dedupe bằng window function trong Spark.

### 1.2 `event_headers`: bỏ header hay gửi chuỗi rỗng?

Chọn **bỏ hẳn** `traceparent` khi không có trace. Gửi `traceparent: ""` là header W3C
**không hợp lệ**: consumer phía sau sẽ parse ra context rỗng/lỗi và — tệ hơn — có thể
tạo một trace mới rồi coi như "đã có parent", làm đứt liên kết mà không báo lỗi.
Không có header thì consumer biết rõ là "không có parent" và bắt đầu trace mới một cách
tường minh. Header value để dạng `bytes` vì Kafka header là byte thuần, không phải str.

### 1.3 `readiness_status`: vì sao mandatory/non-mandatory thay vì một cờ boolean?

Một hệ RAG vẫn còn giá trị khi thiếu một phần. Nếu chỉ có `ready`/`not_ready` thì
Feast chết ⇒ pod bị load balancer rút khỏi rotation ⇒ **mất toàn bộ khả năng trả lời**,
dù đáng ra vẫn có thể trả lời không cá nhân hóa. Ba mức cho phép:

- `not_ready` (mandatory fail) → **rút pod khỏi rotation**, vì không thể phục vụ đúng.
- `degraded` (non-mandatory fail) → **vẫn nhận request**, nhưng response ghi rõ
  `degraded=true` + `degraded_reasons` để caller và người vận hành biết chất lượng giảm.
- Thứ tự kiểm tra là mandatory **trước**: một mandatory fail phải "thắng" mọi
  non-mandatory fail, nếu không sẽ báo `degraded` trong khi thực tế không phục vụ được.

Đánh đổi: phải quyết định *cái gì là mandatory* — và quyết định đó là **cấu hình môi
trường**, không phải hằng số code. Chính điều này gây ra khác biệt mình đã kiểm chứng:
cùng một hàm, chạy trong container (`LAB28_VLLM_REQUIRE_REAL=false`) cho `degraded`,
chạy từ host (mặc định `True`) cho `not_ready`. Đây là *feature*, không phải bug — nhưng
nó cho thấy readiness semantics phải được document rõ, nếu không sẽ gây báo động sai.

### 1.4 `feast_online_request`: lấy `FEATURE_REFS` từ `contracts.py`

Không viết lại danh sách 4 feature trong hàm này. Danh sách feature là **contract**
giữa Feast registry, Spark export job và serving path; nếu copy ra nhiều nơi thì khi
thêm/bớt một feature sẽ có nơi cập nhật, nơi không, và lỗi chỉ lộ ra ở runtime dưới dạng
`NOT_FOUND` khó truy. Một nguồn sự thật ⇒ đổi một chỗ, mọi consumer đổi theo.

### 1.5 Seed qua gateway hay thẳng API?

Vì lỗi 405 intermittent của Envoy ([REPORT §9](REPORT.md#9-phát-hiện-đáng-lưu-ý-envoy-gateway-trả-405-không-đúng-intermittent)),
`seed --via-gateway` không vào đủ dữ liệu. Mình **không** sửa `envoy.yaml` để "cho dễ",
vì rate limit của gateway chính là bằng chứng IP08 phải giữ nguyên. Thay vào đó tách hai
việc: (a) nạp dữ liệu bằng `lab28 seed` đi thẳng API — vẫn là đường ingestion thật, vẫn
qua validation + idempotency key + traceparent, chỉ bỏ qua một hop Envoy; (b) chứng minh
IP08 riêng bằng burst GET có kiểm soát, thu được đúng cặp 200 + 429 kèm `x-request-id`.

## 2. Production gaps — còn thiếu gì để chạy thật

1. **`/ready` không cache, tự nó là bottleneck.** P50 828 ms / P99 2.17 s vì mỗi request
   fan-out 5 probe live (đo được ở [REPORT §7](REPORT.md#7-load-profile--phân-tích-bottleneck)).
   Khi scale pod, chính readiness probe của K8s sẽ bơm tải lên Kafka/MLflow/Qdrant/Feast.
   Cần probe nền + cache TTL ngắn, `/ready` chỉ đọc snapshot.
2. **IP07 chưa chứng minh được với vLLM thật** — không có GPU. Đang `degraded` ở LLM.
   Production cần vLLM thật + kiểm tra danh tính (`/version`, `/v1/models`, metric `vllm:`),
   cộng thêm timeout/circuit breaker và fallback rõ ràng khi GPU node chết.
3. **Envoy 405 intermittent** — một defect hạ tầng chưa có root cause dứt điểm. Chạy
   production với nó thì một tỉ lệ request ghi dữ liệu sẽ bị mất một cách âm thầm
   (client thấy 405 chứ không phải 5xx nên thường **không retry**). Phải fix hoặc pin
   Envoy version khác trước khi lên thật, và bật access log ở gateway để quan sát.
4. **Chưa apply lên cluster thật.** Manifest K8s + Argo CD pass contract validation nhưng
   drift/self-heal/rollback chưa demo trực tiếp trên cluster (thiếu kubeconfig).
5. **Rate limit 10 req/s là local per-Envoy**, không phải global. Nhiều replica gateway ⇒
   giới hạn thật = 10 × số replica, không đoán được. Production cần global rate limit
   service (RLS) nếu muốn quota theo tenant.
6. **Airflow DAG `schedule=None`**, trigger thủ công. Production cần Kafka sensor hoặc
   schedule ngắn + `max_active_runs` hợp lý, kèm alert cho consumer lag.
7. **Cold start rất đắt.** DAG run đầu tiên mất 319 s chủ yếu vì tải embedding model
   trong container Airflow (task `index_new_documents` 227 s). Cần bake model vào image
   hoặc mount cache dùng chung, nếu không mỗi lần scale/rollout sẽ chậm và dễ timeout.
8. **Secret quản lý thủ công.** Password Airflow đang sinh ra file local; production cần
   secret manager (External Secrets/Vault), không để sinh ra trong workspace.
9. **Không có SLO/alert routing thật.** Có rule group `lab28-slo` (2 rule) nhưng chưa nối
   Alertmanager → on-call. Alert không tới người thì không phải alert.

## 3. Đóng góp

Làm **cá nhân** (không theo nhóm), nên một người thực hiện toàn bộ các vai:

| Vai | Phần đã làm |
|---|---|
| Ingestion & Orchestration (IP01–IP02) | `event_headers`; verify header traceparent + idempotency-key trên `data.raw`; trigger DAG qua REST API v2, thu run/task/asset event |
| Data & ML (IP03–IP04–IP06) | `dedupe_latest`, `feast_online_request`; verify Delta MERGE history + time travel; Feast materialize + online read; MLflow release/champion + rollback (J3) |
| Serving & Retrieval (IP05–IP07) | Index Qdrant với point ID deterministic; xác nhận IP07 `UNVERIFIED` đúng cách, không fake vLLM |
| Platform & Observability (IP08–IP10) | `readiness_status`; thu evidence gateway 200/429; Prometheus targets + rule; Grafana dashboard; trace 6 span/3 service qua Jaeger; validate manifest K8s/GitOps |
| Presenter / Incident Commander | [`REPORT.md`](REPORT.md), evidence index, phân tích bottleneck, và điều tra sự cố Envoy 405 ở [REPORT §9](REPORT.md#9-phát-hiện-đáng-lưu-ý-envoy-gateway-trả-405-không-đúng-intermittent) |

## 4. Reflection

### 4.1 Điều khó nhất

**Phân biệt "code mình sai" với "hạ tầng đang flake".** Lần chạy J1 đầu tiên fail 7 test,
lần hai fail toàn bộ do timeout, lần ba fail 2 test. Rất dễ kết luận vội là `dedupe_latest`
hoặc `readiness_status` sai rồi đi sửa code đang đúng. Thứ tự mình đã làm để tách bạch:

1. Đọc kỹ *test nào* fail: `test_the_pipeline_run_succeeded` **pass** trong khi
   `test_the_lakehouse_advanced_and_holds_the_row` fail ⇒ DAG chạy xong nhưng batch rỗng
   ⇒ vấn đề ở **event có tới Kafka không**, không phải ở logic MERGE.
2. Query thẳng Airflow REST API: run `it-f21468ae` thực ra **success sau 319 s**, trong khi
   test chỉ chờ 300 s. Nguyên nhân là task `index_new_documents` mất 227 s **tải embedding
   model lần đầu** trong container. Chạy lại khi cache đã ấm → 12/12 pass. Đây là
   *cold start*, không phải bug.
3. Với lỗi 405 còn lại: so sánh cùng một request qua gateway (`:8080`) và thẳng API
   (`:8000`). Direct 20/20 = 202, qua gateway ~50% fail ⇒ khoanh vùng chắc chắn là Envoy.
   `x-envoy-upstream-service-time: 0` xác nhận request **chưa từng tới** FastAPI.

Bài học: bằng chứng phải chỉ đúng **ranh giới nào** hỏng. "Test đỏ" không phải là chẩn đoán.

### 4.2 Trade-off khó nhất đã chọn

Khi `seed --via-gateway` bị 405/429, có hai lối: (a) sửa `gateway/envoy.yaml` cho dễ thở,
(b) giữ nguyên hạ tầng và tách đường nạp dữ liệu khỏi đường lấy bằng chứng IP08.

Mình chọn **(b)**. Sửa rate limit sẽ làm chính bằng chứng IP08 mất giá trị — cặp 200/429
là thứ đề bài yêu cầu chứng minh. Đổi config để test xanh là tự làm hỏng phép đo của mình.
Cái giá phải trả: `seed --via-gateway` vẫn exit 1 và mình phải giải thích dài dòng trong
[REPORT §9](REPORT.md#9-phát-hiện-đáng-lưu-ý-envoy-gateway-trả-405-không-đúng-intermittent)
thay vì có một dòng "tất cả xanh" cho đẹp.

### 4.3 Điều sẽ cải tiến nếu có thêm thời gian

1. **Root-cause dứt điểm lỗi Envoy 405** — bật access log ở gateway, thử pin sang bản
   Envoy stable khác, và dựng một repro tối giản để báo upstream. Hiện mới khoanh vùng
   được, chưa kết luận được nguyên nhân.
2. **Cache `/ready`** (probe nền + TTL 1–5 s) để P50 không còn 828 ms — đây là cải tiến
   có tác động lớn nhất và rẻ nhất trong danh sách.
3. **Bake embedding model vào image Airflow** để cold start không còn 227 s và DAG run
   đầu tiên không vượt timeout của test.
4. **Nối vLLM thật** để đóng IP07 — phần duy nhất còn `UNVERIFIED` về mặt kỹ thuật.
5. **Apply manifest lên cluster thật** (kind/minikube) để demo được drift + self-heal +
   desired-state rollback, thay vì chỉ validate contract.
6. **Nối Alertmanager** cho rule group `lab28-slo` — alert hiện chưa tới ai.

## 5. Ba câu tự kiểm

**Nếu Kafka gửi lại nguyên một batch, chuyện gì xảy ra?** `dedupe_latest` collapse về
1 row/`idempotency_key`, rồi Delta `MERGE` khớp row cũ và update thay vì insert.
Delta version **có thể tăng** (một commit MERGE mới) nhưng **số row không tăng** —
J2 assert đúng cả hai điều này. Qdrant cũng không trùng vì point ID là `uuid5(namespace, doc_id)`.

**Nếu Feast chết giữa demo?** `/ready` → `degraded` (Feast là non-mandatory), request
vẫn được nhận, response ghi `degraded_reasons`. Answer mất phần cá nhân hóa nhưng vẫn
grounded trên Qdrant. Đây chính là J4.

**Làm sao chứng minh một câu trả lời tái lập được?** `ServingEvidence` gắn trace ID +
`mlflow_release_version` + `mlflow_run_id` + `vllm_model_id` + `embedding_model_id` +
`delta_version`. Từ đó tra lại được đúng trace trong Jaeger, đúng model version trong
MLflow, và đúng version Delta bằng time travel.
