# Optional extension — Kaggle T4 + vLLM

Extension này dành cho học viên đã hoàn thành core. Kaggle GPU chỉ giải quyết tài
nguyên inference LLM; nó không tự giải quyết Kafka, Docker, state persistence,
network tunnel, quota, reproducibility hoặc failure recovery.

## Khi nào nên dùng

- Muốn thử OpenAI-compatible LLM serving với vLLM.
- Kaggle đang cấp T4 và session còn quota.
- Core tests/readiness đã pass ở local hoặc browser workspace.

Không dùng P100 làm baseline. [Kaggle thông báo P100 nghỉ ngày
2026-09-15](https://www.kaggle.com/product-announcements/735239) và T4x2 vẫn được
duy trì; availability/quota vẫn có thể thay đổi theo tài khoản.

## Notebook cells gợi ý

Kiểm tra GPU trước:

```bash
!nvidia-smi
!pip install -q --no-cache-dir "vllm==0.8.5"
```

Chạy model nhỏ phù hợp T4:

```bash
!vllm serve Qwen/Qwen3-4B-Instruct-2507 \
  --host 0.0.0.0 --port 8000 \
  --dtype half --max-model-len 4096 \
  --gpu-memory-utilization 0.85
```

Lệnh dùng vLLM 0.8.5 (CUDA 12.1 prebuilt wheel, phù hợp image Kaggle T4 hiện tại) và
và [model card Qwen3-4B-Instruct-2507](https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507).

Kiểm tra endpoint trong cùng session:

```bash
!curl -s http://127.0.0.1:8000/v1/models
```

## Bài tập Operator

Viết một adapter thay CPU classifier nhưng vẫn trả contract có output, model
identifier/version, latency và trace ID. So sánh P50/P95, memory và failure mode.
Không hard-code URL tunnel hay token vào notebook/repository.

## Giới hạn cần ghi trong ADR

- Session và GPU quota có thể hết giữa buổi.
- Tunnel public tạo thêm rủi ro security và latency.
- Model download làm cold start lâu; cần cache/preflight.
- Hai T4 không tự động tăng tốc nếu không cấu hình tensor parallel phù hợp.
- Kết quả extension không phải bằng chứng Kafka/Delta/MLflow core đã hoạt động.
