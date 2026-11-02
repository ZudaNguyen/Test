# Bước 1 — Hạ tầng & Data

Dựng 4 container Spark/HDFS, tải Yellow Taxi tháng 01/2023, đưa dữ liệu gốc lên HDFS và kiểm tra Spark đọc được dữ liệu. Chưa làm sạch hoặc phân tích.

## 1. Chuẩn bị

- Mở Docker Desktop, chờ engine chạy ở chế độ Linux containers.
- Có Python 3.11+ và PowerShell 5.1+; cần Internet cho lần tải đầu tiên.
- Mở terminal PowerShell tại thư mục dự án chứa `compose.yaml`.

## 2. Chạy toàn bộ bước 1

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\step1.ps1 -Action All
```

Lệnh tự build image, dựng cụm, tải dữ liệu, đưa lên HDFS, kiểm tra SHA-256 và chạy Spark đọc thử. Lần đầu lâu hơn vì cần tải image và dữ liệu.

Kết quả thành công có dòng:

```text
VERIFIED: 3066766 trip rows; 265 zone rows.
DONE. Evidence: ...
```

Chạy lại `All` sẽ kiểm tra và tái sử dụng dữ liệu đã có.

## 3. Khởi động và kiểm tra khi đã có dữ liệu

```powershell
# Dựng lại hoặc khởi động hạ tầng
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\step1.ps1 -Action Start

# Kiểm tra Spark đọc dữ liệu từ HDFS
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\step1.ps1 -Action Verify
```

## 4. Dừng và bật lại, giữ container và dữ liệu

```powershell
# Dừng
docker --context desktop-linux compose stop

# Bật lại các container đã dừng
docker --context desktop-linux compose start
```

**Lưu ý:** `-Action Stop` trong script dùng `compose down`, nên gỡ container và mạng nhưng vẫn giữ volume dữ liệu. Nếu đã dùng lệnh đó, chạy `-Action Start` để dựng lại. Dùng `compose stop` ở trên khi chỉ muốn tạm dừng.

## 5. Xem kết quả

- [Spark Master](http://localhost:8080): có **1 worker ALIVE**; sau Verify có application **FINISHED**.
- [HDFS NameNode](http://localhost:9870): tab **Datanodes** có **1 Live DataNode**.
- `data/raw/`: dữ liệu gốc và manifest nguồn tải/SHA-256.
- `logs/<run_id>/`: log và kết quả mỗi lần chạy; xem `run-summary.json`, `spark-read.json`.
- [Minh chứng bước 1](evidence/STEP1_RESULT.md): kết quả đã kiểm chứng và log chạy lại.

Lệnh liệt kê dữ liệu trên HDFS:

```powershell
docker --context desktop-linux compose exec namenode /opt/hadoop/bin/hdfs dfs -ls -R /taxi
```

Thời gian Verify chỉ là kiểm tra đọc dữ liệu, chưa phải benchmark xử lý. AI hỗ trợ cấu hình/script; nhóm cần tự chạy lại và hiểu phần bàn giao.
