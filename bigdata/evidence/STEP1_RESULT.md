# Kết quả bước 1: Hạ tầng & Data

Đã hoàn thành và kiểm chứng ngày **30/10/2026**, giờ Việt Nam. Phạm vi: Docker, Spark Standalone, HDFS và dữ liệu gốc. Chưa làm sạch hoặc tính KPI.

## Kết quả thực tế

| Hạng mục | Kết quả |
|---|---|
| Dịch vụ | 1 Spark Master, 1 Spark Worker, 1 NameNode, 1 DataNode; cả 4 healthy |
| Phiên bản | Spark 3.5.8, Hadoop 3.4.1; base image khóa digest |
| Chuyến đi | Yellow Taxi tháng 01/2023, **3.066.766 dòng, 19 cột** |
| Dung lượng Parquet | **47.673.370 byte**, khoảng 45,5 MiB |
| Bảng vùng | **265 dòng**, 12.331 byte |
| Luồng | TLC → Windows → staging → HDFS → Spark executor |
| Toàn vẹn | SHA-256 bản trên HDFS trùng bản tải về, gồm Parquet, CSV và manifest |
| HDFS | HEALTHY; 3 tệp, 3 block; không có block thiếu hoặc hỏng |
| Thực thi Spark | Driver ở `spark-master`, executor ở `spark-worker`; mỗi lượt có 12 task kết thúc Success |
| Chạy lại | Đã dừng project, giữ volume, dựng lại và chạy toàn bộ luồng lần hai thành công |

Nguồn tải và SHA-256 đầy đủ: [input manifest](step1/after-restart/input-manifest.json). Schema gốc: [trip-schema.json](step1/after-restart/trip-schema.json). Ví dụ input thật: [sample-trips.json](step1/after-restart/sample-trips.json).

## Hai lượt chạy đã đạt

| Lượt | Run ID (UTC) | Toàn bộ lệnh All | Xác minh trong job Spark |
|---|---|---:|---:|
| Đầu tiên | `20261030T161704937Z` | 464,942 giây | 18,657 giây |
| Sau dừng/dựng lại | `20261030T162529747Z` | 295,873 giây | 18,758 giây |

Lượt đầu đã có base image tải sẵn trước khi chạy All. Lượt hai tái sử dụng dữ liệu tải về và các block HDFS hiện có. Thời gian All gồm dựng/chờ dịch vụ, chuẩn bị dữ liệu, kiểm tra hash, đọc Spark và thu log; không bao gồm lần tải base image trước đó.

Thời gian xác minh trong job bao gồm tạo SparkSession, kiểm tra executor, đọc schema, đếm và lấy mẫu. Phép đếm có thể tận dụng metadata Parquet. Đây là số đo vận hành bước 1; chưa phải benchmark xử lý, thử nghiệm mở rộng hoặc so sánh với Pandas. Snapshot bộ nhớ sau job không đại diện peak memory.

## Minh chứng có thể kiểm tra

- [Tổng hợp đối chiếu hai lượt](step1/replay-summary.json): số dòng, manifest và schema không đổi; 3 block HDFS giữ nguyên; source hash của lượt sau khớp các tệp hiện tại.
- [Lượt đầu](step1/first-run/run-summary.json), [lượt sau](step1/after-restart/run-summary.json): cả hai `action=All`, `status=passed`.
- [Log dừng cụm](step1/stop/compose-down.log): dừng/xóa container và mạng project, giữ named volume.
- [Log nhập HDFS lần hai](step1/after-restart/hdfs-import.log): cả ba tệp đều `EXISTS` và được xác minh hash, không ghi lại tệp đích HDFS.
- [HDFS fsck](step1/after-restart/hdfs-fsck.txt): block khỏe và replication bằng 1.
- [Spark read](step1/after-restart/spark-read.json), [Spark submit log](step1/after-restart/spark-submit.log), [Spark event log](step1/after-restart/spark-eventlog.jsonl): nguồn HDFS, application ID, task và executor thực.
- [Trạng thái container](step1/after-restart/compose-ps.txt), [năng lực Docker](step1/after-restart/docker-info.json), [snapshot tài nguyên](step1/after-restart/container-stats.jsonl).
- [Hash source/config](step1/after-restart/source-hashes.json), [digest base image](step1/after-restart/image-digests.jsonl).

Log đầy đủ, kể cả các lần sửa lỗi ban đầu, nằm trong `logs/`. Thư mục `evidence/step1` chỉ chọn các lượt thành công và lượt dừng dùng cho kiểm tra tái lập; các tệp này không bị `.gitignore` loại khỏi source.

## Môi trường và giới hạn

Docker thực tế được cấp khoảng **3,88 GiB RAM**, 12 CPU nhìn thấy từ engine. Tổng CPU quota của bốn container trong Compose là **2 CPU**; worker cung cấp một core và một executor 1 GiB. Cả hai lượt đã đạt trên môi trường này mà không sửa giới hạn RAM toàn máy. Mức 6 GiB trong README là đề xuất có thêm khoảng trống cho demo, không phải mức đã cấp thực tế.

Windows Update được kiểm tra vẫn là `Stopped / Disabled`; quá trình triển khai không bật dịch vụ này. Dữ liệu chuyến đi gốc được giữ nguyên. Các cổng UI chỉ mở trên localhost. Container dùng named volume và chuyển tệp qua Docker CLI, không cần share thư mục Windows.

Dung lượng HDFS báo trong `dfsadmin -report` là filesystem nhìn từ máy ảo Docker; không dùng giá trị đó thay cho dung lượng trống thật của ổ C.

Replication bằng 1 và một máy chủ vật lý phù hợp demo tối thiểu; chưa có thí nghiệm chịu lỗi hoặc mở rộng nhiều máy. Mới xác nhận dữ liệu đọc được và toàn vẹn so với bản tải về, chưa xác nhận tính chính xác nghiệp vụ từng chuyến đi.

## Bàn giao cho bước 2

Đường dẫn gốc: `hdfs://namenode:9000/taxi/raw/yellow/year=2023/month=01/yellow_tripdata_2023-01.parquet`.

Schema thực cho thấy hai trường thời gian là `timestamp_ntz`, `passenger_count` là `double`, các trường tiền là `double`. Bước 2 cần thống nhất quy tắc kiểu dữ liệu và chất lượng trước khi xử lý. Chưa tự cast, loại dòng, gộp trùng hoặc giới hạn ngày trong bước 1.

Hướng dẫn chạy từng phần, chạy lại và dừng cụm: [README](../README.md).
