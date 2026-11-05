# Kiểm chứng runtime — bản TLC

**Kết quả: ĐẠT**, kiểm tra ngày 03/11/2026, múi giờ Việt Nam. Đây là kết quả của source lấy từ `sangvominh/TLC`, Hadoop 3.5.0 và Spark 4.1.4.

## Luồng đã chạy thực tế

1. Khởi động Docker Desktop, tải hai image đúng phiên bản.
2. Tạo và khởi động NameNode, DataNode, Spark Master, Spark Worker.
3. Xác nhận 1 worker ALIVE và 1 DataNode sống.
4. Chạy `scripts/upload_to_hdfs.sh` để upload Parquet.
5. Chạy nguyên bản `src/test_spark.py` qua Spark Standalone.
6. Đối chiếu SHA-256 của dữ liệu đọc từ HDFS và chạy HDFS fsck.
7. Chạy helper Windows để dựng lại container, giữ volume và chạy lại Spark test.

## Kết quả

| Kiểm tra | Kết quả |
|---|---|
| Dữ liệu | Yellow Taxi tháng 01/2023, 47.673.370 byte |
| HDFS | `/user/tlc/raw/yellow_tripdata_2023-01.parquet`, RPC cổng 8020 |
| Schema | 19 cột; đã hiển thị 5 dòng mẫu |
| Số dòng | **3.066.766**, giống nhau ở cả hai lượt |
| Thực thi | Log xác nhận task hoàn thành trên `spark-worker` |
| Trạng thái ứng dụng cuối | FINISHED; cả hai lệnh Spark trả exit code 0 |
| Lưu trữ | HDFS HEALTHY, không thiếu/hỏng block; block giữ nguyên sau dựng lại |
| Toàn vẹn | SHA-256 HDFS khớp bản trên máy trước và sau dựng lại |
| Container cuối lượt | Cả 4 running; snapshot không ghi nhận OOMKilled hoặc restart |
| Windows Update | Vẫn Stopped / Disabled |

SHA-256 dữ liệu: `32df6f67578fa86c484a6b5ef23a5281992ff085521082340b0f9e5889e9a572`.

| Lượt Spark | Application ID | Thời gian lệnh spark-submit |
|---|---|---:|
| Lần đầu | `app-20261103122557-0000` | 33,621 giây |
| Sau dựng lại | `app-20261103122926-0000` | 30,514 giây |

Đây là kiểm tra kết nối/đọc dữ liệu, chưa phải benchmark xử lý. Phép count có thể tận dụng metadata Parquet; chưa chứng minh mở rộng hoặc tốc độ làm sạch.

## Điều chỉnh cần thiết trên máy Windows

Docker Desktop từ chối bind mount thư mục dự án với lỗi `user declined directory sharing`. Đã thêm `compose.windows.yml` và `scripts/start_windows.ps1` để copy cấu hình, source, dữ liệu bằng Docker CLI trước khi container chạy. Compose gốc, hai XML, script upload và Spark test không đổi nội dung so với repo nguồn.

Helper đã được chạy kiểm chứng. HDFS vẫn dùng hai named volume, không mất dữ liệu khi dựng lại container. Trong chế độ này, sửa source trên Windows xong cần chép lại vào container theo README; không có cơ chế tự đồng bộ file như bind mount.

## Log và lưu ý khi đọc

Toàn bộ log mới nằm ở [logs/runtime-20261103T120939Z](logs/runtime-20261103T120939Z/):

- [runtime-check.json](logs/runtime-20261103T120939Z/runtime-check.json): tổng hợp kết quả và source hash.
- [spark-test.log](logs/runtime-20261103T120939Z/spark-test.log), [spark-replay.log](logs/runtime-20261103T120939Z/spark-replay.log): schema, mẫu, số dòng và task trên worker.
- [hdfs-upload.log](logs/runtime-20261103T120939Z/hdfs-upload.log): kết quả upload.
- [hdfs-fsck-replay.txt](logs/runtime-20261103T120939Z/hdfs-fsck-replay.txt), [hdfs-sha256-replay.txt](logs/runtime-20261103T120939Z/hdfs-sha256-replay.txt): sức khỏe và toàn vẹn sau dựng lại.
- [windows-helper.log](logs/runtime-20261103T120939Z/windows-helper.log): kiểm chứng helper.
- [spark-master-final.json](logs/runtime-20261103T120939Z/spark-master-final.json), [container-state.txt](logs/runtime-20261103T120939Z/container-state.txt): trạng thái cuối.

Trong cả hai lượt có thông báo `Could not find AppClient` khi Spark đang shutdown, sau khi đã in số dòng. Cả hai lệnh vẫn kết thúc với exit code 0 và ứng dụng cuối là FINISHED. Log được giữ nguyên nội dung để đối chiếu; thông báo này không làm thất bại hai lượt kiểm tra trên.

Thư mục `logs/` đang được Git bỏ qua. Khi cần nộp minh chứng, đóng gói log kèm báo cáo này. Chưa triển khai làm sạch của Phase 2.
