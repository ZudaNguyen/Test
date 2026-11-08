> Historical policy v1 result, preserved for comparison; current guide-v2 results are in the project-root PHASE2_RESULT.md.

# Phase 2 — Làm sạch dữ liệu TLC

Đã chạy và kiểm chứng ngày **03/11/2026**, cập nhật và chạy lại ngày **04/11/2026** trên Spark 4.1.4, Hadoop HDFS của Phase 1. Bản cập nhật có đúng cột `trip_duration_min` và bảng số dòng trước/sau từng quy tắc. Các lượt chạy dữ liệu thật thành công, có executor thực thi trên `spark-worker`.

## 1. Kết quả thực tế

| Dữ liệu | Số dòng |
|---|---:|
| Đầu vào Yellow Taxi tháng 01/2023 | 3.066.766 |
| Đủ điều kiện đưa sang phân tích | 2.884.463 |
| Tách sang tập rejected | 182.303 (5,94%) |
| Bảng tra vùng taxi | 265 |

`3.066.766 = 2.884.463 + 182.303`. Không xóa hay sửa file gốc. Hash SHA-256 của file gốc trong HDFS được kiểm chứng ngày 03/11 là `32df6f67578fa86c484a6b5ef23a5281992ff085521082340b0f9e5889e9a572`; hash input cục bộ của lượt cập nhật ngày 04/11 vẫn khớp.

## 2. Cách vận hành và vai trò của code

```text
HDFS raw Parquet + CSV tra vùng
          ↓ Spark đọc bằng DataFrame
Kiểm tra từng dòng và gắn lý do/cảnh báo
          ├── Không có lý do loại → chuẩn hóa → cleaned Parquet
          └── Có lý do loại → giữ giá trị gốc + lý do → rejected Parquet
          ↓
Đọc lại đầu ra, kiểm tra số dòng/schema/quy tắc → báo cáo JSON + log
```

- [scripts/run_phase2.ps1](../../../scripts/run_phase2.ps1): kiểm tra cụm, ghi hash source/input, chép source vào Spark, chạy kiểm thử nhỏ, đưa bảng tra vùng vào HDFS, gọi `spark-submit` và xuất báo cáo về máy.
- [src/clean_data.py](../../../src/clean_data.py): xử lý dữ liệu thật. `mark_rows()` đánh dấu lỗi/cảnh báo; `profile_rows()` đếm; `clean_projection()` chuẩn hóa tập hợp lệ; `main()` ghi và kiểm tra lại đầu ra.
- [src/test_cleaning_rules.py](../../../src/test_cleaning_rules.py): 25 dòng giả lập với kết quả dự kiến, dùng để kiểm tra quy tắc, không trộn vào dữ liệu TLC.

Spark Master điều phối, Spark Worker thực thi, HDFS lưu dữ liệu. Python điều khiển các phép toán Spark SQL; không tải toàn bộ dữ liệu về Python/Pandas. Chỉ thu thống kê tổng hợp, bảng mã vùng nhỏ và tối đa 5 dòng mẫu mỗi tập về driver. Dữ liệu trung gian cache bằng `DISK_ONLY` để giảm áp lực RAM. Job dùng tối đa 2 lõi executor, executor 1 GiB và driver 768 MiB; đây là cấu hình job, chưa tính tài nguyên các dịch vụ Hadoop/Spark.

## 3. Quy tắc đã áp dụng

| Lý do loại | Số dòng có lý do này |
|---|---:|
| Thiếu/không đọc được giờ đón | 0 |
| Thiếu/không đọc được giờ trả | 0 |
| Giờ trả không sau giờ đón | 1.121 |
| Giờ đón ngoài tháng 01/2023 | 48 |
| Số hành khách thiếu/không hợp lệ | 122.907 |
| Quãng đường không hợp lệ hoặc ≤ 0 | 45.862 |
| Giá cước thiếu/không hợp lệ/âm | 25.049 |
| Tổng tiền thiếu/không hợp lệ/âm | 25.204 |
| Tiền tip có ghi nhận nhưng không hợp lệ/âm | 225 |

Một dòng có thể vi phạm nhiều quy tắc, nên **không cộng bảng này để tính số dòng bị loại**. Trong dữ liệu gốc, `passenger_count` có 71.743 giá trị null; đây là một phần trong 122.907 dòng không đạt quy tắc hành khách.

Từ báo cáo phiên bản 2, `quality.sequential_rule_counts` bổ sung bảng **trước bước → bị loại tại bước → sau bước** theo thứ tự cố định: giờ đón, giờ trả, thời lượng, tháng, hành khách, quãng đường, giá cước, tổng tiền, tip. Một dòng chỉ đóng góp vào số bị loại ở quy tắc đầu tiên không đạt; tập rejected vẫn giữ đầy đủ mọi lý do. Vì vậy bảng tuần tự có thể cộng số bị loại, còn bảng vi phạm độc lập phía trên thì không. Mỗi lượt chạy xuất thêm `rule-counts.csv` để mở bằng Excel hoặc gửi nhóm trưởng.

Thống kê tuần tự từ lượt chạy cập nhật ngày 04/11/2026:

| Bước kiểm tra | Trước bước | Bị loại tại bước | Sau bước |
|---|---:|---:|---:|
| Giờ đón hợp lệ | 3.066.766 | 0 | 3.066.766 |
| Giờ trả hợp lệ | 3.066.766 | 0 | 3.066.766 |
| Thời lượng > 0 | 3.066.766 | 1.121 | 3.065.645 |
| Giờ đón trong tháng | 3.065.645 | 48 | 3.065.597 |
| Số hành khách hợp lệ | 3.065.597 | 122.792 | 2.942.805 |
| Quãng đường hợp lệ | 2.942.805 | 36.240 | 2.906.565 |
| Giá cước hợp lệ | 2.906.565 | 21.989 | 2.884.576 |
| Tổng tiền hợp lệ | 2.884.576 | 113 | 2.884.463 |
| Tip hợp lệ nếu có ghi nhận | 2.884.463 | 0 | 2.884.463 |

Tổng cột **bị loại tại bước = 182.303**. Bước tip không loại thêm dòng nào vì các dòng tip không hợp lệ đã bị loại ở bước trước; điều này không có nghĩa đầu vào không có lỗi tip.

Đây là **chính sách chọn dữ liệu cho phân tích**, không phải kết luận mọi dòng rejected đều sai. Tiền âm có thể liên quan điều chỉnh giao dịch; dòng thiếu số hành khách vẫn có thể hữu ích cho mục tiêu khác. Khi báo cáo phân tích cần nêu rõ phạm vi đã lọc và khả năng ảnh hưởng kết quả.

Các điểm cần nhớ:

- Hành khách phải là số nguyên dương trong phạm vi int32; khoảng cách phải hữu hạn và > 0. Không tự đặt trần khoảng cách/thời lượng.
- Giá cước/tổng tiền bằng 0 được giữ. Tip thiếu giữ null và gắn cảnh báo, không tự thay bằng 0.
- Mã vùng không xác định hoặc mã thanh toán lạ chỉ gắn cảnh báo. Mã vùng 264/265 cũng được đánh dấu không xác định. Số cảnh báo vùng đón/trả trên **toàn bộ đầu vào** là 41.763 / 33.549; có thể chồng lặp hoặc nằm trong tập rejected.
- Khoản phụ thu không chuẩn hóa được chuyển thành null kèm cảnh báo; phụ thu âm không tự động làm loại dòng.
- Không xóa dòng giống nhau vì đầu vào không có khóa chuyến đi bảo đảm duy nhất.
- Tiền chuyển sang `decimal(18,2)`, làm tròn đến cent. Thời gian giữ `timestamp_ntz` theo giờ ghi nhận ban đầu; không tự chuyển UTC.
- Tập sạch có 25 cột: 19 cột nguồn, `source_file`, `warning_flags` và 4 cột mới: `pickup_date`, `pickup_hour`, `is_weekend`, `trip_duration_min`. Tên cột thời lượng được cập nhật theo yêu cầu bàn giao ngày 04/11/2026; các lượt chạy cũ vẫn giữ schema cũ để đối chiếu.

Ví dụ dòng giữ lại: đón `2023-01-01 00:32:10`, trả `00:40:36`, 1 hành khách, 0,97 mile, giá cước 9,30. Đầu ra thêm ngày `2023-01-01`, giờ `0`, cuối tuần `true`, thời lượng khoảng `8,4333` phút. Dòng có `passenger_count = 0` được chuyển sang rejected với `invalid_passenger_count`; các giá trị nguồn được giữ lại để đối chiếu.

## 4. Minh chứng và số đo

Lượt cập nhật ngày **04/11/2026**: `p2-202301-20261104T053321000Z-69e307`, ứng dụng `app-20261104053438-0000` ở trạng thái `FINISHED`. Tổng thời gian job đến báo cáo **91,287 giây**: profile/cache 36,508 giây, ghi sạch 11,420 giây, ghi rejected 2,956 giây, đọc lại/kiểm tra 11,240 giây; tốc độ tính theo tổng thời gian khoảng 33.595 dòng/giây. Các số đo có cùng phạm vi như mô tả bên dưới.

Đã xác nhận đủ 4 cột yêu cầu, giữ đủ 19 cột nguồn, CSV khớp JSON, số dòng trước/sau từng bước nối tiếp đúng và tổng bị loại khớp đầu ra. Số dòng, số vi phạm độc lập và cảnh báo không đổi so với bản ngày 03/11; chính sách lọc vẫn là `tlc-cleaning-v1`, định dạng báo cáo được nâng lên phiên bản 2. Không ghi đè các lượt chạy cũ.

Hai lượt chạy ban đầu ngày 03/11/2026 (schema thời lượng cũ):

| Lượt | Run ID | Thời gian job đến khi tạo báo cáo |
|---|---|---:|
| 1 | `p2-202301-20261103T142137649Z-f107a7` | 122,575 giây |
| 2 | `p2-202301-20261103T143016815Z-e71064` | 98,091 giây |

Số đo lượt 2: profile/cache 36,080 giây; ghi sạch 11,915 giây; ghi rejected 4,490 giây; đọc lại/kiểm tra 12,797 giây. Tổng thời gian còn có khởi tạo Spark, đọc bảng tra, lấy mẫu và các phần điều phối; không bằng tổng 4 mục này. Tốc độ theo tổng thời gian là khoảng 31.264 dòng đầu vào/giây.

Đây là số đo hai lần chạy trên máy hiện tại, **chưa phải benchmark so sánh công nghệ hoặc mở rộng quy mô**. Tổng không tính thời gian kiểm thử giả lập, upload CSV, chép báo cáo ra Windows và dừng Spark; chênh lệch hai lượt không chứng minh mức tăng tốc ổn định.

Đã kiểm tra:

- Bộ kiểm thử cập nhật ngày 04/11/2026 có 6 kiểm thử trên 25 dòng giả lập đều đạt: dự kiến 7 dòng sạch, 18 dòng rejected; bao gồm null, NaN/Infinity, số âm, số hành khách lẻ, tràn số, ranh giới tháng, tiền làm tròn, dòng giống nhau, lỗi chồng lặp và số dòng trước/sau từng quy tắc. Bản đầu ngày 03/11 có 5 kiểm thử.
- Đọc lại toàn bộ hai đầu ra, số dòng khớp và bảo toàn đầu vào.
- Schema tập sạch đúng dự kiến; mọi dòng sạch đạt các điều kiện loại; mọi dòng rejected có lý do.
- Hai lượt cho cùng thống kê chất lượng, schema, chính sách và kết quả kiểm tra. Không khẳng định các file Parquet giống từng byte hoặc thứ tự dòng cố định.
- Hai ứng dụng Spark ở trạng thái `FINISHED`; sau chạy vẫn có 1 worker và 1 DataNode hoạt động.

Mở các file sau để đối đáp/demo:

- [Báo cáo lần thành công mới nhất](../../../reports/phase2/latest.json): số dòng, schema, ví dụ input/output, thời gian và đường dẫn HDFS.
- [Bảng trước/sau từng quy tắc ngày 04/11](../../../reports/phase2/p2-202301-20261104T053321000Z-69e307/rule-counts.csv): có thể mở bằng Excel để đối chiếu.
- [Kiểm chứng bản cập nhật](../../../reports/phase2/p2-202301-20261104T053321000Z-69e307/contract-verification.json): tên cột, số dòng, CSV, hash source/input và trạng thái Spark.
- [Log xử lý ngày 04/11](../../../reports/phase2/p2-202301-20261104T053321000Z-69e307/spark-cleaning.log).
- [Log 6 kiểm thử ngày 04/11](../../../reports/phase2/p2-202301-20261104T053321000Z-69e307/synthetic-tests.log).
- [Đối chiếu hai lượt chạy ban đầu](../../../reports/phase2/reproducibility.json): các kiểm tra trùng khớp và hash dữ liệu gốc tại thời điểm kiểm chứng ngày 03/11; không đại diện cho hash source sau cập nhật ngày 04/11.
- [Log xử lý lượt 2](../../../reports/phase2/p2-202301-20261103T143016815Z-e71064/spark-cleaning.log).
- [Log kiểm thử giả lập](../../../reports/phase2/p2-202301-20261103T142137649Z-f107a7/synthetic-tests.log).
- [Hash source/input lượt 2](../../../reports/phase2/p2-202301-20261103T143016815Z-e71064/source-input-hashes.json).

## 5. Chạy lại và bàn giao sang phân tích

Mở Docker Desktop, mở PowerShell tại thư mục dự án. Nếu cụm đang dừng, khởi động theo README. Sau đó:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase2.ps1
```

Thành công sẽ in `PHASE 2 PASSED` cùng số dòng, đường dẫn báo cáo và tập sạch. Script dừng khi lỗi và lưu log; mỗi lượt có thư mục riêng, không ghi đè đầu ra cũ. Báo cáo mới nhất ở `reports/phase2/latest.json` chỉ cập nhật sau khi thành công.

Đầu vào cho Phase 3 hiện tại lấy từ `outputs.cleaned` của báo cáo:

```text
hdfs://namenode:8020/user/tlc/cleaned/year=2023/month=01/run_id=p2-202301-20261104T053321000Z-69e307
```

Chỉ đọc **một run cụ thể**. Nếu quét tất cả `run_id`, các lần chạy sẽ bị tính lặp. Tập rejected ở đường dẫn tương ứng dưới `/user/tlc/rejected/`; báo cáo cũng có bản HDFS dưới `/user/tlc/quality/`.

Phase 2 hiện hoàn thành bước làm sạch và tạo dữ liệu cho phân tích. Chưa thực hiện KPI/biểu đồ của bước phân tích, mô hình dự báo hay benchmark nhiều kích thước dữ liệu.

## 6. Ghi nhận hỗ trợ AI

AI (Codex) hỗ trợ đáng kể việc viết code Phase 2, kiểm thử, chạy kiểm chứng và soạn tài liệu. Nhóm cần đọc, rà soát và giải thích được chính sách lọc, luồng xử lý, kết quả trước khi nộp; đưa ghi nhận này vào báo cáo theo yêu cầu học phần.
