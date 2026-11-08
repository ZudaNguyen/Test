# Phase 2 — Làm sạch theo hướng dẫn BIG Data v2

**ĐẠT, kiểm chứng ngày 08/11/2026.** Căn cứ tài liệu `Hướng-dẫn-đồ-án-BIG-data-v2.pdf`, trang 3–5. Chính sách `tlc-cleaning-v2`, định dạng báo cáo phiên bản 3. Bản này thay phần hướng dẫn Phase 2 trước đây; các báo cáo v1 được giữ làm lịch sử.

## 1. Kết quả dữ liệu thật

| Chỉ tiêu | Kết quả |
|---|---:|
| Dòng đầu vào | 3.066.766 |
| Dòng sạch | 2.993.943 |
| Dòng rejected | 72.823 (2,37%) |
| Dòng sạch có sửa giá trị thành null | 49.797 |
| Dòng sạch có passenger_count null, kể cả null sẵn | 113.936 |
| Số cột sạch | 27: 19 cột nguồn + 3 cột truy vết + 5 cột đặc trưng |
| Bảng tra vùng | 265 dòng |

`3.066.766 = 2.993.943 + 72.823`. Số dòng sửa giá trị nằm trong các tập trên, không cộng thêm vào phương trình. Không dùng số liệu 2.884.463 dòng sạch của chính sách v1 cho bản v2.

## 2. Đối chiếu quy tắc v2

| Yêu cầu | Cách triển khai |
|---|---|
| Giờ trả sau giờ đón, thời lượng tối đa 6 giờ | Thời lượng tính theo microsecond, giữ khoảng `(0,360]` phút. |
| Năm đón 2023 | Giữ pickup trong năm 2023; không lọc thêm theo tháng của tên file. |
| Quãng đường `(0,100]` dặm | Loại thiếu, không hữu hạn, không dương hoặc >100. |
| Tốc độ ≤80 mph | `distance / (duration_minutes / 60)`, dùng `try_divide` an toàn khi mẫu số 0/null. |
| fare_amount >0 | Kiểm tra hữu hạn, dương và vẫn dương sau chuẩn hóa tiền đến cent. |
| passenger_count bằng 0 hoặc >6 | Đưa giá trị về null, giữ dòng nếu các quy tắc loại dòng đều đạt. Null sẵn giữ nguyên. |
| RatecodeID integer, giữ 99 | Cast an toàn thành int; mã 99 không bị loại riêng. |
| Không thiếu khóa vùng đón/trả | Loại nếu một trong hai khóa sau chuẩn hóa là null. Mã không nguyên/không parse được cũng thành null và bị loại để tránh JOIN nhầm. |
| Đặc trưng mới | `trip_duration_min`, `pickup_date`, `pickup_hour`, `pickup_month`, `is_weekend`. |
| Built-in functions | Dùng Spark SQL functions, không Python UDF. |
| Lọc sớm và pushdown | Nhánh tạo tập sạch có một điều kiện lọc tổng hợp trực tiếp trên Parquet gốc, trước sửa giá trị/cache; có execution plan thực tế. |
| Đầu ra Parquet | Có bản trên HDFS và bản xuất về `data/cleaned/`. |

Các quyết định phòng vệ được ghi rõ: hành khách âm/lẻ/NaN/Infinity/không parse được cũng chuyển null thay vì xóa chuyến; RatecodeID không cast được chuyển null có cờ; mã vùng không xác định nhưng khác null (kể cả 264/265) giữ kèm cảnh báo. Tổng tiền và tip âm không phải điều kiện loại của v2, nên giữ kèm cảnh báo. Tiền tùy chọn lỗi chuyển null, không tự điền 0.

Tiền dùng `decimal(18,2)`, làm tròn cent; fare dương quá nhỏ nhưng làm tròn thành 0 không đạt điều kiện cuối. Thời gian giữ `timestamp_ntz` theo giờ nguồn, không tự đổi UTC; thời lượng là chênh lệch giờ nguồn, chưa hiệu chỉnh DST. Không tự xóa dòng trùng vì không có khóa chuyến đi bảo đảm duy nhất.

Đây là quy tắc chọn dữ liệu cho phân tích theo tài liệu, không phải kết luận mọi dòng bị loại đều gian lận. Tham số tháng và thư mục `year/month` chỉ nhận diện lô file đầu vào; `pickup_month` phản ánh tháng thực của từng chuyến và dùng khi GROUP BY.

## 3. Bảng số dòng trước/sau từng bước

| Quy tắc | Trước bước | Loại tại bước | Sau bước |
|---|---:|---:|---:|
| 1. `invalid_pickup_time` | 3.066.766 | 0 | 3.066.766 |
| 2. `invalid_dropoff_time` | 3.066.766 | 0 | 3.066.766 |
| 3. `nonpositive_duration` | 3.066.766 | 1.121 | 3.065.645 |
| 4. `duration_exceeds_6_hours` | 3.065.645 | 2.837 | 3.062.808 |
| 5. `pickup_outside_year_2023` | 3.062.808 | 36 | 3.062.772 |
| 6. `invalid_trip_distance` | 3.062.772 | 44.769 | 3.018.003 |
| 7. `distance_exceeds_100_miles` | 3.018.003 | 88 | 3.017.915 |
| 8. `speed_exceeds_80_mph` | 3.017.915 | 1.546 | 3.016.369 |
| 9. `invalid_fare_amount` | 3.016.369 | 22.426 | 2.993.943 |
| 10. `invalid_pickup_location` | 2.993.943 | 0 | 2.993.943 |
| 11. `invalid_dropoff_location` | 2.993.943 | 0 | 2.993.943 |

Tổng cột loại tại bước là **72.823**. Mỗi dòng chỉ được tính loại ở bước đầu không đạt; tập rejected vẫn lưu đầy đủ mọi lý do. `quality.rejection_counts` là số vi phạm độc lập trên toàn đầu vào, có chồng lặp và không được cộng như bảng tuần tự này.

## 4. Giá trị được sửa, không xóa dòng

| Phép sửa | Dòng nguồn có giá trị này | Dòng sạch được giữ và sửa | Dòng bị loại vì quy tắc khác |
|---|---:|---:|---:|
| `passenger_zero_to_null` | 51.164 | 49.785 | 1.379 |
| `passenger_above_6_to_null` | 20 | 12 | 8 |

Trong tập sạch có **49.797 dòng được sửa số hành khách thành null**, gồm 49.785 dòng ban đầu bằng 0 và 12 dòng >6. Cộng thêm 64.139 dòng có số hành khách null sẵn được giữ, tập sạch có **113.936 passenger_count null**. Các nhóm sửa có thể chồng lặp nói chung; báo cáo đếm riêng số dòng duy nhất có sửa. Cast kiểu thông thường và làm tròn tiền không nằm trong thống kê sửa giá trị này.

Phase 3 đếm chuyến bằng `COUNT(*)`; `COUNT(passenger_count)` chỉ đếm giá trị khác null và sẽ làm thiếu chuyến. Không dùng `dropna()` trên toàn bộ DataFrame để vô tình xóa lại các dòng đã giữ.

## 5. Cách code hoạt động

```text
HDFS Parquet gốc + CSV mã vùng
  ├─ Nhánh audit: gắn toàn bộ lý do/cờ → thống kê → rejected giữ giá trị nguồn
  └─ Nhánh sạch: một filter tổng hợp ngay trên nguồn → chuẩn hóa/sửa null → 5 đặc trưng
      ↓
Ghi Parquet sạch/rejected vào HDFS → đọc lại và kiểm chứng → báo cáo JSON
      ↓
Runner xuất Parquet sạch về data/cleaned, ghi CSV và manifest SHA-256
```

- [src/cleaning_policy_v2.py](src/cleaning_policy_v2.py): điều kiện loại, sửa giá trị, đặc trưng, thống kê tuần tự và chuyển đổi.
- [src/clean_data.py](src/clean_data.py): job Spark Standalone, lưu execution plan, ghi HDFS, đọc lại kiểm chứng và tạo báo cáo.
- [src/test_cleaning_v2.py](src/test_cleaning_v2.py): 8 kiểm thử, 47 dòng có kết quả tính tay: 24 sạch, 23 rejected.
- [src/test_cleaning_rules.py](src/test_cleaning_rules.py): điểm chạy bộ kiểm thử.
- [scripts/run_phase2.ps1](scripts/run_phase2.ps1): chuẩn bị, chạy, xuất CSV và dữ liệu sạch về máy, kiểm tra header/footer Parquet và ghi hash.

Audit cần thấy toàn bộ dòng để đếm trước/sau; nhánh này đọc tất cả dữ liệu và cache trên đĩa. Nhánh sạch đọc nguồn riêng với filter sớm, nên tổng job có thể quét dữ liệu nhiều lần. Chỉ các điều kiện được Parquet hỗ trợ mới push xuống bộ đọc; không phải HDFS tự lọc SQL. Execution plan đã ghi `PushedFilters` thật, gồm giới hạn năm, khoảng cách, fare và điều kiện không null. Không khẳng định mọi điều kiện đều pushdown hay không có áp lực RAM. Thống kê tổng hợp, bảng vùng nhỏ và tối đa 5 dòng mỗi nhóm mẫu được collect; toàn bộ TLC không đưa về Python/Pandas.

## 6. Minh chứng và số đo

- Run: `p2v2-202301-20261108T122833705Z-9fc6e4`; ứng dụng `app-20261108123030-0001` thực thi trên `spark-worker`.
- [Báo cáo JSON](reports/phase2/p2v2-202301-20261108T122833705Z-9fc6e4/quality_report.json), [trạng thái runner](reports/phase2/p2v2-202301-20261108T122833705Z-9fc6e4/runner-status.json).
- [CSV loại dòng](reports/phase2/p2v2-202301-20261108T122833705Z-9fc6e4/rule-counts.csv), [CSV sửa giá trị](reports/phase2/p2v2-202301-20261108T122833705Z-9fc6e4/transformation-counts.csv).
- [Execution plan](reports/phase2/p2v2-202301-20261108T122833705Z-9fc6e4/cleaned-explain.txt), [log Spark](reports/phase2/p2v2-202301-20261108T122833705Z-9fc6e4/spark-cleaning.log), [log 8 kiểm thử](reports/phase2/p2v2-202301-20261108T122833705Z-9fc6e4/synthetic-tests.log).
- [Hash source/input](reports/phase2/p2v2-202301-20261108T122833705Z-9fc6e4/source-input-hashes.json), [manifest Parquet xuất về máy](reports/phase2/p2v2-202301-20261108T122833705Z-9fc6e4/local-output-manifest.json).
- [Đối chiếu hợp đồng v2](reports/phase2/p2v2-202301-20261108T122833705Z-9fc6e4/contract-verification.json).

Tổng thời gian job đến báo cáo **240,493 giây**; profile/cache 62,118, ghi sạch 11,101, ghi rejected 3,614, đọc lại/kiểm chứng 25,381 giây. Tổng còn có khởi tạo, đọc metadata/bảng vùng, lập plan, lấy mẫu và các phần điều phối, nên không bằng tổng 4 số thành phần. Không tính kiểm thử giả lập, upload CSV, xuất file về Windows và dừng Spark. Đây chưa phải benchmark Spark/Pandas hay phép so tốc độ với v1 vì chính sách khác nhau.

Đã kiểm tra bảo toàn số dòng, bảng tuần tự, count/schema sau ghi, điều kiện v2 trên dữ liệu đọc lại, 5 đặc trưng và số hành khách null được giữ. Bản xuất có `_SUCCESS`, 1 file Parquet hợp lệ, kích thước/hash được ghi trong manifest. File gốc HDFS vẫn có SHA-256 `32df6f67578fa86c484a6b5ef23a5281992ff085521082340b0f9e5889e9a572`, đã kiểm tra ngày 08/11.

## 7. Chạy lại và bàn giao

Mở Docker Desktop, khởi động cụm và upload dữ liệu gốc theo README nếu chưa có. Tại thư mục dự án:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase2.ps1
```

Lệnh phải kết thúc với `PHASE 2 PASSED`. Mỗi lần chạy có run_id riêng. [latest.json](reports/phase2/latest.json) và [latest-export.json](reports/phase2/latest-export.json) chỉ trỏ tới lượt thành công mới nhất.

HDFS đầu vào Phase 3 của lượt đã kiểm chứng:

```text
hdfs://namenode:8020/user/tlc/cleaned/year=2023/month=01/run_id=p2v2-202301-20261108T122833705Z-9fc6e4
```

Bản tương ứng trên máy:

```text
data/cleaned/year=2023/month=01/run_id=p2v2-202301-20261108T122833705Z-9fc6e4
```

Chỉ dùng một run, không quét gộp các phiên bản lịch sử. Nếu sửa/chạy lại code thì dùng đường dẫn trong báo cáo của lượt mới. Báo cáo v1 và đợt thử v2 có thêm lọc tháng được giữ làm lịch sử, không dùng làm đầu vào bàn giao v2 cuối này. ZIP ngày 05/11 là bản chính sách cũ.

Phase 2 hoàn thành phần làm sạch/chuyển đổi. KPI, benchmark Pandas/Spark và pipeline đầy đủ của các giai đoạn sau chưa nằm trong kết quả này.

## 8. Ghi nhận AI

Codex hỗ trợ đáng kể code, kiểm thử, kiểm chứng và tài liệu. Nhóm cần tự đọc, rà soát, hiểu và giải thích được từng quy tắc/cách đo; ghi nhận việc dùng AI trong báo cáo học phần.
