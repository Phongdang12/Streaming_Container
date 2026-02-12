# HƯỚNG DẪN CHẠY HỆ THỐNG - CHUẨN DATA ENGINEER (UPDATED)

> **Workflow thực tế**: Infrastructure setup 1 lần (Automated) → Chạy processing jobs từng bước (Manual Control)

---

## 🏗️ KIẾN TRÚC MỚI: AUTOMATED HIVE METASTORE

Hệ thống đã được nâng cấp để **tự động hóa** việc quản lý metadata, giúp Superset luôn nhìn thấy dữ liệu:

1.  **Hive Metastore (HMS)**: Là "trái tim" quản lý metadata. Tất cả tables (Gold) được lưu trữ metadata tại đây.
2.  **Automated Registration**:
    *   Container `trino-init` tự động chạy khi start infrastructure.
    *   Nó chờ Trino, HMS, MinIO sẵn sàng.
    *   Nó **TỰ ĐỘNG REGISTER** 7 bảng Gold vào Trino/HMS.
    *   Container `catalog-sync` chạy ngầm mỗi 60s để phát hiện tables mới.
3.  **Superset**: Kết nối tới Trino qua catalog `delta`. Chỉ cần login là thấy tables.

---

## 🚀 CÁCH CHẠY HỆ THỐNG (CHUẨN NHẤT)

### Bước 1: Khởi động Infrastructure (1 lần duy nhất)

Chạy script này để dựng toàn bộ nền tảng (Kafka, Spark Master, MinIO, Trino, HMS, Superset):

```powershell
.\scripts\start-infra.ps1
```

**Script sẽ:**
*   ✅ Start tất cả services hạ tầng.
*   ✅ **Tự động register** các bảng Gold (bạn không cần gõ lệnh `register_table` thủ công nữa).
*   ✅ Đợi hệ thống Healthy (~2 phút).

> ⚠️ **Lúc này chưa có dữ liệu** vì chưa chạy Producer và Spark Jobs.

---

### Bước 2: Kiểm tra hệ thống sẵn sàng (MỚI)

Trước khi chạy jobs, hãy đảm bảo mọi thứ đã được kết nối đúng bằng script validate.
Vì bạn đang dùng Windows (PowerShell), hãy dùng file `.ps1`:

```powershell
.\scripts\validate.ps1
```

*   Nếu thấy **Counts = 0** nhưng không báo lỗi kết nối: ✅ Hệ thống sẵn sàng nhận data.
*   Nếu báo lỗi kết nối: Chờ thêm 1 chút hoặc kiểm tra logs `trino-init`.

---

### Bước 3: Chạy Producer (Tạo dữ liệu đầu vào)

Mở **Terminal mới**, chạy Producer để đẩy dữ liệu vào Kafka:

```powershell
docker-compose up producer-stream
```

> **Tip:** Để terminal này chạy để thấy logs gửi tin nhắn. Nếu muốn chạy ngầm, thêm `-d`.

---

### Bước 4: Chạy Spark Bronze-Silver (Data Lakehouse Foundation)

Mở **Terminal mới**, chạy job xử lý tầng Bronze và Silver:

```powershell
docker-compose up spark-stream-bronze-silver
```

**Quan sát logs:**
*   Bạn sẽ thấy logs xử lý theo Batch (e.g., `Batch: 5 - Processing 200 records`).
*   Dữ liệu đang được ghi vào MinIO (Bronze/Silver Delta Tables).

---

### Bước 5: Chạy Spark Gold Ops (Business Layer)

Mở **Terminal mới**, chạy job xử lý tầng Gold (Aggregation & State):

```powershell
docker-compose up spark-stream-gold-ops
```

**Quan sát logs:**
*   Job này đọc từ Silver và tính toán KPIs.
*   Logs `MERGE` cho thấy dữ liệu đang được cập nhật vào Gold tables.
*   **Ngay lúc này, dữ liệu sẽ bắt đầu xuất hiện trong Trino/Superset.**

---

### Bước 6: Chạy Spark KPI Batch (Optional - 15 phút/lần)

Mở **Terminal mới**, chạy job tính toán KPI định kỳ:

```powershell
docker-compose up spark-kpi-batch
```

---

## 📉 CHẾ ĐỘ VISUALIZATION ONLY (TIẾT KIỆM TÀI NGUYÊN)

Khi **đã có đủ dữ liệu** và bạn chỉ muốn tập trung vẽ Dashboard trên Superset mà không muốn máy bị lag do chạy Spark/Kafka, hãy chạy lệnh sau:

```powershell
.\scripts\start-viz.ps1
```

Lệnh này sẽ:
1.  🛑 **STOP** toàn bộ các service nặng: Spark Master/Worker, Spark Jobs, Kafka, Producer.
2.  ✅ **KEEP/START** các service nhẹ cần thiết cho Dashboard: MinIO, Trino, Hive Metastore, Postgres, Redis, Superset.

Máy bạn sẽ nhẹ hơn rất nhiều để thao tác trên trình duyệt.

---

## 📊 KIỂM TRA DỮ LIỆU (VERIFY)

### 1. Kiểm tra nhanh bằng Validate Script
```powershell
.\scripts\validate.ps1
```
Lúc này bạn sẽ thấy **Counts > 0**. Điều này chứng tỏ:
1.  Producer đã gửi data.
2.  Spark đã xử lý và ghi xuống MinIO.
3.  Trino đã đọc được data từ MinIO qua Hive Metastore.

### 2. Query trực tiếp trên Trino
```powershell
docker exec -it trino trino
```

```sql
USE delta.lakehouse;

-- Kiểm tra tables đã được register tự động chưa
SHOW TABLES;

-- Xem dữ liệu Gold
SELECT * FROM gold_container_cycle LIMIT 5;
SELECT * FROM gold_container_current_status LIMIT 5;

-- Kiểm tra độ trễ (real-time)
SELECT count(*) FROM gold_container_cycle;
```

---

## 📈 SỬ DỤNG SUPERSET DASHBOARD

1.  Truy cập: **http://localhost:8088**
2.  Login: `admin` / `admin`
3.  Vào **SQL Lab**:
    *   Database: **Trino Delta Lake**
    *   Schema: **lakehouse**
    *   Test query: `SELECT * FROM gold_container_cycle LIMIT 10`
4.  Vào **Dashboards**: Tạo charts từ các bảng Gold này.

> **Lưu ý:** Nếu không thấy bảng trong Dataset, hãy click nút "Sync columns from source" hoặc Refresh trình duyệt.

---

## 🛠️ XỬ LÝ LỖI THƯỜNG GẶP (TROUBLESHOOTING)

### Lỗi 1: Validate script báo "Table not found"
*   **Nguyên nhân**: `trino-init` chưa chạy xong hoặc bị lỗi.
*   **Cách sửa**:
    ```powershell
    docker logs trino-init
    # Nếu thấy lỗi, hãy restart init service:
    docker-compose restart trino-init
    ```

### Lỗi 2: Validate script báo Count = 0 mãi
*   **Nguyên nhân**: Spark job chưa chạy hoặc Producer chưa gửi data.
*   **Cách sửa**: Kiểm tra terminal chạy Spark Bronze/Silver và Gold. Có thấy logs `Batch processing` không?

### Lỗi 3: Superset không kết nối được Trino
*   **Nguyên nhân**: Superset khởi động trước khi Trino sẵn sàng.
*   **Cách sửa**:
    ```powershell
    docker-compose restart superset
    ```

### Lỗi 4: Muốn reset toàn bộ làm lại từ đầu?
```powershell
# Xóa sạch containers và volumes (MẤT HẾT DATA)
docker-compose down -v

# Start lại
.\scripts\start-infra.ps1
```
