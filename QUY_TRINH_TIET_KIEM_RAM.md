# 📉 QUY TRÌNH CHẠY TIẾT KIỆM TÀI NGUYÊN (LOCAL MODE)



## 🛠️ CÔNG CỤ HỖ TRỢ (Dọn dẹp & Sửa lỗi)

Nếu gặp lỗi full ổ cứng hoặc muốn reset toàn bộ:

```powershell
# Fix lỗi Docker hết chỗ (Prune system)
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force; .\fix_docker_space.ps1

# Xóa sạch dữ liệu và containers cũ (Reset all)
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force; Set-Location "C:\Users\Admin\OneDrive\Documents\Streaming_Container"; .\cleanup_all.ps1
```

## 📊 CÀI ĐẶT DASHBOARD TỰ ĐỘNG

Nếu dashboard chưa hiện trên Superset, chạy lệnh sau:

```powershell
# Cài thư viện python requests (dùng python -m để tránh lỗi path)
python -m pip install requests

# Chạy script deploy dashboard
python scripts/deploy_superset_dashboard.py
```



## 🏗️ BƯỚC 1: KHỞI ĐỘNG HẠ TẦNG (INFRASTRUCTURE)

Chạy các service nền tảng (Kafka, MinIO, Trino, HMS, Superset).

```powershell
.\scripts\start-infra.ps1
```

*   **Chờ đợi**: Khoảng 2-3 phút cho đến khi script báo "INFRASTRUCTURE STATUS".
*   **Kiểm tra**: Mở Spark UI: **http://localhost:8080** và Superset: **http://localhost:8088** để xác nhận dịch vụ đã sẵn sàng.

---


docker-compose run --rm producer-stream python producer.py `
  --mode loop --reset-sim `
  --interval 0 `
  --inter-event-delay 0 `
  --sim-data-start 2025-12-01
# ✅ Xong trong ~3 phút


## 🏭 BƯỚC 2: SINH DỮ LIỆU & XỬ LÝ TẦNG ĐẦU (BRONZE/SILVER)

Thay vì chạy tất cả, ta chỉ chạy Producer và Spark Job xử lý dữ liệu thô.

1.  **Chạy Producer** (Gửi dữ liệu vào Kafka):
    ```powershell
    docker-compose up -d producer-stream
    docker-compose up producer-stream

    ```

2.  **Chạy Spark Bronze-Silver-Canonical** (Đọc Kafka -> Ghi Silver -> Ghi Canonical):
    ```powershell
    docker-compose up -d spark-stream-bronze-silver
    docker-compose logs -f spark-stream-bronze-silver

    ```

3.  **⏳ CHỜ ĐỢI (~5-10 phút)**:
    *   Để hệ thống chạy khoảng 5-10 phút để tích lũy đủ dữ liệu (khoảng vài chục ngàn dòng).
    *   Kiểm tra logs Bronze-Silver: chờ thấy các batch ghi dữ liệu Silver và thấy "Starting canonical Silver stream".
    *   Canonical (`silver_container_events`) được cập nhật realtime trong cùng log.

4.  **🛑 STOP BƯỚC 2**:
    *   Sau khi đã có đủ dữ liệu, **TẮT** chúng để giải phóng RAM.
    ```powershell
    docker-compose stop producer-stream spark-stream-bronze-silver
    ```

---

## 🥇 BƯỚC 3: XỬ LÝ TẦNG CUỐI (GOLD)

Bây giờ RAM đã trống, ta chạy job nặng nhất để tính toán KPI và tạo bảng Gold.

1.  **Chạy Spark Gold Ops**:
    ```powershell
    docker-compose up -d spark-stream-gold-ops
    docker-compose up spark-stream-gold-ops

    ```

3.  **Chạy Spark KPI Batch** (Tùy chọn):
    ```powershell
    docker-compose up -d spark-kpi-batch
    docker-compose up spark-kpi-batch

    ```

4.  **🛑 STOP BƯỚC 3**:
    *   Sau khi job chạy ổn định và logs báo đã xử lý xong các batch lớn, hãy **TẮT** luôn.
    ```powershell
        docker-compose stop spark-stream-gold-ops spark-kpi-batch
``    ```

---

## 📊 BƯỚC 4: CHẾ ĐỘ VISUALIZATION (NHẸ NHÀNG)

Lúc này dữ liệu đã nằm an toàn trong MinIO (ổ cứng). Ta không cần Spark hay Kafka nữa.

1.  **Tắt Kafka và Spark để giải phóng thêm RAM** (Trino + Superset vẫn chạy):
    ```powershell
    docker-compose stop kafka kafka-ui spark-master spark-worker spark-submit
    ```

2.  **Tận hưởng**:
    *   Truy cập Superset: **http://localhost:8088** (admin / admin).
    *   Vẽ biểu đồ, query thoải mái mà máy tính vẫn mượt mà.


---

```powershell
# 1. Start nền tảng
.\scripts\start-infra.ps1

# 2. Sinh & Xử lý thô (Chạy 5-10p rồi tắt)
docker-compose up -d producer-stream spark-stream-bronze-silver
# ... (Đi uống cafe 5p) ...
docker-compose stop producer-stream spark-stream-bronze-silver

# 3. Xử lý tinh (Chạy 5-10p rồi tắt)
docker-compose up -d spark-stream-gold-ops
# ... (Chờ xử lý xong) ...
docker-compose stop spark-stream-gold-ops

# 4. Xem báo cáo (Nhẹ máy)
docker-compose stop kafka kafka-ui spark-master spark-worker spark-submit
```
