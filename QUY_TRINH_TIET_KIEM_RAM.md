# 📉 QUY TRÌNH CHẠY TIẾT KIỆM TÀI NGUYÊN (LOCAL MODE)

> **Mục tiêu**: Chạy được hệ thống Big Data phức tạp trên máy tính cá nhân (RAM 8GB-16GB) mà không bị treo máy hay crash Docker.
> **Nguyên tắc**: Chạy tuần tự (Sequential) thay vì chạy song song (Parallel).

hãy mở Spark UI: http://localhost:8080 để xem số lượng Cores/Memory đang được sử dụng. dụng/Core/Core/Memory đang được sử dụng.

docker logs -f spark-kpi-batch
docker-compose restart spark-kpi-batch

# Stop service hiện tại
docker-compose stop spark-stream-bronze-silver

# Start lại với config mới
docker-compose up -d spark-stream-bronze-silver

# Xem logs để kiểm tra
docker logs -f spark-stream-bronze-silver


---

## 🛑 BƯỚC 0: CLEAN UP (QUAN TRỌNG)

Trước khi bắt đầu, hãy đảm bảo Docker sạch sẽ để tránh xung đột tài nguyên cũ.

1.  **Restart Docker Desktop**: Chuột phải vào icon Docker -> Quit, sau đó mở lại.
2.  **Xóa containers cũ**:
    ```powershell
    docker-compose down
    ```

---

## 🏗️ BƯỚC 1: KHỞI ĐỘNG HẠ TẦNG (INFRASTRUCTURE)

Chạy các service nền tảng (Kafka, MinIO, Trino, HMS, Superset).

```powershell
.\scripts\start-infra.ps1
```

*   **Chờ đợi**: Khoảng 2-3 phút cho đến khi script báo "INFRASTRUCTURE STATUS".
*   **Kiểm tra**: Chạy `.\scripts\validate.ps1` để đảm bảo Trino đã sẵn sàng (dù chưa có bảng).

---

## 🏭 BƯỚC 2: SINH DỮ LIỆU & XỬ LÝ TẦNG ĐẦU (BRONZE/SILVER)

Thay vì chạy tất cả, ta chỉ chạy Producer và Spark Job xử lý dữ liệu thô.

1.  **Chạy Producer** (Gửi dữ liệu vào Kafka):
    ```powershell
    docker-compose up -d producer-stream
    docker-compose up producer-stream

    ```

2.  **Chạy Spark Bronze-Silver** (Đọc Kafka -> Ghi xuống MinIO):
    ```powershell
    docker-compose up -d spark-stream-bronze-silver
    docker-compose up spark-stream-bronze-silver

    ```

3.  **⏳ CHỜ ĐỢI (~5-10 phút)**:
    *   Để hệ thống chạy khoảng 5-10 phút để tích lũy đủ dữ liệu (khoảng vài chục ngàn dòng).
    *   Bạn có thể kiểm tra logs: `docker logs -f spark-stream-bronze-silver` để thấy nó đang xử lý từng batch.

4.  **🛑 STOP BƯỚC 2**:
    *   Sau khi đã có đủ dữ liệu, hãy **TẮT** chúng để giải phóng RAM cho bước sau.
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

2.  **⏳ CHỜ ĐỢI (~5-10 phút)**:
    *   Job này sẽ đọc dữ liệu Silver đã tạo ở Bước 2 và tổng hợp lại.
    *   Lúc này dữ liệu sẽ bắt đầu xuất hiện trong Trino/Superset.

3.  **Chạy Spark KPI Batch** (Tùy chọn):
    ```powershell
    docker-compose up -d spark-kpi-batch
        docker-compose up  spark-kpi-batch

    ```

4.  **🛑 STOP BƯỚC 3**:
    *   Sau khi job chạy ổn định và logs báo đã xử lý xong các batch lớn, hãy **TẮT** luôn.
    ```powershell
    docker-compose stop spark-stream-gold-ops spark-kpi-batch
    ```

---

## 📊 BƯỚC 4: CHẾ ĐỘ VISUALIZATION (NHẸ NHÀNG)

Lúc này dữ liệu đã nằm an toàn trong MinIO (ổ cứng). Ta không cần Spark hay Kafka nữa.

1.  **Chuyển sang chế độ Viz**:
    ```powershell
    .\scripts\start-viz.ps1
    ```
    *   Lệnh này sẽ đảm bảo Spark/Kafka đã tắt hẳn và chỉ giữ lại Trino/Superset.

2.  **Tận hưởng**:
    *   Truy cập Superset: **http://localhost:8088** (admin / admin).
    *   Vẽ biểu đồ, query thoải mái mà máy tính vẫn mượt mà.

---

## 📊 KIỂM TRA TIẾN ĐỘ XỬ LÝ

Để biết khi nào Gold đã xử lý hết dữ liệu từ Silver và Bronze:

### Cách 1: Sử dụng Script Monitoring (Khuyến nghị)

```powershell
.\scripts\check-progress.ps1
```

Script này sẽ hiển thị:
- **Số lượng records** trong mỗi bảng (Bronze, Silver, Gold)
- **Latest timestamp** của dữ liệu đã xử lý
- **Trạng thái checkpoint** cho các streaming queries
- **So sánh timestamp** giữa Silver và Gold để biết Gold đã xử lý đến đâu
- **Trạng thái streaming queries** (nếu đang chạy)
- **Tỷ lệ xử lý** giữa các layers
- **🎯 Gold Status: Caught Up Check** - Phân tích chi tiết trạng thái Gold

**Các thông báo quan trọng:**

**Trạng thái Gold:**
- ✅ **"Gold đã xử lý xong và đang chờ dữ liệu mới!"**: Gold đã catch up hoàn toàn với Silver và đang idle, chờ dữ liệu mới từ Bronze/Silver
  - Chênh lệch timestamp < 2 phút
  - Gold streams đang idle (không có dữ liệu mới để xử lý)
- 🔄 **"Gold đang xử lý dữ liệu..."**: Gold đang trong quá trình xử lý batch hiện tại
- ⚠️ **"Gold chưa xử lý hết dữ liệu từ Silver"**: Gold đang chậm hơn Silver, cần chờ thêm

**Trạng thái khác:**
- ✅ **"Gold is up-to-date with Silver"**: Gold đã xử lý hết dữ liệu từ Silver (chênh lệch < 1 phút)
- ⚠️ **"Gold is X minutes/hours behind Silver"**: Gold đang chậm hơn Silver, cần chờ thêm
- ⚠️ **"Path exists but not a Delta table"**: Folder đã tồn tại nhưng chưa được khởi tạo như Delta table
- ❌ **"Does not exist"**: Bảng chưa được tạo

### Cách 2: Kiểm tra Streaming Query Progress

Nếu các streaming jobs đang chạy, bạn có thể check:

```powershell
# Xem logs của Gold streaming
docker logs spark-stream-gold-ops --tail 50

# Hoặc check Spark UI
# Mở browser: http://localhost:8080
# Xem tab "Streaming" để thấy progress của các queries
```

### Cách 3: Kiểm tra Checkpoint trong MinIO

Checkpoint lưu trữ tiến độ xử lý của streaming queries. Bạn có thể kiểm tra trong MinIO:
- Bucket: `checkpoints`
- Các folder checkpoint:
  - `bronze_*`: Checkpoint cho Bronze layer
  - `silver_*`: Checkpoint cho Silver layer  
  - `gold_*`: Checkpoint cho Gold layer

Nếu checkpoint tồn tại, nghĩa là streaming query đã từng chạy và có thể resume từ điểm đó.

**Dấu hiệu Gold đã xử lý hết và đang chờ dữ liệu mới:**

1. **Trong script `check-progress.ps1`:**
   - ✅ Thấy message: **"Gold đã xử lý xong và đang chờ dữ liệu mới!"**
   - ✅ Chênh lệch timestamp giữa Silver và Gold < 2 phút
   - ✅ Gold streams hiển thị status: **"IDLE - Đang chờ dữ liệu mới"**
   - ✅ Last Batch Input Rows = 0 (không có dữ liệu mới trong batch cuối)

2. **Trong logs:**
   - ✅ Không còn warning "Current batch is falling behind"
   - ✅ Batch ID tăng đều đặn nhưng không có dữ liệu mới được xử lý
   - ✅ Logs hiển thị "Batch X: No events to process" hoặc "Batch X: No cycles generated"

3. **Trong Spark UI (http://localhost:8080):**
   - ✅ Tab "Streaming" hiển thị queries với Input Rate = 0
   - ✅ Processed Rate = 0 hoặc rất thấp
   - ✅ Status = "ACTIVE" nhưng không có activity

4. **Trong MinIO:**
   - ✅ Checkpoint status hiển thị ✓ (checkpoint đã được tạo)
   - ✅ Latest timestamp trong Gold tables gần với Silver tables

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
.\scripts\start-viz.ps1
```
