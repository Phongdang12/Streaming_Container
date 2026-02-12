# 📊 HƯỚNG DẪN TẠO DASHBOARD SUPERSET - CONTAINER OPERATIONS (CHUẨN 2026)

## 🎯 MỤC TIÊU
Xây dựng một **Control Tower Dashboard** toàn diện dựa trên 7 bảng dữ liệu chất lượng đã được làm sạch.
Dashboard chia làm 3 Tabs phục vụ 3 mục đích khác nhau:
1.  **Real-time Operations**: Giám sát hiện trường, cảnh báo kẹt cảng.
2.  **Productivity Analysis**: Phân tích năng suất theo Ca (Shift) và Ngày (Daily).
3.  **Optimization**: Phân tích giờ cao điểm và lập kế hoạch.

---

## 🛠️ BƯỚC 1: KIỂM TRA KẾT NỐI DỮ LIỆU

Trước khi vẽ, hãy đảm bảo Trino đã nhận diện đủ **7 bảng vàng**:
1. Login Superset: `http://localhost:8088` (admin/admin).
2. Vào **SQL Lab**, chọn Database **Trino Delta Lake**, Schema **lakehouse**.
3. Kiểm tra danh sách bảng bên trái:
   - `gold_backlog_metrics`
   - `gold_container_current_status`
   - `gold_container_cycle`
   - `gold_kpi_daily`
   - `gold_kpi_peak_hours` (New)
   - `gold_kpi_shift` (New)
   - `gold_ops_metrics_realtime`

---

## 🚀 TAB 1: REAL-TIME OPERATIONS (GIÁM SÁT THỜI GIAN THỰC)

Dành cho Điều độ viên (Dispatchers) để xử lý tắc nghẽn ngay lập tức.

### Chart 1.1: Current Inventory by Danger Level (Báo động hàng tồn)
*   **Dataset**: `gold_ops_metrics_realtime`
*   **Chart Type**: Bar Chart (Stacked)
*   **Settings**:
    *   **X-Axis**: `facility`
    *   **Metrics**: `SUM(container_count)`
    *   **Breakdowns** (Phân nhóm): `dwell_bucket`
*   **Ý nghĩa**: Nhìn nhanh xem Cảng nào đang có nhiều `CRITICAL_GT20D` (Màu đỏ) để ưu tiên giải phóng.

### Chart 1.2: Operational Backlog (Công việc tồn đọng)
*   **Dataset**: `gold_backlog_metrics`
*   **Chart Type**: Bar Chart (Horizontal)
*   **Settings**:
    *   **Y-Axis**: `backlog_type` (WAITING_REPAIR, WAITING_CLEANING...)
    *   **Metrics**: `SUM(backlog_count)`
    *   **Breakdowns**: `facility`
*   **Ý nghĩa**: Biết được bộ phận nào (Sửa chữa hay Vệ sinh) đang bị dồn ứ hồ sơ.

### Chart 1.3: Critical Containers List (Danh sách cont cần xử lý gấp)
*   **Dataset**: `gold_ops_metrics_realtime` (Lọc lấy nhóm Critical) hoặc `gold_container_cycle` (CT chi tiết)
*   **Khuyên dùng**: `gold_container_cycle`
*   **Chart Type**: Table
*   **Settings**:
    *   **Columns**: `container_no_norm`, `facility`, `dwell_time_hours`, `gate_in_time`
    *   **Filters**: `cycle_status = 'OPEN'` AND `dwell_time_hours > 240` (10 ngày)
    *   **Sort**: `dwell_time_hours` DESC
*   **Ý nghĩa**: Danh sách cụ thể để in ra và đi tìm container xử lý.

### Chart 1.4: Real-time Event Stream (Hoạt động mới nhất)
*   **Dataset**: `gold_container_current_status`
*   **Chart Type**: Table
*   **Settings**:
    *   **Columns**: `event_time_parsed`, `container_no_norm`, `event_type_norm`, `facility`, `last_location`
    *   **Sort**: `event_time_parsed` DESC
    *   **Limit**: 50
*   **Ý nghĩa**: Bảng tin ticker chạy các sự kiện đang diễn ra tại cổng.

---

## 📈 TAB 2: PRODUCTIVITY (PHÂN TÍCH HIỆU SUẤT)

Dành cho Quản lý Cảng để xem xét năng suất làm việc theo Ca và Xu hướng ngày.

### Chart 2.1: Shift Productivity Comparison (Năng suất theo Ca)
*   **Dataset**: `gold_kpi_shift`
*   **Chart Type**: Bar Chart
*   **Settings**:
    *   **X-Axis**: `operational_date`
    *   **Metrics**: `SUM(value)`
    *   **Breakdowns**: `shift_id` (MORNING, AFTERNOON, NIGHT)
    *   **Filters**: `kpi_type` IN ('SHIFT_GATE_IN', 'SHIFT_YARD_MOVES')
*   **Ý nghĩa**: So sánh năng suất giữa ca Sáng/Chiều/Đêm. Nếu ca Đêm quá thấp -> Cần điều chỉnh nhân sự.

### Chart 2.2: Daily Throughput Trend (Xu hướng thông qua - 60 ngày)
*   **Dataset**: `gold_kpi_daily`
*   **Chart Type**: Line Chart (Time Series)
*   **Settings**:
    *   **X-Axis**: `day_ts`
    *   **Metrics**: `SUM(value)`
    *   **Filters**: `kpi_type` = 'DAILY_THROUGHPUT'
*   **Ý nghĩa**: Theo dõi sản lượng tổng thể của cảng đang tăng hay giảm.

### Chart 2.3: Avg Dwell Time Trend (Xu hướng thời gian lưu bãi)
*   **Dataset**: `gold_kpi_daily`
*   **Chart Type**: Dual Line Chart (Kết hợp)
*   **Settings**:
    *   **X-Axis**: `day_ts`
    *   **Left Axis**: `AVG(metric1)` (Avg Dwell Hours)
    *   **Right Axis**: `AVG(metric3)` (P95 Dwell Hours - Những cont nằm lâu nhất)
    *   **Filters**: `kpi_type` = 'DAILY_THROUGHPUT'
*   **Ý nghĩa**: Nếu đường P95 nới rộng khoảng cách với Avg -> Có một nhóm nhỏ container bị kẹt rất lâu.

---

## 🔥 TAB 3: OPTIMIZATION (TỐI ƯU HÓA & HEATMAP)

Dành cho Quy hoạch (Planning) để tránh tắc nghẽn cổng.

### Chart 3.1: Gate Peak Hour Heatmap (Bản đồ nhiệt giờ cao điểm)
*   **Dataset**: `gold_kpi_peak_hours`
*   **Chart Type**: Heatmap
*   **Settings**:
    *   **X-Axis**: `hour_of_day` (0-23)
    *   **Y-Axis**: `day_name` (Monday, Tuesday...)
    *   **Metric**: `SUM(avg_activity)`
    *   **Color Scheme**: Red/Orange (Màu nóng)
*   **Ý nghĩa**: **BIỂU ĐỒ QUAN TRỌNG NHẤT**. Cho biết giờ nào trong ngày nào là đông xe nhất (Ví dụ: 14h chiều Thứ 6). Giúp phân bổ cổng (Gate lane) hợp lý.

### Chart 3.2: Busy Facility Distribution (Phân bổ tải giữa các cảng)
*   **Dataset**: `gold_kpi_peak_hours`
*   **Chart Type**: Pie Chart
*   **Settings**:
    *   **Dimensions**: `facility`
    *   **Metric**: `SUM(total_activity)`
*   **Ý nghĩa**: Xem tỷ trọng hoạt động giữa CT01, CT02... Cảng nào đang gánh tải nhiều nhất?

---

## 🎨 HƯỚNG DẪN LAYOUT (BỐ CỤC ĐẸP)

### Row 1: KPI Cards (Big Number)
*   Total Containers In-Yard (from `gold_ops_metrics_realtime`)
*   Total Backlog Count (from `gold_backlog_metrics`)
*   Avg Dwell Time 7 Days (from `gold_kpi_daily` - `rolling_7d_avg_throughput`)

### Row 2: Operation Status
*   [Chart 1.1: Inventory by Danger Level] (Width: 6)
*   [Chart 1.2: Operational Backlog] (Width: 6)

### Row 3: Trends & Shifts
*   [Chart 2.1: Shift Productivity] (Width: 8)
*   [Chart 3.1: Peak Hour Heatmap] (Width: 4)

### Row 4: Detailed Lists
*   [Chart 1.3: Critical List] (Full Width)

---

## 💡 MẸO SỬ DỤNG
1.  **Auto-Refresh**: Set dashboard refresh **30s** cho Tab 1, **5 phút** cho Tab 2 & 3.
2.  **Filter Box**: Thêm bộ lọc `Facility` chung cho cả Dashboard để người dùng có thể switch view từ "Toàn cảng" sang từng bãi (CT01, CT02...).
3.  **Màu sắc**:
    -   `CRITICAL` / `High Dwell` -> **Đỏ**
    -   `Warning` -> **Vàng**
    -   `Normal` / `Closed` -> **Xanh dương/Xanh lá**
