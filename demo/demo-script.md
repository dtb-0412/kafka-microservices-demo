# Kịch bản demo Event-Driven Architecture với Apache Kafka

## Repository: `Copubah/kafka-microservices-demo`

> Mục tiêu: trình diễn luồng HTTP → Order Service → Kafka → Payment Service → Kafka → Notification Service; chứng minh xử lý bất đồng bộ, giao tiếp qua event, sự độc lập giữa các service và khả năng consumer xử lý tiếp sự kiện còn tồn.

---

## 0. Phạm vi và những gì cần chứng minh

Repository gồm ba ứng dụng:

| Thành phần | Vai trò |
| :--- | :--- |
| `order-service` | Nhận yêu cầu tạo đơn qua HTTP, phát `order_created` |
| `payment-service` | Nhận `order_created`, mô phỏng thanh toán trong 2 giây, phát `payment_completed` |
| `notification-service` | Nhận `payment_completed`, mô phỏng gửi thông báo bằng log |

Hạ tầng được khai báo trong `docker-compose.yml`:

| Thành phần | Cổng host | Mục đích |
| :--- | :---: | :--- |
| ZooKeeper | `2181` | Hạ tầng điều phối cho Kafka trong cấu hình này |
| Kafka | `9092` | Lưu và phân phối event; container kết nối nội bộ qua `kafka:29092` |
| Kafka UI | `8080` | Xem topic, nội dung message và consumer group |
| Order Service | `8001` | API tạo đơn |
| Payment Service | `8002` | HTTP kiểm tra service; nghiệp vụ chạy qua Kafka |
| Notification Service | `8003` | HTTP kiểm tra service; thông báo được in trong log |

### Những tính năng có thể demo

1. **Event-driven communication:** các service phản ứng với event trong Kafka, không gọi HTTP trực tiếp sang service kế tiếp.
2. **Asynchronous processing:** tạo đơn thành công trước khi hoàn tất các bước sau, kể cả khi Payment đang dừng.
3. **Loose coupling:** Notification dừng nhưng Order và Payment vẫn hoạt động.
4. **Buffering và xử lý tiếp:** Kafka giữ event khi consumer dừng; consumer chạy lại đọc phần còn tồn.
5. **Event log và consumer offset:** đọc không xóa message; tiến độ đọc được quản lý riêng theo consumer group và partition.

**Phạm vi:** thanh toán/thông báo là mô phỏng. Repo chưa có Event Sourcing/CQRS hoàn chỉnh, chống xử lý trùng hay bảo đảm xử lý đúng một lần. Các kịch bản dừng lúc hệ thống rảnh chứng minh xử lý tiếp khi consumer tạm dừng, không chứng minh an toàn trong mọi tình huống crash. Một broker không chứng minh high availability.

---

## 1. Chuẩn bị demo

**Khởi động và kiểm tra hệ thống**

```powershell
$ErrorActionPreference = 'Stop'
docker compose config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Cau hinh Compose khong hop le.' }
docker compose up -d --build --wait --wait-timeout 240
if ($LASTEXITCODE -ne 0) { throw 'Khoi dong chua thanh cong. Kiem tra log truoc khi demo.' }
docker compose ps -a

8001, 8002, 8003 | ForEach-Object {
    Invoke-RestMethod -Uri "http://localhost:$_/health" -TimeoutSec 15
}
docker compose logs --tail 15 payment-service notification-service
```

**Theo dõi log trong Terminal 2 của VS Code**

```powershell
.\watch-demo-logs.ps1
```

**Chuẩn bị mã lượt và kiểm tra tiến độ**

```powershell
$runId = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
Write-Host "Ma luot demo: $runId"
Start-Sleep -Seconds 10
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --all-groups --describe
```

**Giải thích:** mã lượt được chèn tự động vào dữ liệu, không cần sửa JSON hay ghi chép ID bằng tay. Nếu có dữ liệu tồn từ trước, chờ log xử lý xong và cột `LAG` về `0` trước kịch bản 1. Nếu mới khởi tạo hoàn toàn, group/offset có thể chưa xuất hiện; kiểm tra lại sau đơn đầu tiên.

**Kafka UI**

```text
http://localhost:8080
```

Chọn cluster `local` → **Topics** → chọn topic → **Messages** → tải message mới nhất bằng lựa chọn **Newest** nếu có. Mở rộng dòng để xem **Value**. Đối chiếu `order_id` được in trên terminal. Khi cần, F5 và tải lại message. **Consumer Groups** cho phép xem lại offset/lag đã hiển thị trong terminal.

---

# Kịch bản 1 — Tạo đơn và theo dõi chuỗi sự kiện

**Mục đích: chứng minh một yêu cầu HTTP khởi phát chuỗi xử lý qua hai topic; Payment vừa là consumer vừa là producer.**

### Bước 1.1 — Gửi yêu cầu tạo đơn và quan sát các event được phát ra

**Mục đích:** tạo đơn và tự lưu kết quả vào biến để đối chiếu các bước sau.

```powershell
$body = @{
    customer_id = "demo-$runId-flow"
    amount = 99.99
    items = @('laptop', 'mouse')
} | ConvertTo-Json

$order1 = Invoke-RestMethod -Uri 'http://localhost:8001/orders' -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 30
$order1 | ConvertTo-Json
```

**Kết quả:** response có `order_id`, khách hàng chứa mã lượt hiện tại và `status: created`.

**Giải thích:** Payment nhận dữ liệu từ event rồi phát kết quả thành event mới. `order_id` liên kết hai sự kiện; các service không cần gọi HTTP tuần tự cho nhau.

---

# Kịch bản 2 — Dừng Payment, tích lũy đơn rồi xử lý tiếp

**Mục đích: chứng minh producer và consumer không cần hoạt động cùng lúc; Kafka làm bộ đệm và tạo đơn không phải chờ hoàn tất thanh toán.**

### Bước 2.1 — Dừng Payment Service có kiểm soát

**Mục đích:** tạo tình huống consumer vắng mặt trước khi phát đơn mới.

```powershell
Start-Sleep -Seconds 5
docker compose stop payment-service
if ($LASTEXITCODE -ne 0) { throw 'Chua dung duoc Payment.' }
docker compose ps -a payment-service
```

**Kết quả:** Payment có trạng thái `Exited`; các service còn lại vẫn chạy.

**Quan sát Terminal 2:** có thể thấy log shutdown của Payment.

### Bước 2.2 — Tạo hai đơn khi Payment đang dừng

**Mục đích:** quan sát tạo đơn thành công nhưng công việc thanh toán còn chờ.

```powershell
$body = @{
    customer_id = "demo-$runId-queue-1"
    amount = 45.50
    items = @('Python book')
} | ConvertTo-Json
$order2 = Invoke-RestMethod -Uri 'http://localhost:8001/orders' -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 30

$body = @{
    customer_id = "demo-$runId-queue-2"
    amount = 12.99
    items = @('coffee')
} | ConvertTo-Json
$order3 = Invoke-RestMethod -Uri 'http://localhost:8001/orders' -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 30

$order2, $order3 | Format-Table order_id, customer_id, amount, status
Start-Sleep -Seconds 5
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --all-groups --describe
```

**Kết quả:** hai đơn đều `created`; group `payment-service` có lag tăng, thường là `2` nếu chỉ có hai đơn mới. Kafka UI có hai event trong `order_created`, chưa có event thanh toán của hai ID này.

**Quan sát Terminal 2:** Order in hai dòng `Order created and published` ứng với `$order2.order_id` và `$order3.order_id`, nhưng chưa có `Received order`/`Payment completed` hay thông báo cho hai đơn đó. Chỉ nhìn các ID của lượt hiện tại, bỏ qua log đơn cũ.

**Giải thích:** Order vẫn hoạt động khi Payment dừng; Kafka giữ công việc chờ consumer. Kafka phải còn hoạt động để nhận event. Member của group có thể chưa biến mất ngay do session timeout; dùng trạng thái container và lag để đối chiếu.

### Bước 2.3 — Khởi động Payment và quan sát xử lý phần còn tồn

**Mục đích:** xử lý các đơn đã phát mà không gửi lại request.

```powershell
docker compose start payment-service
if ($LASTEXITCODE -ne 0) { throw 'Chua khoi dong duoc Payment.' }
Start-Sleep -Seconds 15
Start-Sleep -Seconds 10
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --all-groups --describe
```

**Quan sát Terminal 2 trong lúc block đang chờ:** Payment khởi động, nhận hai đơn còn tồn rồi in `Payment completed` cho từng ID; Notification tiếp tục in hai khối thông báo. Không có dòng tạo đơn mới từ Order ở bước này.

**Kết quả Terminal 1:** lag về `0`.

**Giải thích:** consumer đọc phần còn tồn theo tiến độ của group. Trên Kafka UI, `payment_completed` xuất hiện thêm hai event có cùng ID với hai đơn đã tạo khi Payment dừng.

---

# Kịch bản 3 — Dừng Notification, Order và Payment vẫn hoạt động

**Mục đích: chứng minh bước thông báo có thể tạm dừng độc lập mà không ngăn tạo đơn và thanh toán.**

### Bước 3.1 — Dừng riêng Notification Service

**Mục đích:** cô lập consumer cuối chuỗi.

```powershell
Start-Sleep -Seconds 5
docker compose stop notification-service
if ($LASTEXITCODE -ne 0) { throw 'Chua dung duoc Notification.' }
docker compose ps -a
```

**Kết quả:** Notification `Exited`; Order, Payment và Kafka vẫn chạy.

**Quan sát Terminal 2:** có thể thấy log shutdown của Notification; Terminal 2 tiếp tục theo dõi các service còn lại.

**Giải thích:** Payment có thể phát kết quả vào Kafka mà không cần Notification trực tuyến nhận ngay.

### Bước 3.2 — Tạo đơn và xác nhận thanh toán khi chưa có thông báo

**Mục đích:** thấy thanh toán hoàn tất trong khi công việc thông báo còn chờ.

```powershell
$body = @{
    customer_id = "demo-$runId-notify"
    amount = 299.99
    items = @('keyboard', 'headphones')
} | ConvertTo-Json

$order4 = Invoke-RestMethod -Uri 'http://localhost:8001/orders' -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 10
$order4 | ConvertTo-Json
Start-Sleep -Seconds 5
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --all-groups --describe
```

**Quan sát Terminal 2 trong lúc block đang chờ:** Order phát đơn, Payment nhận và in `Payment completed` với ID mới, nhưng không xuất hiện thông báo của Notification cho ID này.

**Kết quả Terminal 1:** lag Notification tăng, thường là `1`, trong khi Payment có thể về `0`. Kafka UI có `payment_completed` cho đơn mới.

**Giải thích:** thanh toán không phụ thuộc vào việc gửi thông báo ngay lập tức. Hai consumer group có tiến độ riêng trên hai topic tương ứng.

### Bước 3.3 — Khởi động Notification và đối chiếu kết quả

**Mục đích:** phục hồi thông báo mà không tạo lại đơn hoặc thanh toán lại.

```powershell
docker compose start notification-service
if ($LASTEXITCODE -ne 0) { throw 'Chua khoi dong duoc Notification.' }
Start-Sleep -Seconds 5
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --all-groups --describe
```

**Quan sát Terminal 2 trong lúc block đang chờ:** Notification khởi động và in khối thông báo chứa đúng ID và `$299.99`. Order không tạo đơn mới, Payment không phải xử lý lại đơn này.

**Kết quả Terminal 1:** lag Notification về `0`. Nếu chưa có thông báo, tiếp tục quan sát Terminal 2; chờ thêm rồi chạy lại lệnh kiểm tra consumer group.

**Giải thích:** Notification đọc event đã nằm trong Kafka khi nó vắng mặt. Không có request tạo đơn hay yêu cầu thanh toán mới ở bước này.
---