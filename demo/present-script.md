# Script trình bày: Event-Driven Architecture (EDA)

Thời lượng ước tính: 5 phút

---

## 1. Mở đầu: vấn đề cần giải quyết (~30s)

Khi khách đặt hàng, hệ thống cần trừ kho, thanh toán, gửi email và tạo vận đơn. Nếu Order Service gọi trực tiếp từng service bằng REST, nó phải biết tất cả service kia, phải chờ từng cái trả lời, và chỉ cần một service chết là cả đơn hàng thất bại. Thêm một chức năng mới nghĩa là phải sửa lại Order Service. Đây là vấn đề coupling chặt của kiến trúc hướng yêu cầu (request-driven).

## 2. EDA là gì (~50s)

EDA đảo ngược cách giao tiếp này. Thay vì ra lệnh cho service khác, một service chỉ công bố một **sự kiện**, ví dụ `OrderCreated`.

Sự kiện là một **sự thật đã xảy ra**: đặt tên ở thì quá khứ, bất biến, không thể sửa. Điều này khác với command, là yêu cầu "hãy làm việc này" và gửi đến một bên cụ thể.

Có ba vai trò chính:
- **Producer** là nơi phát sinh và công bố sự kiện.
- **Event broker** (như Kafka, RabbitMQ) là kênh trung gian nhận, lưu và phân phối sự kiện.
- **Consumer** là bên đăng ký nhận sự kiện và tự quyết định phản ứng.

Điểm mấu chốt là producer **không biết** có bao nhiêu consumer, và cũng không chờ họ.

## 3. Hai topology (~50s)

EDA có hai cách tổ chức.

**Broker topology**, hay còn gọi là choreography: không có bộ điều phối trung tâm. Mỗi service nghe sự kiện, xử lý xong thì, giống như các vũ công tự phối hợp. Ưu điểm là rất lỏng lẻo và linh hoạt. Nhược điểm là khó nhìn ra toàn bộ luồng nghiệp vụ.

**Mediator topology**, hay orchestration: có một orchestrator điều phối thứ tự các bước và xử lý lỗi. Luồng rõ ràng và dễ kiểm soát hơn, nhưng orchestrator trở thành điểm tập trung và coupling cao hơn.

## 4. Các mẫu sự kiện thường gặp (~40s)

Có ba cách dùng sự kiện phổ biến:
- **Event notification**: sự kiện chỉ báo "có chuyện xảy ra", consumer muốn biết thêm thì tự hỏi lại.
- **Event-carried state transfer**: sự kiện mang theo đủ dữ liệu để consumer không cần gọi ngược lại producer.
- **Event sourcing**: sự kiện chính là nguồn lưu trữ trạng thái, đây là chủ đề 5.

Về kiểu phân phối: **publish/subscribe** gửi một sự kiện đến nhiều consumer, còn **queue** chia công việc cho một trong nhiều worker.

## 5. Lợi ích (~40s)

- **Loose coupling**: các service độc lập, chỉ thống nhất với nhau về định dạng sự kiện.
- **Dễ mở rộng chức năng**: muốn thêm tính năng, chỉ cần thêm một consumer mới, không sửa producer.
- **Khả năng chịu lỗi**: broker giữ sự kiện, consumer chết thì khi sống lại vẫn xử lý tiếp, không mất dữ liệu.
- **Scale độc lập** và xử lý bất đồng bộ, hệ thống chịu được tải đột biến.
- Phù hợp với xử lý thời gian thực và tích hợp nhiều hệ thống.

## 6. Thách thức và cách xử lý (~60s)

- **Eventual consistency**: dữ liệu giữa các service chỉ nhất quán sau một khoảng trễ. Giao dịch nhiều bước phải dùng **Saga**, với bước bù trừ khi lỗi.
- **Trùng lặp sự kiện**: broker thường đảm bảo *at-least-once*, nên một sự kiện có thể đến hai lần. Consumer phải **idempotent**, tức xử lý lại cũng không gây hậu quả.
- **Thứ tự sự kiện**: thường chỉ đảm bảo trong cùng partition, nên chọn partition key hợp lý, ví dụ `orderId`.
- **Mất sự kiện hoặc xử lý lỗi**: dùng retry và **Dead Letter Queue** cho sự kiện không xử lý được. Để ghi DB và phát sự kiện không bị lệch nhau, dùng **Outbox pattern**.
- **Khó debug**: luồng bất đồng bộ khó lần vết, cần **correlation ID** và distributed tracing.
- **Thay đổi định dạng sự kiện**: cần quản lý schema và versioning để không phá consumer cũ.

## 7. Khi nào nên và không nên dùng (~25s)

Nên dùng khi có nhiều thành phần cần phản ứng với cùng một sự kiện, cần scale độc lập, hoặc tích hợp bất đồng bộ.

Không nên dùng cho hệ thống nhỏ, nghiệp vụ cần phản hồi đồng bộ và nhất quán tức thời, hoặc khi đội ngũ chưa sẵn sàng vận hành broker và xử lý các vấn đề ở trên.

## 8. Kết (~15s)

Tóm lại, EDA đổi từ "gọi nhau trực tiếp" sang "công bố sự việc đã xảy ra", đổi lấy sự lỏng lẻo và khả năng mở rộng, với cái giá là độ phức tạp trong nhất quán và vận hành.
