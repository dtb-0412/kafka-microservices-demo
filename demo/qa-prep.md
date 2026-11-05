# Q&A vấn đáp – Demo Event-Driven Architecture (EDA) với Kafka

Repo demo: `Copubah/kafka-microservices-demo` (domain "đơn hàng": Order → Payment → Notification).

## Tóm tắt nhanh

| Thành phần | Cổng | Vai trò |
| :--- | :---: | :--- |
| `order-service` | 8001 | Nhận `POST /orders`, phát event `order_created` |
| `payment-service` | 8002 | Nhận `order_created`, mô phỏng thanh toán 2 giây, phát `payment_completed` |
| `notification-service` | 8003 | Nhận `payment_completed`, mô phỏng gửi thông báo bằng log |
| Kafka | 9092 (host), `kafka:29092` (nội bộ) | Event broker: lưu và phân phối event |
| ZooKeeper | 2181 | Điều phối cho Kafka trong cấu hình này |
| Kafka UI | 8080 | Xem topic, message, consumer group |

- Luồng: HTTP → Order → Kafka (`order_created`) → Payment → Kafka (`payment_completed`) → Notification.
- Cổng 8002 và 8003 chỉ để kiểm tra service (`/health`); nghiệp vụ chạy qua Kafka.
- Công nghệ: Docker Compose, Kafka, ZooKeeper, Kafka UI. ⚠ Ngôn ngữ và framework của 3 service: xem trong repo trước khi thi.
- Demo gồm 3 kịch bản: tạo đơn và theo dõi chuỗi event; dừng Payment rồi xử lý tiếp; dừng Notification.
- Phạm vi: thanh toán và thông báo là mô phỏng. Repo chưa có Event Sourcing/CQRS, chống xử lý trùng, hay đảm bảo đúng một lần. Một broker không chứng minh high availability.

---

## Nhóm A. Tổng quan repo và kiến trúc

### 1. Repo này làm gì, gồm những thành phần nào?
Ví dụ áp dụng EDA cho quy trình đặt hàng. Có 3 service: Order (nhận đơn), Payment (thanh toán), Notification (thông báo). Chúng không gọi HTTP sang nhau mà giao tiếp qua Kafka bằng hai topic `order_created` và `payment_completed`. Hạ tầng gồm Kafka, ZooKeeper và Kafka UI, dựng bằng Docker Compose.

### 2. Mô tả luồng khi tạo một đơn hàng.
1. Client gọi `POST /orders` vào Order Service, nhận ngay response có `order_id` và `status: created`.
2. Order phát event `order_created` vào Kafka.
3. Payment nhận event, mô phỏng thanh toán 2 giây, rồi phát `payment_completed`.
4. Notification nhận `payment_completed` và in thông báo ra log.

`order_id` nối các event của cùng một đơn. Response ở bước 1 trả về trước khi bước 3 và 4 xong, đó là xử lý bất đồng bộ.

### 3. Vai trò của Kafka và ZooKeeper trong repo? Vì sao có ZooKeeper?
Kafka là event broker: nhận, lưu và phân phối event giữa các service. ZooKeeper là thành phần điều phối mà cấu hình Kafka này cần (quản lý metadata của cluster). Kafka bản mới có chế độ KRaft không cần ZooKeeper, nhưng repo này dùng cách cũ. ⚠ Nếu bị hỏi sâu về ZooKeeper, nói rõ đó là hạ tầng hỗ trợ, không phải trọng tâm của demo.

---

## Nhóm B. Lý thuyết EDA

### 4. Vì sao dùng EDA thay vì để Order Service gọi REST trực tiếp?
Với REST, Order phải biết tất cả service kia, chờ từng cái trả lời, một service chết thì cả đơn thất bại, và thêm chức năng thì phải sửa Order. Đó là coupling chặt của kiến trúc request-driven. Với EDA, Order chỉ công bố event, ai quan tâm thì tự đăng ký nhận. Order không biết có bao nhiêu consumer và không chờ họ.

### 5. Event khác command ở điểm nào? Trong repo, đâu là event?
Event là sự thật đã xảy ra, đặt tên thì quá khứ, bất biến, không ai "nhận lệnh". Command là yêu cầu "hãy làm việc này", gửi đến một bên cụ thể. Trong repo, `order_created` và `payment_completed` là event (tên quá khứ). `POST /orders` là yêu cầu từ client vào Order Service, còn Order không ra lệnh cho Payment mà chỉ công bố `order_created`, Payment tự quyết định phản ứng.

### 6. Trong repo, ai là producer, event broker, consumer?
- Order: producer của `order_created`.
- Kafka: event broker.
- Payment: consumer của `order_created` và đồng thời là producer của `payment_completed`.
- Notification: consumer của `payment_completed`.

Một service có thể vừa là consumer vừa là producer. Kịch bản 2 chứng minh producer không biết consumer: Payment dừng mà Order vẫn nhận đơn bình thường.

### 7. Repo dùng broker topology (choreography) hay mediator topology (orchestration)?
Choreography: không có orchestrator trung tâm, mỗi service nghe event và tự xử lý rồi phát event tiếp theo. Ưu điểm là lỏng lẻo và linh hoạt. Nhược điểm là khó nhìn toàn bộ luồng, nên trong demo phải đối chiếu `order_id` giữa log, Kafka UI và consumer group. Nếu nghiệp vụ nhiều bước, cần thứ tự chặt và bù trừ khi lỗi thì orchestration (hoặc Saga) hợp hơn, đổi lại orchestrator là điểm tập trung và coupling cao hơn.

### 8. Event trong repo thuộc mẫu event notification hay event-carried state transfer?
Trong kịch bản 3, Notification in được `order_id` và số tiền `$299.99` từ event nhận được, không thấy gọi ngược HTTP sang Order hay Payment, nên gần với event-carried state transfer (event mang đủ dữ liệu). ⚠ Mở Kafka UI, xem **Value** của `payment_completed` để xác nhận payload có những trường nào trước khi trả lời chắc chắn. Event notification thì chỉ báo "có chuyện xảy ra", consumer muốn biết thêm phải tự hỏi lại.

### 9. Publish/subscribe và queue khác nhau thế nào? Kafka làm cả hai ra sao?
Publish/subscribe: một event đến nhiều consumer. Queue: chia việc cho một trong nhiều worker. Kafka dùng consumer group để làm cả hai: các group khác nhau đều nhận đủ event (pub/sub), còn các instance trong cùng một group chia nhau partition (giống queue). Trong repo mỗi service là một group riêng (ví dụ group `payment-service`), nên Payment và Notification tiến độ độc lập. ⚠ Xem tên group của Notification bằng `kafka-consumer-groups --all-groups --describe`.

### 10. EDA khác Event Sourcing và CQRS thế nào? Repo có dùng không?
EDA dùng event để các thành phần giao tiếp, thường qua broker. Event Sourcing dùng event làm nguồn lưu trữ trạng thái. CQRS tách mô hình ghi và đọc. Ba thứ hay đi cùng nhau nhưng độc lập. Repo này chỉ là EDA qua Kafka, chưa có Event Sourcing hay CQRS hoàn chỉnh. Event trong Kafka phục vụ giao tiếp giữa service, không phải kho lưu trạng thái của nghiệp vụ.

---

## Nhóm C. Kafka: topic, offset, consumer group

### 11. Topic, partition, offset, consumer group là gì? Repo dùng thế nào?
Topic là kênh chứa event cùng loại (`order_created`, `payment_completed`). Topic chia thành partition; mỗi message trong một partition có offset tăng dần. Consumer group là nhóm consumer cùng đọc một topic; Kafka lưu offset đã đọc riêng cho từng group và partition. ⚠ Số partition của mỗi topic: xem trong Kafka UI.

### 12. Kafka đọc xong có xóa message không? Lag là gì, vì sao dùng nó trong demo?
Không. Đọc không xóa message, message được giữ theo chính sách lưu của Kafka, còn tiến độ đọc nằm ở offset của từng group. Lag = số message đã có trong topic nhưng group chưa xử lý (log end offset trừ offset hiện tại của group). Demo dùng lệnh `kafka-consumer-groups --describe` để chứng minh bằng số: lag tăng khi consumer dừng và về `0` khi chạy lại.

---

## Nhóm D. Thách thức và đánh đổi

### 13. Eventual consistency xuất hiện ở đâu? Saga là gì, repo có không?
Ngay khi tạo đơn, response trả `status: created` nhưng thanh toán và thông báo chưa xong, dữ liệu chỉ nhất quán sau một khoảng trễ. Giao dịch nhiều bước giữa nhiều service thường dùng Saga: mỗi bước có bước bù trừ khi lỗi (ví dụ thanh toán lỗi thì hủy đơn). Repo chưa có Saga; thanh toán chỉ là mô phỏng. ⚠ Xem code Payment có nhánh thanh toán thất bại hay không.

### 14. Một event có thể bị xử lý hai lần không? Repo xử lý thế nào?
Có thể. Broker thường đảm bảo at-least-once: nếu consumer xử lý xong nhưng chưa kịp commit offset rồi chết, event sẽ được đọc lại. Cách xử lý là consumer idempotent, tức xử lý lại cũng không gây hậu quả (ví dụ lưu `order_id` đã xử lý và bỏ qua nếu gặp lại). Repo chưa có cơ chế chống trùng, và demo dừng service lúc hệ thống rảnh nên không chứng minh được tình huống này.

### 15. Thứ tự event được đảm bảo thế nào?
Kafka chỉ đảm bảo thứ tự trong cùng một partition. Muốn các event của cùng một đơn đi theo đúng thứ tự thì chọn partition key hợp lý, ví dụ `orderId`, để chúng vào cùng partition. Giữa hai topic không có thứ tự toàn cục; thứ tự nghiệp vụ ở đây đến từ quan hệ nhân quả (`payment_completed` chỉ phát sau khi nhận `order_created`). ⚠ Xem Order Service có đặt key khi phát event hay không.

### 16. Nếu mất event hoặc xử lý lỗi thì sao? Outbox pattern để làm gì?
Dùng retry cho lỗi tạm thời và Dead Letter Queue cho event không xử lý được. Outbox pattern giải quyết việc ghi DB và phát event bị lệch nhau: ghi event vào bảng outbox trong cùng giao dịch với dữ liệu nghiệp vụ, rồi có tiến trình riêng đẩy sang broker. Repo chưa có các cơ chế này. Với repo, Kafka phải còn hoạt động thì Order mới phát được event. ⚠ Thử xem Order phản hồi thế nào nếu Kafka không sẵn sàng trước khi nói chắc.

### 17. Làm sao debug luồng bất đồng bộ? Đổi định dạng event thì sao?
Luồng bất đồng bộ khó lần vết, nên cần correlation ID và distributed tracing. Trong demo, `order_id` đóng vai trò liên kết các event và log, kèm Kafka UI để xem message. Về định dạng event, nếu đổi cấu trúc mà không quản lý có thể làm hỏng consumer cũ, nên cần quản lý schema và versioning (ví dụ schema registry). Repo chưa có phần này.

---

## Nhóm E. Triển khai và hạ tầng

### 18. Em triển khai thế nào, cần gì? Image, container và database nằm ở đâu?
Cần Docker Desktop. Các bước: `docker compose config --quiet` kiểm tra cấu hình, `docker compose up -d --build --wait --wait-timeout 240` build và dựng toàn bộ container nền, `docker compose ps -a` xem trạng thái, rồi gọi `/health` ở cổng 8001, 8002, 8003 và xem log. Dùng `.\watch-demo-logs.ps1` để theo dõi log, Kafka UI ở `http://localhost:8080`. Container Kafka kết nối nội bộ qua `kafka:29092`, từ máy host qua `9092`. Ba service được build từ mã nguồn; Kafka, ZooKeeper, Kafka UI dùng image có sẵn. Dữ liệu event nằm trong Kafka (container riêng), không nhét vào image service. ⚠ Xem compose có khai báo volume cho Kafka không để biết dữ liệu có mất khi xóa container.

---

## Nhóm F. Câu hỏi theo từng kịch bản demo

### Kịch bản 1 – Tạo đơn và theo dõi chuỗi sự kiện

### 19. Chạy kịch bản 1 và giải thích em chứng minh được gì?
Gửi `POST /orders` với `customer_id` chứa mã lượt (`$runId`), `amount = 99.99`, hai món hàng. Response có `order_id` và `status: created`. Sau đó quan sát: log Order in đã phát event, Payment nhận và hoàn tất, Notification in thông báo; trên Kafka UI thấy event trong `order_created` rồi `payment_completed` với cùng `order_id`. Chứng minh: một request HTTP khởi phát chuỗi xử lý qua hai topic, Payment vừa là consumer vừa là producer, và các service không gọi HTTP tuần tự cho nhau. Response trả về `created` trước khi thanh toán xong vì Payment mô phỏng mất 2 giây và chạy bất đồng bộ.

### Kịch bản 2 – Dừng Payment, tích lũy đơn rồi xử lý tiếp

### 20. Payment dừng mà vẫn tạo đơn được, vì sao? Lag cho thấy gì?
Em chạy `docker compose stop payment-service` (trạng thái `Exited`), rồi tạo hai đơn: cả hai đều `created`. Order chỉ ghi event vào Kafka chứ không gọi Payment, và Kafka giữ event chờ consumer. Lệnh `--describe` cho thấy lag của group `payment-service` thường là `2`, Kafka UI có hai event trong `order_created` nhưng chưa có `payment_completed` cho hai ID này. Điều kiện: Kafka phải còn hoạt động. Member của group có thể chưa biến mất ngay do session timeout, nên đối chiếu thêm trạng thái container và lag.

### 21. Khởi động lại Payment thì chuyện gì xảy ra? Vì sao không cần gửi lại request?
Payment chạy lại, đọc tiếp từ offset mà group đã lưu, xử lý hai đơn còn tồn, phát hai `payment_completed`, và Notification in hai thông báo. Lag về `0`. Không có request tạo đơn mới và Order không in thêm dòng tạo đơn. Lý do là event vẫn nằm trong Kafka và tiến độ đọc được quản lý riêng theo consumer group. ⚠ Nếu bị hỏi cách commit offset (tự động hay thủ công), xem cấu hình consumer trong code.

### Kịch bản 3 – Dừng Notification

### 22. Dừng Notification thì Order và Payment có bị ảnh hưởng không? Hai consumer group độc lập ra sao?
Không. Sau `docker compose stop notification-service`, em tạo đơn 299.99: Payment vẫn nhận và in `Payment completed`, lag của Payment có thể về `0`, trong khi lag của Notification tăng, thường là `1`. Mỗi consumer group có offset riêng trên topic của nó. Khi chạy lại Notification, nó in thông báo chứa đúng ID và `$299.99`; Order không tạo đơn mới và Payment không xử lý lại đơn đó. Chứng minh loose coupling: một service cuối chuỗi dừng không chặn các bước trước.

---

## Nhóm G. Phạm vi và hạn chế

### 23. Demo chứng minh được gì và chưa chứng minh được gì?
Chứng minh được: giao tiếp qua event, xử lý bất đồng bộ, loose coupling, Kafka giữ event khi consumer dừng và consumer xử lý tiếp phần còn tồn, offset theo từng consumer group. Chưa chứng minh được: an toàn trong mọi tình huống crash (các lần dừng đều lúc hệ thống rảnh), chống xử lý trùng, đúng một lần, Event Sourcing/CQRS. Một broker cũng không chứng minh high availability.

### 24. Repo này sẵn sàng cho production chưa? Cần bổ sung gì?
Chưa, đây là ví dụ minh họa với thanh toán và thông báo giả lập. Cần thêm: consumer idempotent, retry và Dead Letter Queue, Outbox pattern, Saga cho giao dịch nhiều bước, quản lý schema và versioning, correlation ID và tracing, giám sát lag, bảo mật, kiểm thử, và Kafka nhiều broker có replication để chịu lỗi (hiện chỉ có một broker).
