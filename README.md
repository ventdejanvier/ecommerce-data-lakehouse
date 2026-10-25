# E-commerce Data Lakehouse & Recommendation System

Dự án xây dựng một hệ thống xử lý dữ liệu thương mại điện tử theo kiến trúc Data Lakehouse, kết hợp ứng dụng mua sắm mô phỏng và các phương pháp gợi ý sản phẩm.

Hệ thống sử dụng **Apache Spark, Delta Lake và MinIO** để xử lý dữ liệu tương tác; **Kafka và Redis** để tiếp nhận, lưu trữ tín hiệu hành vi gần đây; **FastAPI và PostgreSQL** để cung cấp dữ liệu cho giao diện **Next.js**. Ngoài ra, dự án cấu hình **Airflow, Hive Metastore, Trino và Metabase** phục vụ điều phối, truy vấn và phân tích dữ liệu.

## 1. Mục tiêu dự án

- Xây dựng quy trình xử lý dữ liệu theo mô hình **Medallion Architecture (Bronze - Silver - Gold)**.
- Làm sạch, chuẩn hóa và tổng hợp dữ liệu hành vi mua sắm bằng **PySpark**.
- Khai thác dữ liệu phục vụ phân tích sản phẩm, hành vi người dùng và phân nhóm khách hàng.
- Tích hợp các chiến lược gợi ý sản phẩm vào API và giao diện thương mại điện tử mô phỏng.
- Ghi nhận sự kiện tương tác và sử dụng tín hiệu quan tâm gần đây để điều chỉnh thứ tự gợi ý.

## 2. Kiến trúc và luồng xử lý

Dự án kết hợp hai nguồn dữ liệu chính:

**Dữ liệu lịch sử:** Dữ liệu sự kiện từ tệp CSV được Apache Spark nạp và xử lý theo ba tầng của Data Lakehouse, lưu dưới định dạng Delta Lake trên MinIO.

| Tầng | Vai trò | Kết quả chính |
| --- | --- | --- |
| **Bronze** | Tiếp nhận dữ liệu nguồn và bổ sung thông tin lần nạp | Dữ liệu sự kiện thô |
| **Silver** | Loại bỏ bản ghi trùng, xử lý dữ liệu thiếu và làm giàu thuộc tính | Dữ liệu sự kiện đã được chuẩn hóa |
| **Gold** | Tạo dữ liệu phục vụ báo cáo và phân tích | Danh mục sản phẩm, lịch sử tương tác, phễu mua hàng, sản phẩm phổ biến và phân nhóm RFM |

**Dữ liệu tương tác từ ứng dụng:** Các thao tác trên giao diện Next.js được chuyển đến Tracking API của FastAPI. Backend có thể gửi sự kiện đến **Kafka** và cập nhật mức độ quan tâm theo danh mục trong **Redis**. Những tín hiệu này được dùng khi xếp hạng lại một số kết quả gợi ý.

Tại lớp cung cấp dữ liệu, **FastAPI** truy vấn **PostgreSQL** để lấy thông tin sản phẩm và các kết quả recommendation đã được chuẩn bị. **Trino** và **Metabase** nằm trong cấu hình hạ tầng phục vụ truy vấn, trực quan hóa và khám phá dữ liệu.

## 3. Công nghệ sử dụng

| Nhóm | Công nghệ | Vai trò |
| --- | --- | --- |
| Xử lý dữ liệu | Python, PySpark, Spark SQL | Nạp, biến đổi và tổng hợp dữ liệu |
| Lưu trữ Lakehouse | Delta Lake, MinIO | Quản lý dữ liệu theo các tầng Bronze, Silver, Gold |
| Điều phối | Apache Airflow | Quản lý tác vụ xử lý dữ liệu |
| Sự kiện và bộ nhớ tạm | Apache Kafka, Redis | Tiếp nhận sự kiện, lưu tín hiệu quan tâm gần đây |
| Cơ sở dữ liệu và API | PostgreSQL, SQLAlchemy, FastAPI | Truy xuất sản phẩm và kết quả gợi ý |
| Giao diện | Next.js, React, TypeScript, Tailwind CSS, Zustand | Xây dựng website thương mại điện tử mô phỏng |
| Phân tích dữ liệu | Hive Metastore, Trino, Metabase | Quản lý metadata, truy vấn và hỗ trợ báo cáo |
| Môi trường | Docker Compose, Jupyter | Thiết lập các dịch vụ và thực thi thử nghiệm |

## 4. Chức năng chính

### Xử lý và phân tích dữ liệu

- Nạp dữ liệu tương tác người dùng từ CSV vào tầng Bronze.
- Loại bỏ dữ liệu trùng lặp, bổ sung thông tin danh mục và thương hiệu tại tầng Silver.
- Tổng hợp dữ liệu tầng Gold, bao gồm:
  - Thông tin sản phẩm (`dim_products`).
  - Sự kiện tương tác (`fact_events`).
  - Điểm tương tác người dùng - sản phẩm (`user_interactions`).
  - Phễu hành vi mua hàng (`behavior_funnel`).
  - Sản phẩm nổi bật (`top_trending`).
  - Phân nhóm khách hàng theo RFM (`user_rfm`).

### Gợi ý sản phẩm

Backend hỗ trợ truy xuất và kết hợp nhiều nguồn gợi ý:

- **ALS / Collaborative Filtering:** đề xuất dựa trên dữ liệu tương tác người dùng - sản phẩm.
- **Cluster-based:** sử dụng kết quả phân nhóm khách hàng.
- **Content-based:** tìm các sản phẩm tương tự theo đặc trưng sản phẩm.
- **Item-based:** đề xuất sản phẩm liên quan đến các mặt hàng trong giỏ.
- **Fallback và reranking:** sử dụng các nguồn dự phòng hoặc điều chỉnh thứ tự theo tín hiệu danh mục gần đây trong Redis.

### Ứng dụng thương mại điện tử mô phỏng

- Hiển thị danh sách và thông tin chi tiết sản phẩm.
- Tìm kiếm, lọc sản phẩm theo danh mục hoặc thương hiệu.
- Quản lý giỏ hàng trong giao diện mô phỏng.
- Hiển thị gợi ý tại trang chủ, trang sản phẩm và khu vực giỏ hàng.
- Ghi nhận sự kiện tương tác để phục vụ luồng tracking và điều chỉnh gợi ý.

## 5. Cấu trúc repository

Các thư mục chính:

- `backend/`: FastAPI, truy vấn PostgreSQL, xử lý recommendation, Kafka producer và Redis.
- `frontend/`: ứng dụng Next.js, các thành phần giao diện và logic tracking.
- `docker/docker-compose.yml`: cấu hình môi trường và các dịch vụ hỗ trợ.
- `docker/scripts/`: các script nạp dữ liệu và xử lý Bronze → Silver → Gold.
- `docker/dags/`: DAG Airflow phục vụ tác vụ biến đổi dữ liệu.
- `docker/trino/`: cấu hình catalog truy vấn.
- `docs/`: tài liệu bổ sung của dự án (nếu có).

## 6. Hướng dẫn cài đặt và chạy thử

### Yêu cầu

- Git và **Docker Desktop / Docker Engine** có hỗ trợ Docker Compose.
- **Python 3.12+** và trình quản lý môi trường Python **uv** để chạy backend.
- **Node.js** và npm tương thích với phiên bản Next.js được sử dụng trong `frontend/package.json`.
- Bộ dữ liệu sự kiện thương mại điện tử có cấu trúc phù hợp.

Hệ thống có nhiều dịch vụ dữ liệu nên cần đủ RAM và dung lượng lưu trữ để chạy thử cục bộ.

### Bước 1: Tải mã nguồn

```bash
git clone https://github.com/ventdejanvier/ecommerce-data-lakehouse.git
cd ecommerce-data-lakehouse
```

### Bước 2: Khởi động hạ tầng Docker

Thực hiện tại thư mục gốc repository:

```bash
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps
```

Một số địa chỉ truy cập sau khi dịch vụ khởi động thành công:

| Dịch vụ | Địa chỉ |
| --- | --- |
| MinIO Console | `http://localhost:9001` |
| Spark Master UI | `http://localhost:8080` |
| Apache Airflow | `http://localhost:8081` |
| Trino | `http://localhost:8082` |
| Jupyter | `http://localhost:8888` |
| Metabase | `http://localhost:3001` |
| PostgreSQL | `localhost:5434` |
| Redis | `localhost:6379` |

### Bước 3: Chuẩn bị dữ liệu đầu vào

Đặt tệp dữ liệu tại:

```text
docker/data/electronics_events.csv
```

Các cột đầu vào mà script nạp dữ liệu yêu cầu:

```text
brand, category_code, category_id, event_time, event_type,
price, product_id, user_id, user_session
```

Tại MinIO, tạo các bucket `bronze`, `silver` và `gold`.

### Bước 4: Thực hiện quy trình Bronze → Silver → Gold

Chạy lần lượt các lệnh sau từ thư mục gốc repository, sau khi Jupyter và các dịch vụ lưu trữ cần thiết đã sẵn sàng:

```bash
# Nạp dữ liệu từ CSV vào Bronze
docker exec jupyter-notebook spark-submit --packages io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.2 /home/jovyan/scripts/ingest_raw_to_bronze.py

# Xử lý Bronze sang Silver
docker exec jupyter-notebook spark-submit --packages io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.2 /home/jovyan/scripts/transform_bronze_to_silver.py

# Tổng hợp Silver sang Gold
docker exec jupyter-notebook spark-submit --packages io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.2 /home/jovyan/scripts/transform_silver_to_gold.py
```

Dự án có DAG `transform_gold_dag` cho bước Gold trong Airflow. DAG hiện được cấu hình chạy thủ công, không tự lên lịch cho toàn bộ pipeline.

### Bước 5: Chạy backend FastAPI

Mở một terminal khác tại thư mục gốc dự án:

```bash
cd backend
uv sync
uv run uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

Có thể thiết lập tệp `backend/.env` khi cần thay đổi thông tin kết nối:

```dotenv
DATABASE_URL=postgresql://user:password@localhost:5434/data_lakehouse
REDIS_HOST=127.0.0.1
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

- Kiểm tra API: `http://localhost:8000/api/health`
- Tài liệu API: `http://localhost:8000/docs`

### Bước 6: Chạy frontend Next.js

Tại một terminal khác, từ thư mục gốc dự án:

```bash
cd frontend
npm install
npm run dev
```

Truy cập website tại `http://localhost:3000`.

Nếu địa chỉ backend khác mặc định, có thể khai báo trong `frontend/.env.local`:

```dotenv
NEXT_PUBLIC_BACKEND_API_URL=http://localhost:8000
BACKEND_API_URL=http://127.0.0.1:8000
```

### Dừng hệ thống

Tại thư mục gốc repository:

```bash
docker compose -f docker/docker-compose.yml down
```

## 7. Một số API tiêu biểu

| Phương thức | Endpoint | Mục đích |
| --- | --- | --- |
| `GET` | `/api/health` | Kiểm tra trạng thái cơ bản của backend |
| `GET` | `/api/products` | Danh sách, tìm kiếm và lọc sản phẩm |
| `GET` | `/api/categories` | Danh sách danh mục |
| `GET` | `/api/recommend/global` | Sản phẩm gợi ý phổ biến |
| `GET` | `/api/recommend/home/{user_id}` | Gợi ý tại trang chủ |
| `GET` | `/api/recommend/product/{product_id}` | Sản phẩm tương tự |
| `POST` | `/api/recommend/cart` | Gợi ý dựa trên giỏ hàng |
| `POST` | `/api/track` | Ghi nhận sự kiện tương tác |

Có thể xem định nghĩa request/response và các API khác tại Swagger UI khi backend đang chạy.

## 8. Giới hạn hiện tại và hướng phát triển

- **Mô hình gợi ý:** Các bảng kết quả recommendation trong PostgreSQL cần được khởi tạo và nạp riêng.
- **Tự động hóa:** Chưa xác nhận có quy trình khép kín tự động từ sự kiện Kafka đến huấn luyện lại mô hình và cập nhật bảng phục vụ.
- **Xác thực:** Luồng đăng nhập trên frontend đang ở mức mô phỏng, không phải cơ chế xác thực dành cho môi trường production.
- **Triển khai:** Docker Compose chủ yếu cấu hình các dịch vụ hạ tầng; frontend và backend chạy riêng theo hướng dẫn ở trên.

Dự án có thể được mở rộng theo hướng tự động hóa quy trình dữ liệu và model serving, bổ sung kiểm thử tích hợp, cơ chế bảo mật dữ liệu, theo dõi chất lượng gợi ý và hoàn thiện triển khai hệ thống.

## Tác giả

**[ventdejanvier](https://github.com/ventdejanvier)**
