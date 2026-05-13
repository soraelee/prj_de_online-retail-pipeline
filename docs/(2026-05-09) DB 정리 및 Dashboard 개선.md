# DB 정리 및 Dashboard 개선 기록

## 1. 개요

2026-05-09에는 기존 적재 데이터의 중복 문제를 정리하고, API 및 Spring Boot Dashboard에 추가 인사이트를 표현하도록 수정하였다.

주요 작업 범위는 다음과 같다.

| 구분 | 작업 내용 |
|---|---|
| DB 정리 | `raw_retail_events`, `order_info`, `order_detail`, `dim_customer`, `dim_product`, `mart_*` 중복 정리 |
| API | 일별 추이, 시간대별 주문/취소량, 취소 이상 상품, 국가별 매출 API 추가 |
| Dashboard | JSP 화면 구성 개편, ApexCharts 차트 추가/수정 |
| Docker | Spring Boot Dashboard를 Docker Compose에서 실행할 수 있도록 구성 |

---

## 2. DB 중복 정리

### 2-1. 문제

기존 DB에는 Streaming 재처리 및 batch append 영향으로 다음 테이블에 중복 데이터가 존재하였다.

| 테이블 | 중복 기준 |
|---|---|
| `raw_retail_events` | `event_id` |
| `order_info` | `invoice_no` |
| `order_detail` | `event_id` |
| `dim_customer` | `customer_id` |
| `dim_product` | `stock_code` |

특히 `dim_customer`, `dim_product`는 전체 raw 기준으로 매번 재생성되면서 append되어 동일 key가 여러 번 쌓였다.

### 2-2. 정리 방식

정리 스크립트:

```
scripts/cleanup_retail_duplicates.sql
```

처리 흐름:

```
backup_* 테이블 생성
↓
raw_retail_events 중복 정리
↓
order_info 재생성
↓
order_detail 재생성
↓
dim_customer 재생성
↓
dim_product 재생성
↓
mart_daily_orders 재생성
↓
mart_product_sales 재생성
↓
mart_customer_repeat 재생성
↓
unique index 생성
```

### 2-3. 정리 후 결과

최종 row count:

| 테이블 | row count |
|---|---:|
| `raw_retail_events` | 166,224 |
| `order_info` | 8,498 |
| `order_detail` | 166,224 |
| `dim_customer` | 2,418 |
| `dim_product` | 2,835 |
| `mart_daily_orders` | 115 |
| `mart_product_sales` | 94,097 |
| `mart_customer_repeat` | 115 |

중복 검증:

| 테이블 | 중복 수 |
|---|---:|
| `raw_retail_events` | 0 |
| `order_info` | 0 |
| `order_detail` | 0 |
| `dim_customer` | 0 |
| `dim_product` | 0 |

### 2-4. 추가된 unique index

```
idx_raw_retail_events_event_id
idx_order_info_invoice_no
idx_order_detail_event_id
idx_dim_customer_customer_id
idx_dim_product_stock_code
```

### 2-5. 주의점

Spark Streaming이 실행 중인 상태에서 동일 `event_id`가 다시 들어오면 unique index에 의해 DB insert가 실패할 수 있다.

따라서 다음 개선이 필요하다.

```
1. producer.py 중복 send 제거
2. stream_raw_events.py에서 DB 중복 insert 방지
3. backfill 실행 전 처리 구간 reset 또는 upsert 전략 정리
```

---

## 3. API 인사이트 추가

파일:

```
api/main.py
```

### 3-1. 추가 API

| API | 목적 |
|---|---|
| `/api/v1/insights/daily-trend` | 일별 주문/취소/매출 추이 |
| `/api/v1/insights/hourly-order-cancel` | 시간대별 주문/취소량 |
| `/api/v1/insights/top-cancel-products` | 취소 이상 Top 상품 |
| `/api/v1/insights/country-sales` | 국가별 매출 |

### 3-2. 취소 이상 상품 예외 처리

취소 이상 Top 상품에서는 `AMAZON FEE` 계열을 제외하였다.

제외 조건:

```
UPPER(COALESCE(m.stock_code, '')) NOT IN ('AMAZONFE', 'AMAZONFEE')
UPPER(COALESCE(p.product_name, p.description, '')) NOT LIKE 'AMAZON FEE%'
```

### 3-3. 상품명 표시

상품별 차트에서 `stock_code`만 표시되던 문제를 개선하기 위해 `dim_product.product_name`을 함께 조회하도록 정리하였다.

표시 우선순위:

```
product_name
↓
description
↓
stock_code
```

---

## 4. Spring Boot Dashboard API Proxy 추가

파일:

```
dashboard/retail/src/main/java/com/project/pipeline/retail/Dashboard/DashboardAPIController.java
```

FastAPI를 직접 호출하지 않고 Spring Boot Dashboard에서 프록시하는 API를 추가하였다.

| Spring API | FastAPI |
|---|---|
| `/dashboard-api/insights/daily-trend` | `/api/v1/insights/daily-trend` |
| `/dashboard-api/insights/hourly-order-cancel` | `/api/v1/insights/hourly-order-cancel` |
| `/dashboard-api/insights/top-cancel-products` | `/api/v1/insights/top-cancel-products` |
| `/dashboard-api/insights/country-sales` | `/api/v1/insights/country-sales` |

---

## 5. Dashboard 화면 개선

수정 파일:

```
dashboard/retail/src/main/webapp/WEB-INF/views/dashboard.jsp
dashboard/retail/src/main/resources/static/js/main/main.js
dashboard/retail/src/main/resources/static/css/main/main.css
```

### 5-1. 화면 구성

최종 화면 구성:

```
요약 카드
↓
일별 주문 / 취소 / 매출 추이
↓
시간대별 주문 / 취소량
↓
전체 주문 / 취소 비율 | 국가별 매출 Top 10
↓
상품별 주문 Top 5 | 취소 이상 Top 5 상품
↓
일별 고객 재구매율
```

### 5-2. 추가/수정된 차트

| 차트 | 표현 방식 |
|---|---|
| 일별 주문 / 취소 추이 | line chart |
| 시간대별 주문 / 취소량 | bar chart |
| 전체 주문 / 취소 비율 | donut chart |
| 국가별 매출 Top 10 | donut chart |
| 상품별 주문 Top 5 | horizontal bar chart |
| 취소 이상 Top 5 상품 | horizontal bar chart |
| 일별 고객 재구매율 | line chart |

### 5-3. 기본 조회 기간

조회 날짜 기본값은 브라우저 기준 현재 월의 시작일과 마지막일로 설정하였다.

예시:

```
2026-05-01 ~ 2026-05-31
```

### 5-4. UI 오류 수정

#### `jquery-ui.css` 404 제거

기존 JSP에서 없는 파일을 참조하고 있었다.

```
../css/plugin/jquery-ui.css
```

실제 프로젝트에는 JS 파일만 존재하므로 CSS 참조를 제거하였다.

#### ApexCharts tooltip 오류 해결

오류:

```
tooltip.shared cannot be enabled when tooltip.intersect is true
```

해결:

```
tooltip: {
    shared: true,
    intersect: false
}
```

#### 시간대별 주문/취소량 라벨 수정

수정 내용:

```
1. 흰색 라벨로 안 보이던 문제 수정
2. 라벨 배경 제거
3. 1K, 2K 축약 표기 제거
4. 1800처럼 전체 숫자로 표시
```

---

## 6. Docker Dashboard 구성

추가 파일:

```
Dockerfile.dashboard
```

수정 파일:

```
docker-compose.yaml
```

### 6-1. 추가된 compose service

```
dashboard:
  build:
    context: .
    dockerfile: Dockerfile.dashboard
  container_name: retail-dashboard
  ports:
    - "8082:8082"
  environment:
    RETAIL_API_BASE_URL: http://api:8000
  depends_on:
    - api
```

### 6-2. 실행 명령

```
docker compose up --build -d dashboard
```

접속 주소:

```
http://localhost:8082/main
```

---

## 7. 검증 내용

### 7-1. API 검증

FastAPI 컨테이너 rebuild:

```
docker compose up --build -d api
```

확인한 API:

```
/api/v1/insights/daily-trend
/api/v1/insights/hourly-order-cancel
/api/v1/insights/top-cancel-products
/api/v1/insights/country-sales
```

### 7-2. Spring Boot 검증

Gradle test:

```
./gradlew test -q
```

Dashboard Docker build:

```
docker compose up --build -d dashboard
```

페이지 응답 확인:

```
curl -I http://localhost:8082/main
```

Spring proxy API 확인:

```
curl http://localhost:8082/dashboard-api/insights/hourly-order-cancel?startDate=2026-05-01&endDate=2026-05-31
```

---

## 8. 후속 작업

### 8-1. Streaming 중복 insert 방지

현재 DB에는 unique index가 생성되어 있으므로, 동일 이벤트가 다시 들어오면 Spark Streaming batch가 실패할 수 있다.

후속으로 `stream_raw_events.py`의 JDBC append 전략을 다음 중 하나로 바꿀 필요가 있다.

```
1. DB insert 전 중복 event_id 필터링
2. PostgreSQL ON CONFLICT DO NOTHING 사용
3. raw/order 테이블을 staging table에 쓴 뒤 merge
```

### 8-2. Dashboard 시각 검증

현재는 API 응답과 정적 파일 반영 여부를 `curl` 기준으로 확인하였다.

추후 브라우저 기준 확인 항목:

```
1. 차트 라벨 겹침 여부
2. 국가별 donut chart 가독성
3. 상품명 길이가 긴 경우 tooltip/axis 표시 상태
4. 반응형 구간에서 grid 배치 유지 여부
```
