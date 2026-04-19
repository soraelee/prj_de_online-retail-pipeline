# 데이터 엔지니어링 프로젝트
## 프로젝트 개요
**Online Retail Event Collection Pipeline**

<aside>
💡
본 프로젝트는 온라인 리테일 CSV 데이터를 실시간 이벤트처럼 재생하여 Kafka로 수집하는 파이프라인을 구현하였다. 각 거래 row를 주문 또는 취소 이벤트로 변환하고, `invoice_no`를 key로 하여 `retail-events` topic에 전송함으로써 Kafka 기반 이벤트 수집 구조를 설계하였다.

</aside>

### 데이터

**Retail Transaction Logs**

구매 이력 데이터판매 분석 / 피크 시간 탐지

https://archive.ics.uci.edu/dataset/352/online%2Bretail

- data/online-retail.csv 에 저장

**컬럼**
- InvoiceNo : 송장번호 (앞에 C : 취소) 
- Description : 제품명 (상품 특성 구분)
- StockCode : 상품 코드(각 제품에 고유하게 할당된 5자리 정수)
- Quantity : 품목 수량
- InvoiceDate(dd/mm/yyyy hh:mi) : 각 거래가 생성된 날짜와 시간
- unitPrice : 제품 단위 가격 (영국 단위)
- customerID : 고객 ID(5자리 정수)
- Country : 고객 거주 국가명

### 흐름

CSV 원본 데이터 → Python Producer → Kafka Topic → Consumer/Spark(또는 향후 처리) → 저장/분석

## 파이프라인 구성도

```
[Online Retail CSV]
        ↓
[Python Producer]
 - CSV row read
 - event_type 생성
 - JSON 변환
        ↓
[Kafka Broker]
 - retail-events topic
        ↓
[Consumer / Spark]
 - 이벤트 수신
 - 집계 / 정제
        ↓
[DB or File Storage]
        ↓
[Dashboard / Analysis]
```

![파이프라인 구성도](docs/online_retail_pipeline.png)


## Kafka 수집 설계

https://excalidraw.com/#json=5SDq4YuITu_4sUBd2y6Os,qb_f3KVSaG0Gi11H0w7zbg

![Kafka 수집 설계](docs/kafka-producer.png)

- 주문 / 취소 이벤트를 구분
- key는 `invoice_no`
- delivery callback으로 성공/실패 로그 기록
- invalid row는 skip 또는 dead-letter 후보로 관리
    - **“Kafka로 보내기 전에 메시지로 쓸 row가 정상인지 확인하는 단계”** 에 가깝고, 그 안에 **key 검사도 포함될 수 있다** 정도

## Configuration

- topic name: `retail-events`
- partition key : `invoice_no`
- partitions: `3`
- replication factor: `1` (학습용 로컬 환경)
- key: `invoice_no`
- value format: JSON
- acks: `all`
- retries: `3`

## 메세지 생성 방식

```
{
	'event_id': '536365-85123A', 
	'event_type': 'order', 
	'invoice_no': '536365', 
	'stock_code': '85123A', 
	'description': 'WHITE HANGING HEART T-LIGHT HOLDER', 
	'quantity': 6, 
	'unit_price': 2.55, 
	'customer_id': '17850.0', 
	'country': 'United Kingdom', 
	'invoice_timestamp': '12/1/2010 8:26', 
	'metadata': {
		'source': 'online_retail_csv', 
		'version': 'v1'
	}
}
```

## Error handling

### 유실 방지

- `acks='all'`
- `retries=3`
- `flush()`
- 종료 전 `flush()` / `close()`


### 잘못된 row 처리

- 필수값 누락 row는 skip
- parsing 실패 row는 에러 로그 기록 
- 별도 `error_rows.log` 또는 `dead_letter_rows.jsonl` 파일 저장 가능

(로우를 따로 저장하거나 로그를 받는 부분은 추후 고민 후 구성)

## DB 테이블 생성

``` sql
CREATE DATABASE retail_pipeline;

-- raw 데이터 저장
CREATE TABLE retail_events_raw (
    event_id VARCHAR(100),
    event_type VARCHAR(20),
    invoice_no VARCHAR(30),
    stock_code VARCHAR(30),
    description TEXT,
    quantity INT,
    unit_price NUMERIC(10,2),
    customer_id VARCHAR(30),
    country VARCHAR(100),
    invoice_timestamp TIMESTAMP,
    ingested_at TIMESTAMP,
    load_run_id VARCHAR(30)
);
```