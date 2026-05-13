# Backfill Dag

backfill_retail_ingestion DAG를 수동 실행
→ target_start / target_end를 conf로 전달
→ 해당 구간만 재처리

```
backfill_retail_ingestion
   ↓
validate_backfill_params
   ↓
reset_target_range
   ↓
create_kafka_topic
   ↓
run_collector_for_range
   ↓
check_raw_count
```

### 장애대응용 DAG예시

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
    "owner": "sorae",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="backfill_retail_ingestion",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    reset_target_range = BashOperator(
        task_id="reset_target_range",
        bash_command=dedent("""
            docker exec postgres psql -v ON_ERROR_STOP=1 -U postgres -d retail_pipeline <<'SQL'
            DELETE FROM order_detail
            WHERE invoice_no IN (
                SELECT invoice_no
                FROM order_info
                WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start") }}'
                  AND invoice_timestamp <  '{{ dag_run.conf.get("target_end") }}'
            );

            DELETE FROM order_info
            WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start") }}'
              AND invoice_timestamp <  '{{ dag_run.conf.get("target_end") }}';

            DELETE FROM raw_retail_events
            WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start") }}'
              AND invoice_timestamp <  '{{ dag_run.conf.get("target_end") }}';
            SQL
        """)
    )

    create_kafka_topic = BashOperator(
        task_id="create_kafka_topic",
        bash_command=dedent("""
            docker exec kafka kafka-topics \
              --bootstrap-server kafka:29092 \
              --create \
              --if-not-exists \
              --topic retail-events \
              --partitions 1 \
              --replication-factor 1
        """)
    )

    run_collector_for_range = BashOperator(
        task_id="run_collector_for_range",
        bash_command=dedent("""
            docker compose run --rm \
              -e TARGET_START='{{ dag_run.conf.get("target_start") }}' \
              -e TARGET_END='{{ dag_run.conf.get("target_end") }}' \
              collector
        """)
    )

    check_raw_count = BashOperator(
        task_id="check_raw_count",
        bash_command=dedent("""
            docker exec postgres psql -U postgres -d retail_pipeline -c "
            SELECT COUNT(*) AS raw_count
            FROM raw_retail_events
            WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start") }}'
              AND invoice_timestamp <  '{{ dag_run.conf.get("target_end") }}';
            "
        """)
    )

    reset_target_range >> create_kafka_topic >> run_collector_for_range >> check_raw_count
```

#### 실행 전 실행 할 명령어
```bash
docker compose exec spark-master ps -ef | grep stream_raw_events
```

#### 실행 명령어
```bash
docker compose exec airflow airflow dags trigger backfill_retail_ingestion \  
--conf '{"target_start":"2025-12-10 00:00:00","target_end":"2025-12-11 00:00:00","producer_sleep":"0.1"}'
```

#### 진행 순서
1. 장애 구간 확인
2. backfill_retail_ingestion DAG 실행
3. target_start / target_end 지정
4. 해당 구간 raw/order 데이터 삭제
5. collector가 해당 구간 데이터를 Kafka로 재발행
6. Spark Streaming이 PostgreSQL에 재적재
7. check_raw_count로 적재 건수 확인
8. retail_pipeline DAG로 dim/mart 재생성

## 로드테스트 및 장애대응 개선 아이디어

본 프로젝트는 로컬 환경에서 Kafka, Spark, PostgreSQL, Airflow 기반 데이터 파이프라인을 구성하였으며, 운영 안정성을 높이기 위해 다음 항목을 개선 방향으로 고려할 수 있다.

1. Airflow Task 실패 시 Slack 알림 전송
2. Kafka Producer 전송 실패 및 timeout에 대한 retry 처리
3. Spark Streaming 프로세스 생존 여부 확인
4. target interval 기준 raw 적재 건수 검증
5. JSON parsing 실패 메시지의 failed table 또는 dead-letter topic 저장
6. Kafka offset과 PostgreSQL count 비교를 통한 누락 탐지
7. PostgreSQL insert 성능 및 lock 모니터링
8. 장애 구간에 대한 target_start/target_end 기반 backfill DAG 구성
9. 실행 명령어, 로그 위치, 복구 절차를 포함한 운영 runbook 작성
10. Producer 메시지 발행량 증가에 따른 Kafka/Spark/PostgreSQL 부하 테스트

핵심 테스트 1: Producer 부하 테스트
- Python으로 Kafka에 메시지를 많이 발행
- Kafka offset 증가 확인
- Spark Streaming이 DB에 적재하는지 확인
- PostgreSQL count 확인

핵심 장애 1: Spark/DB 적재 실패 또는 Kafka 연결 실패
- Kafka 브로커 중단 또는 Postgres 중단 시나리오 설명
- 실제 데모는 하나만 선택
- 대응 전략: retry, Slack 알림, backfill DAG/구간 재처리


# 6회차 과제: 로드 테스트 및 장애 대응

## 1. 현재 파이프라인 구조

본 프로젝트는 Online Retail CSV 데이터를 기반으로 Kafka, Spark Streaming, PostgreSQL, Airflow를 이용한 데이터 파이프라인을 구성하였다.

전체 흐름은 다음과 같다.

CSV 원본 데이터
→ Python Collector / Producer
→ Kafka topic: retail-events
→ Spark Streaming
→ PostgreSQL raw_retail_events
→ Airflow Batch DAG
→ dim / mart 테이블 생성

주요 구성 요소는 다음과 같다.

| 구성 요소 | 역할 |
|---|---|
| Collector / Producer | CSV 데이터를 읽어 Kafka 메시지로 발행 |
| Kafka | 이벤트 메시지 저장 및 스트리밍 전달 |
| Spark Streaming | Kafka 메시지를 읽어 PostgreSQL raw 테이블에 적재 |
| PostgreSQL | raw / dim / mart 데이터 저장 |
| Airflow | 배치 처리, backfill, 검증 작업 오케스트레이션 |
| Slack Alert | DAG 실패 또는 장애 발생 시 알림 전송 |

## 2. 부하 테스트 시나리오

### 2.1 테스트 목적

Producer가 짧은 시간 안에 대량의 메시지를 Kafka로 발행했을 때, Kafka, Spark Streaming, PostgreSQL이 정상적으로 데이터를 처리할 수 있는지 확인한다.

특히 다음 항목을 확인한다.

1. Kafka topic의 offset이 정상적으로 증가하는지
2. Spark Streaming이 Kafka 메시지를 계속 consume하는지
3. PostgreSQL raw_retail_events 테이블에 데이터가 정상 적재되는지
4. 발행 건수와 적재 건수 사이에 누락이 발생하는지
5. 처리 지연 또는 장애 발생 시 어떤 대응이 필요한지

### 2.2 테스트 방식

기존 Producer는 실시간 흐름을 흉내내기 위해 메시지 발행 사이에 sleep을 두었다.

로드 테스트에서는 sleep을 제거하거나 매우 짧게 설정하여 데이터를 빠르게 발행한다.

예시 조건은 다음과 같다.

| 항목          | 값                                                      |
| ----------- | ------------------------------------------------------ |
| Kafka topic | retail-events                                          |
| 테스트 구간      | 2026-01-20 00:00:00 ~ 2026-04-20 00:00:00              |
| 메시지 발행 방식   | sleep 없이 연속 발행                                         |
| 검증 대상       | Kafka offset, Spark Streaming 로그, PostgreSQL raw count |
| 기대 결과       | 발행 건수와 raw 적재 건수가 일치하거나, 차이가 발생할 경우 원인 확인              |

### 2.3 평상시 vs 피크 시나리오
본 프로젝트에서는 실시간 쇼핑몰 주문 이벤트 수집 상황을 가정한다.
평상시에는 주문 이벤트가 일정한 간격으로 발생하지만, 피크 상황에서는 짧은 시간 동안 주문 이벤트가 집중적으로 발생할 수 있다.
예를 들어 다음과 같은 상황을 피크 시나리오로 볼 수 있다.
1. 특정 시간대 주문량 증가
2. 할인 이벤트 또는 프로모션 기간
3. 배치성 데이터 재처리
4. 장애 복구 후 누락 구간 재발행
5. Producer가 sleep 없이 데이터를 연속 발행하는 경우테스트 기준은 다음과 같이 나누었다.

| 구분 | Producer 동작 방식 | 목적 |
|---|---|---|
| 평상시 | 메시지 발행 사이에 sleep 적용 | 일반적인 실시간 이벤트 흐름 재현 |
| 피크 시 | sleep 없이 연속 발행 | 짧은 시간 동안 대량 이벤트 유입 상황 재현 |
| 장애 복구 시 | 특정 target interval 재발행 | 누락 구간 backfill 상황 재현 |

평상시 실행 예시는 다음과 같다.

```bash
docker exec \
  -e TARGET_START='2025-12-10 00:00:00' \  
  -e TARGET_END='2025-12-11 00:00:00' \  
  -e PRODUCER_SLEEP='0.1' \  
  collector \  
  python /app/producer.py
```

피크 상황 실행 예시는 다음과 같다.

```
docker exec \
  -e TARGET_START='2025-12-10 00:00:00' \  
  -e TARGET_END='2025-12-11 00:00:00' \  
  -e PRODUCER_SLEEP='0' \  
  collector \  
  python /app/producer.py
```
#### producer의 sleep 환경변수 수정

```python
import os
import time

producer_sleep = float(os.getenv("PRODUCER_SLEEP", "0.1"))

for row in rows:
    producer.send(TOPIC, value=message)

    if producer_sleep > 0:
        time.sleep(producer_sleep)
```

### 2.4 Local 환경의 리소스 제약  
  
본 테스트는 로컬 Docker 환경에서 수행되므로 CPU, 메모리, 디스크 I/O, 컨테이너 리소스가 제한적이다.  
  
이로 인해 다음과 같은 제약이 존재한다.  
  
| 항목        | 제약 사항                                                       |     |
| --------- | ----------------------------------------------------------- | --- |
| CPU       | Kafka, Spark, PostgreSQL, Airflow가 하나의 로컬 머신 자원을 공유         |     |
| Memory    | 여러 컨테이너가 동시에 실행되어 메모리 부족 가능성 존재                             |     |
| Disk I/O  | Kafka log, Spark checkpoint, PostgreSQL write가 모두 로컬 디스크 사용 |     |
| Network   | 실제 분산 환경이 아닌 Docker 내부 네트워크 사용                              |     |
| Scale-out | Kafka broker, Spark worker, PostgreSQL이 단일 노드 중심으로 구성       |     |
  
따라서 본 테스트에서는 실제 운영 수준의 대규모 트래픽을 재현하기보다는, 로컬 환경에서 가능한 범위 내에서 다음 항목을 확인하는 데 초점을 두었다.  
  
1. Producer 발행량 증가 시 Kafka offset 변화  
2. Spark Streaming consume 여부  
3. PostgreSQL raw 적재 건수 변화  
4. 장애 발생 시 backfill DAG를 통한 복구 가능성  
5. 로컬 환경에서 발생할 수 있는 병목 지점 확인  
  
또한 로컬 환경에서는 sleep 제거만으로 뚜렷한 장애가 발생하지 않을 수 있으므로, 필요 시 target interval 확대, Producer 동시 실행, Spark Streaming 중단 등의 방식으로 부하 또는 장애 상황을 재현한다.
### 2.5 Kafka offset 확인

```
docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \  
--broker-list kafka:29092 \  
--topic retail-events \  
--time -1
```
- 시작 건수 : retail-events:0:146805 
- 종료 건수 : retail-events:0:250785

### 2.6 PostgreSQL 적재 건수 확인

```
docker compose exec postgres psql -U postgres -d retail_pipeline -c "SELECT COUNT(*) AS raw_count FROM raw_retail_events WHERE invoice_timestamp >= '2026-01-21 00:00:00'  AND invoice_timestamp <  '2026-04-20 00:00:00';"
```

 raw_count 
-----------
	97060
(적재되는데 약 15~20분)
### 2.7 부하 테스트 결과

이번 테스트에서 확인된 현상은 DB 적재 실패가 아니라 처리 지연에 가깝다.  
  
Producer는 Kafka에 빠르게 메시지를 발행할 수 있었지만, Spark Streaming은 Kafka에 쌓인 메시지를 순차적으로 consume한 뒤 PostgreSQL에 write하기 때문에 DB count는 Kafka offset보다 늦게 증가하였다.  
  
따라서 부하 상황에서는 다음 지표를 함께 확인해야 한다.

| 항목                   | 결과                                                    |
| -------------------- | ----------------------------------------------------- |
| 테스트 방식               | Producer sleep 제거                                     |
| Kafka offset         | 빠르게 증가                                                |
| PostgreSQL raw count | 즉시 반영되지 않고 점진적으로 증가                                   |
| 장애 여부                | 장애는 발생하지 않음                                           |
| 확인된 현상               | Kafka와 DB 적재 사이 처리 지연 발생                              |
| 원인 추정                | Spark Streaming consume/write 속도가 Producer 발행 속도보다 느림 |
| 대응 방향                | consumer lag 모니터링, raw count 검증, 지연 기준 초과 시 alert     |
	
## 3. 장애 시나리오 및 대응 전략

### 장애 시나리오 1. Spark Streaming 중단

#### 상황
Producer는 Kafka에 메시지를 정상 발행하지만 Spark Streaming이 중단되어 PostgreSQL raw 테이블에 데이터가 적재되지 않는 상황이다.
- Streaming DLQ
	: Kafka에서 읽었지만 parsing/write 실패한 메시지 저장

#### 확인 방법
- Kafka offset은 증가한다.
- Spark Streaming 프로세스가 존재하지 않는다.
- PostgreSQL raw count가 증가하지 않는다.
	→ Kafka consumer lag 확인  
	→ Spark Streaming 재기동  
	→ Kafka에 남은 메시지 이어서 처리

#### 대응 전략
1. Spark Streaming 프로세스 상태 확인
2. Spark 로그 확인
3. Spark Streaming 재기동
4. 장애 target interval 확인
5. backfill DAG 실행
6. raw count 재검증

```bash
docker compose exec airflow airflow dags trigger \  
-c '{"target_start":"2025-12-01 00:00:00","target_end":"2025-12-31 00:00:00"}' \ 
backfill_retail_pipeline
```
### 장애 시나리오 2. PostgreSQL write 지연 또는 실패

#### 상황
Kafka offset은 증가하고 Spark Streaming도 실행 중이지만, PostgreSQL raw count가 늦게 증가하거나 일정 시간 이상 증가하지 않는 상황이다.

#### 원인 후보
- PostgreSQL insert 성능 저하
- DB connection 문제
- table lock
- JDBC write 오류
- index 증가로 인한 insert 비용 증가

#### 대응 전략
1. DB count 증가 여부를 일정 간격으로 확인
2. Spark Streaming 로그에서 JDBC 오류 확인
3. PostgreSQL 로그 확인
4. 장시간 count가 증가하지 않으면 장애로 판단
5. 필요 시 Spark Streaming 재기동
6. target interval 기준 backfill 실행

#### 개선 방향
- batch insert 최적화
- raw table partitioning 검토
- 불필요한 index 최소화
- insert 실패 시 retry 적용
- 지연 기준 초과 시 Slack alert 발송

### 장애 시나리오 4. Producer 또는 Kafka 전송 실패

#### 상황
Producer가 Kafka에 메시지를 발행하지 못하는 상황이다.

예상 오류는 다음과 같다.

- KafkaTimeoutError
- NoBrokersAvailable
- Failed to update metadata
- Kafka broker connection refused

#### 확인 방법
- Kafka offset이 증가하지 않는다.
- Producer 로그에 전송 실패 오류가 발생한다.
- Kafka broker 상태를 확인한다.

#### 대응 전략
1. Producer retry 적용
2. Kafka broker 상태 확인
3. 전송 실패 메시지는 fallback JSONL 파일에 저장
4. Kafka 복구 후 fallback 파일 재전송
5. 반복 실패 시 Slack alert 발송

#### fallback Json 형태
![[Pasted image 20260505120504.png]]

```
{
	"failed_at": "2026-05-04T22:41:40.369649+09:00", 
	"error": "KafkaTimeoutError", 
	"error_message": "KafkaTimeoutError: Timeout after waiting for 10 secs.",
	"message": {
		"invoice_no": "540026", 
		"event_type": "order", 
		"message": {
			"tag": "in_process", 
			"event_id": "540026-22259", 
			"event_type": "order", 
			"invoice_no": "540026", 
			"stock_code": "22259", 
			"category": "ETC", 
			"description": "FELT FARM ANIMAL HEN", 
			"quantity": 6, 
			"unit_price": 0.85, 
			"customer_id": null, 
			"country": "United Kingdom", 
			"original_invoice_timestamp": "2011-01-04T13:25:00", 
			"invoice_timestamp": "2026-01-04T13:25:00", 
			"target_date": "2026-01-04", 
			"target_time": "13:25:00", 
			"metadata": {
				"source": "online_retail_csv", 
				"version": "v1"
			}
		}
	}
}
```

### 3.4 운영 보완점  
  
- target_start / target_end 검증  
- KST 기준 시간대 통일  
- raw count 검증 task 추가  
- Slack alert  
- Grafana/Prometheus 연동은 개선 방향
## 4. Backfill DAG 설계
### 4.1 Backfill이 필요한 이유
실시간 파이프라인에서는 Producer, Kafka, Spark Streaming, PostgreSQL 중 하나라도 장애가 발생하면 특정 시간 구간의 데이터가 누락될 수 있다.이때 전체 데이터를 처음부터 다시 처리하는 것은 비효율적이므로, 장애가 발생한 target interval만 다시 처리할 수 있는 backfill DAG가 필요하다. 
본 프로젝트에서는 `target_start`, `target_end`를 Airflow DAG conf로 전달하여 특정 구간만 재처리할 수 있도록 구성하였다.
### 4.2 DAG 구조
```text
backfill_retail_ingestion
   ↓
validate_backfill_params
   ↓
reset_target_range
   ↓
create_kafka_topic
   ↓
run_collector_for_range
   ↓
check_raw_count
```

### 4.3 처리 흐름

1. 사용자가 장애 구간을 확인한다.
2. Airflow에서 `backfill_retail_ingestion` DAG를 수동 실행한다.
3. DAG 실행 시 `target_start`, `target_end`를 conf로 전달한다.
4. 기존 raw/order 데이터를 해당 구간 기준으로 삭제한다.
5. Collector가 해당 구간 데이터를 Kafka로 재발행한다.
6. Spark Streaming이 Kafka 메시지를 다시 읽어 PostgreSQL에 적재한다.
7. `check_raw_count` task에서 적재 건수를 확인한다.
8. 이후 기존 `retail_pipeline` DAG를 실행하여 dim/mart 테이블을 재생성한다.

## 5. Fallback / Alert 전략  
  
### 5.1 Slack Alert  
  
Airflow task 실패 시 Slack으로 알림을 전송한다.  
  
알림 대상은 다음과 같다.  
  
1. backfill DAG 실패  
2. Producer 실행 실패  
3. raw count 검증 실패  
4. Spark Streaming 프로세스 미실행  
5. PostgreSQL 연결 실패  
  
예시 메시지:  
  
```text  
[Airflow Alert]  
DAG: backfill_retail_ingestion  
Task: run_collector_for_range  
Status: Failed  
Target Range: 2025-12-01 00:00:00 ~ 2025-12-02 00:00:00  
Action: Spark/Kafka/Postgres 상태 확인 후 backfill 재실행 필요
```

```python  
import requests  
  
def slack_fail_alert(context):  
dag_id = context["dag"].dag_id  
task_id = context["task_instance"].task_id  
execution_date = context["execution_date"]  
exception = context.get("exception")  
  
message = f"""  
[Airflow Task Failed]  
DAG: {dag_id}  
Task: {task_id}  
Execution Date: {execution_date}  
Error: {exception}  
"""  
  
requests.post(  
"https://hooks.slack.com/services/xxxxx/xxxxx/xxxxx",  
json={"text": message}  
)  
  
default_args = {  
"owner": "sorae",  
"retries": 1,  
"retry_delay": timedelta(seconds=10),  
"on_failure_callback": slack_fail_alert,  
}
```

### 5.2 Producer Fallback

Kafka 연결 실패 또는 메시지 전송 timeout이 발생하면, 전송 실패 메시지를 fallback JSONL 파일에 저장한다.

이후 Kafka가 복구되면 fallback 파일을 다시 읽어 재전송할 수 있다.

예시 형태:

```
{"event_id":"INV001-85123","invoice_no":"536365","stock_code":"85123A","error":"KafkaTimeoutError","failed_at":"2026-05-03T21:30:00"}{"event_id":"INV002-71053","invoice_no":"536366","stock_code":"71053","error":"KafkaTimeoutError","failed_at":"2026-05-03T21:30:01"}
```

### 5-2. Backfill DAG conf JSON

이건 fallback JSON 파일이 아니라 Airflow 실행 파라미터야.

```
docker compose exec airflow airflow dags trigger backfill_retail_ingestion \
  --conf '{"target_start":"2025-12-01 00:00:00","target_end":"2025-12-02 00:00:00"}'
```



```
Producer fallback JSONL은 실패 메시지를 보관하기 위한 파일이고, Airflow DAG conf JSON은 backfill 실행
```
## 6. 테스트 결과

## 7. 한계점 및 개선 방향

### 7.1 한계점

1. 로컬 Docker Compose 환경에서 테스트하였기 때문에 실제 운영 환경 수준의 부하를 재현하기 어렵다.
2. Kafka, Spark, PostgreSQL이 모두 단일 노드 구성이라 고가용성 검증은 제한적이다.
3. Spark Streaming 장애 감지는 수동 명령어 기반으로 확인하였다.
4. Kafka consumer lag을 정교하게 수집하지 못했다.
5. PostgreSQL insert 성능 병목을 정량적으로 분석하지는 못했다.
6. Grafana 기반 대시보드는 구축하지 않고 개선 방향으로만 제시하였다.
7. Airflow에서 docker compose 명령을 실행할 때 로컬 Docker 경로 및 권한 이슈가 발생할 수 있다.

### 7.2 개선 방향

1. Prometheus/Grafana를 연동하여 Kafka, Spark, PostgreSQL 지표를 시각화한다.
2. Kafka consumer lag을 주기적으로 수집하여 적재 지연을 탐지한다.
3. Spark Streaming 프로세스 상태를 Airflow sensor 또는 별도 health check로 감지한다.
4. Producer 실패 메시지는 dead-letter topic 또는 fallback JSONL 파일로 분리 저장한다.
5. PostgreSQL insert 성능 개선을 위해 batch insert, partitioning, index 최적화를 검토한다.
6. Airflow DAG 실패 시 Slack alert를 전송한다.
7. Backfill DAG 실행 시 raw count뿐 아니라 기대 건수와 실제 적재 건수를 비교하는 검증 task를 추가한다.



