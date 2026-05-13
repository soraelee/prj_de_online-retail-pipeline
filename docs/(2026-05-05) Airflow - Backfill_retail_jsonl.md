
### 1. setup DAG
- 처음 환경 준비용, 수동 실행
- schedule=None
```
create_kafka_topic
↓
start_stream_if_not_alive
↓
check_stream_alive
```

### 2. hourly ingestion DAG

매시간 실행.

```
check_stream_alive
↓
run_collector
↓
wait_and_check_raw_count
```

#### 설명
hourly_retail_ingestion DAG

- 목적: 매 시간마다 target interval 데이터를 Collector를 통해 Kafka로 발행하고,

	Spark Streaming이 raw_retail_events에 적재하는지 확인한다.

- 예시: 01:00에 실행되는 DAG는 00:00부터 01:00까지의 데이터를 처리 대상으로 한다.

- order_info/order_detail, dim, mart 생성은 별도 batch DAG 또는 backfill DAG에서 수행한다.
### 3. backfill DAG

수동 복구용.

```
reset_target_range
↓
check_stream_alive
↓
run_collector_for_range
↓
wait_and_check_raw_count
↓
build_dim
↓
build_mart
↓
check_agg_count
```


## 최종 수정 내용

#### setup_retail_pipeline
- 수동 실행
- 테스트 구간 초기화
- checkpoint 초기화
- Kafka topic 생성
- Spark Streaming 실행 확인

#### hourly_retail_ingestion
- 매시간 실행
- Spark Streaming 살아있는지 확인
- collector 실행
- raw 적재 count 확인

#### backfill_retail_pipeline
- 수동 실행
- 장애 구간 재처리
- raw 재적재
- dim overwrite
- mart 재생성
- 집계 count 확인

#### backfill_retail_jsonl
- 매일 12:30 실행 예정
- fallback JSONL 있으면 Kafka replay
- raw/dim/mart 복구
##### JSONL 읽는 함수 예시
replay_fallback_jsonl.py
```python
import os
import json
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "retail-events")
FALLBACK_PATH = os.getenv("FALLBACK_PATH", "/app/fallback/processing/failed_messages.jsonl")


def extract_kafka_message(record: dict):
    """
    fallback JSONL 한 줄에서 실제 Kafka로 재발행할 message를 추출한다.

    지원 구조:
    1) {"message": {"message": {...}}}
    2) {"message": {...}}
    3) {...}
    """

    wrapper = record.get("message", record)

    if isinstance(wrapper, dict) and isinstance(wrapper.get("message"), dict):
        message = wrapper["message"]
        key = wrapper.get("invoice_no") or message.get("invoice_no")
        return key, message

    if isinstance(wrapper, dict):
        message = wrapper
        key = message.get("invoice_no")
        return key, message

    raise ValueError(f"Invalid message structure: {record}")


def iter_jsonl_messages(path: str):
    valid_count = 0
    invalid_count = 0
    affected_dates = set()

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                record = json.loads(line)
                key, message = extract_kafka_message(record)

                if not isinstance(message, dict):
                    raise ValueError("message is not dict")

                if not message.get("event_id"):
                    print(f"[WARN] line={line_no}, event_id is missing")

                invoice_timestamp = message.get("invoice_timestamp")
                if invoice_timestamp:
                    affected_dates.add(invoice_timestamp[:10])

                valid_count += 1

                yield {
                    "line_no": line_no,
                    "key": key,
                    "message": message,
                    "record": record,
                }

            except Exception as e:
                invalid_count += 1
                print(f"[INVALID JSONL] line={line_no}, error={e}, raw={line[:300]}")

    print(f"[JSONL READ DONE] valid_count={valid_count}, invalid_count={invalid_count}")
    print(f"[AFFECTED DATES] {sorted(affected_dates)}")


def main():
    if not os.path.exists(FALLBACK_PATH):
        print(f"[SKIP] fallback file not found: {FALLBACK_PATH}")
        return

    if os.path.getsize(FALLBACK_PATH) == 0:
        print(f"[SKIP] fallback file is empty: {FALLBACK_PATH}")
        return

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    sent_count = 0
    failed_count = 0

    try:
        for item in iter_jsonl_messages(FALLBACK_PATH):
            try:
                future = producer.send(
                    TOPIC,
                    key=item["key"],
                    value=item["message"],
                )

                metadata = future.get(timeout=10)

                print(
                    f"[REPLAY SENT] line={item['line_no']}, "
                    f"topic={metadata.topic}, "
                    f"partition={metadata.partition}, "
                    f"offset={metadata.offset}, "
                    f"event_id={item['message'].get('event_id')}, "
                    f"invoice_no={item['message'].get('invoice_no')}"
                )

                sent_count += 1

            except Exception as e:
                failed_count += 1
                print(
                    f"[REPLAY FAILED] line={item['line_no']}, "
                    f"event_id={item['message'].get('event_id')}, "
                    f"error={e}"
                )

        producer.flush(timeout=10)

    finally:
        producer.close(timeout=10)

    print(f"[REPLAY RESULT] sent_count={sent_count}, failed_count={failed_count}")

    if failed_count > 0:
        raise RuntimeError(f"Replay failed. failed_count={failed_count}")


if __name__ == "__main__":
    main()
```

##### backfill_retail_jsonl

DAG에서 읽기 전 파일 이동
: DAG에서는 먼저 pending 파일을 processing으로 옮긴다
```python
prepare_jsonl_file = BashOperator(
    task_id="prepare_jsonl_file",
    bash_command=dedent("""
        docker exec collector bash -lc '
        mkdir -p /app/fallback/pending /app/fallback/processing /app/fallback/processed /app/fallback/error

        if [ ! -s /app/fallback/pending/failed_messages.jsonl ]; then
          echo "[SKIP] no fallback jsonl file"
          exit 99
        fi

        TS=$(date +%Y%m%d_%H%M%S)
        mv /app/fallback/pending/failed_messages.jsonl /app/fallback/processing/failed_messages_${TS}.jsonl

        echo "/app/fallback/processing/failed_messages_${TS}.jsonl" > /app/fallback/processing/latest_processing_file.txt
        cat /app/fallback/processing/latest_processing_file.txt
        '
    """)
)
```

replay 실행 task
```python
replay_jsonl_to_kafka = BashOperator(
    task_id="replay_jsonl_to_kafka",
    bash_command=dedent("""
        docker exec collector bash -lc '
        FALLBACK_PATH=$(cat /app/fallback/processing/latest_processing_file.txt)

        echo "[REPLAY FILE] $FALLBACK_PATH"

        FALLBACK_PATH=$FALLBACK_PATH \
        KAFKA_TOPIC=retail-events \
        KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
        python /app/replay_fallback_jsonl.py
        '
    """)
)

```

성공/실패 후 파일 이동
- 성공 시 `processed`
```python
archive_processed_jsonl = BashOperator(
    task_id="archive_processed_jsonl",
    bash_command=dedent("""
        docker exec collector bash -lc '
        FALLBACK_PATH=$(cat /app/fallback/processing/latest_processing_file.txt)
        FILE_NAME=$(basename $FALLBACK_PATH)

        mv $FALLBACK_PATH /app/fallback/processed/$FILE_NAME
        rm -f /app/fallback/processing/latest_processing_file.txt

        echo "[ARCHIVED] /app/fallback/processed/$FILE_NAME"
        '
    """)
)
```

- 실패 시 error로 이동
#### 전체 구상
```
backfill_retail_jsonl
   ↓
prepare_jsonl_file
   ↓
create_kafka_topic
   ↓
check_spark_streaming
   ↓
replay_jsonl_to_kafka
   ↓
check_raw_count
   ↓
build_dim
   ↓
build_mart
   ↓
check_agg_count
   ↓
archive_processed_jsonl
```