import os
import json
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "retail-events")
FALLBACK_PATH = os.getenv("FALLBACK_PATH", "/app/fallback/failed_messages.jsonl")

def extract_kafka_message(record: dict):
    """
    fallback JSONL 한 줄에서 실제 Kafka로 재발행할 message를 추출

    지원구조: {"message": {"message: {"key": ..., "value": ...}}} 형태로 KafkaProducer에서 직렬화된 메시지 구조
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