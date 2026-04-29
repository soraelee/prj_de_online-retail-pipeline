import os
import json
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import subprocess
from datetime import timedelta

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
def create_producer():
    for attempt in range(1, 31):
        try:
            print(f"[Kafka] 연결 시도 {attempt}/30 - {BOOTSTRAP_SERVERS}")

            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None
            )

        except NoBrokersAvailable:
            print("[Kafka] 아직 브로커 연결 불가. 3초 후 재시도")
            time.sleep(3)

    raise RuntimeError("Kafka 브로커 연결 실패")

def json_serializer(data) :
    return json.dumps(data).encode('utf-8')

def get_csv_message():
    # 데이터 불러오기
    df = pd.read_csv("data/online_retail.csv")

    # Airflow에서 전달받는 처리 구간
    target_start = os.getenv("TARGET_START")
    target_end = os.getenv("TARGET_END")

    # 날짜 변환
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df = df.dropna(subset=["InvoiceDate"])

    # 2026.04.29 - 스케줄용 timestamp 추가
    # 2010 -> 2025, 2011 -> 2026
    df["targetDate"] = df["InvoiceDate"] + pd.DateOffset(years=15)

    # target interval 기준 필터링
    if target_start and target_end:
        start_ts = pd.to_datetime(target_start).tz_localize(None)
        end_ts = pd.to_datetime(target_end).tz_localize(None)

        df = df[
            (df["targetDate"] >= start_ts) &
            (df["targetDate"] < end_ts)
        ]

    # 날짜 시간순 오름차순 정렬
    df = df.sort_values(
        by=["InvoiceDate", "InvoiceNo"],
        ascending=[True, True],
        kind="mergesort"
    ).reset_index(drop=True)

    messages = []

    prev_invoice_no = None

    for _, row in df.iterrows():
        current_invoice_no = str(row["InvoiceNo"])

        # 주문번호가 바뀌는 순간, 이전 주문 완료 메시지 추가
        if prev_invoice_no is not None and prev_invoice_no != current_invoice_no:
            messages.append({
                "invoice_no": prev_invoice_no,
                "event_type": "cancel" if prev_invoice_no.startswith("C") else "order",
                "message": {
                    "tag": "complete",
                    "invoice_no": prev_invoice_no
                }
            })

        stock_code = str(row["StockCode"])
        category = "ETC" if stock_code.isdigit() else stock_code[-1]
        normalized_stock_code = stock_code if category == "ETC" else stock_code[:-1]

        event_type = "cancel" if current_invoice_no.startswith("C") else "order"

        msg = {
            "invoice_no": current_invoice_no,
            "event_type": event_type,
            "message": {
                "tag": "in_process",
                "event_id": f"{current_invoice_no}-{stock_code}",
                "event_type": event_type,
                "invoice_no": current_invoice_no,
                "stock_code": normalized_stock_code,
                "category": category,
                "description": None if pd.isna(row["Description"]) else str(row["Description"]),
                "quantity": int(row["Quantity"]),
                "unit_price": float(row["UnitPrice"]),
                "customer_id": None if pd.isna(row["CustomerID"]) else str(int(row["CustomerID"])),
                "country": str(row["Country"]),

                # 원본 timestamp
                "original_invoice_timestamp": row["InvoiceDate"].isoformat(),

                # Airflow 스케줄 기준으로 변환한 timestamp
                "invoice_timestamp": row["targetDate"].isoformat(),
                "target_date": row["targetDate"].strftime("%Y-%m-%d"),
                "target_time": row["targetDate"].strftime("%H:%M:%S"),

                "metadata": {
                    "source": "online_retail_csv",
                    "version": "v1"
                }
            }
        }

        messages.append(msg)

        prev_invoice_no = current_invoice_no

    # 마지막 주문 complete 메시지 추가
    if prev_invoice_no is not None:
        messages.append({
            "invoice_no": prev_invoice_no,
            "event_type": "cancel" if prev_invoice_no.startswith("C") else "order",
            "message": {
                "tag": "complete",
                "invoice_no": prev_invoice_no
            }
        })

    return messages

# def trigger_airflow(start, end):
#     subprocess.run([
#         "airflow", "dags", "trigger", "retail_pipeline",
#         "--conf", json.dumps({"start": start, "end": end})
#     ], check=True)


def main():
    # kafka producer
    producer = create_producer();

    try : 
        data = get_csv_message();

        print(f"[Producer] message count: {len(data)}")

        for msg in data:
            message = msg["message"]
            invoice_no = msg.get("invoice_no")
            event_type = msg.get("event_type")

            future = producer.send(
                "retail-events",
                key=invoice_no,
                value=message
            )

            metadata = future.get(timeout=10)

            print(
                f"[Kafka Sent] topic={metadata.topic}, "
                f"partition={metadata.partition}, "
                f"offset={metadata.offset}, "
                f"event_type={event_type}, "
                f"invoice_no={invoice_no}, "
                f"message={message}"
            )

            time.sleep(0.1)

        producer.flush()
        print("[Producer] all messages flushed")
    
    except KeyboardInterrupt:
        print("프로듀서 종료")
    finally:
        # 남은 메시지 전송 및 리소스 해제
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main() 
