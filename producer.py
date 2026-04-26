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
    df = pd.read_csv('data/online_retail.csv')

    # 날짜를 시간순 오름차순 처리하기
    # 날짜 시간 변환 실패 시 제외
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df = df.dropna(subset=["InvoiceDate"]).sort_values(by=["InvoiceDate", "InvoiceNo"], ascending=[True, True], kind="mergesort").reset_index(drop=True)

    message = []

    # 26.04.25 invoice_no 변경 시. 주문 완료 태그 전달
    invoice_no = '';

    for _, row in df.iterrows():
        msg = {}

        msg['invoice_no'] = invoice_no
        msg['event_type'] = 'cancel' if msg['invoice_no'].startswith('C') else "order"

        if invoice_no != str(row["InvoiceNo"]) :
            msg['message'] = {'tag':'complete', 'invoice_no': invoice_no}
            
            invoice_no = str(row["InvoiceNo"])

        else :
            
            category = 'ETC' if row["StockCode"].isdigit() else str(row["StockCode"])[-1];
            
            msg['message'] = {
                'tag' : 'in_process',
                "event_id": f"{row['InvoiceNo']}-{row['StockCode']}",
                "event_type": "cancel" if str(row["InvoiceNo"]).startswith("C") else "order",
                "invoice_no": str(row["InvoiceNo"]),
                "stock_code": str(row["StockCode"]) if category == 'ETC' else str(row["StockCode"])[:-1],
                "category": category,
                "description": None if pd.isna(row["Description"]) else str(row["Description"]),
                "quantity": int(row["Quantity"]),
                "unit_price": float(row["UnitPrice"]),
                "customer_id": None if pd.isna(row["CustomerID"]) else str(int(row["CustomerID"])),
                "country": str(row["Country"]),
                "invoice_timestamp": str(row["InvoiceDate"]),
                "metadata": {
                    "source": "online_retail_csv",
                    "version": "v1"
                }
            }

        message.append(msg);
    
    return message;       


def trigger_airflow(start, end):
    subprocess.run([
        "airflow", "dags", "trigger", "retail_pipeline",
        "--conf", json.dumps({"start": start, "end": end})
    ], check=True)


def main():
    # kafka producer
    producer = create_producer();

    try : 
        data = get_csv_message();

        prev_date = None
        for msg in data:
            message = msg['message']
            if 'invoice_timestamp' in message:
                current_date = pd.to_datetime(message['invoice_timestamp']).date()
                if prev_date and current_date != prev_date:
                    trigger_airflow(
                        f"{prev_date.isoformat()}T00:00:00",
                        f"{current_date.isoformat()}T00:00:00"
                    )
                prev_date = current_date

            #메세지 전송 (비동기)
            future = producer.send("retail-events", key=msg['invoice_no'], value=message)

            print(f'retail message : {msg["event_type"]} - {message} \n');
            time.sleep(0.5)

        if prev_date is not None:
            trigger_airflow(
                f"{prev_date.isoformat()}T00:00:00",
                f"{(prev_date + timedelta(days=1)).isoformat()}T00:00:00"
            )
    
    except KeyboardInterrupt:
        print("프로듀서 종료")
    finally:
        # 남은 메시지 전송 및 리소스 해제
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main() 
