import json
import pandas as pd
from kafka import KafkaProducer
import time

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

    for _, row in df.iterrows():
        map = {}

        map['invoice_no'] = str(row["InvoiceNo"])
        map['event_type'] = 'cancel' if map['invoice_no'].startswith('C') else "order"

        category = 'ETC' if row["StockCode"].isdigit() else str(row["StockCode"])[-1];
        
        map['message'] = {
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

        message.append(map);
    
    return message;       


def main():
    # kafka producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        acks='all',
        retries=3,
        value_serializer=json_serializer,
        key_serializer=lambda k: k.encode('utf-8')
    )

    try : 
        data = get_csv_message();

        for msg in data:

            #메세지 전송 (비동기)
            future = producer.send("retail-events", key=msg['invoice_no'], value=msg['message'])

            print(f'retail message : {msg["event_type"]} - {msg["message"]} \n');
            time.sleep(0.5)
    
    except KeyboardInterrupt:
        print("프로듀서 종료")
    finally:
        # 남은 메시지 전송 및 리소스 해제
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main() 
