
# Airflow 실행 및 Collector 연동 오류 해결 기록

## 1. 개요

Airflow DAG에서 `collector`를 실행하여 Kafka Producer를 구동하는 과정에서 여러 오류가 발생하였다.  
주요 원인은 Airflow 컨테이너 내부에서 Docker 명령을 실행할 때의 환경 차이, Docker Compose 버전 문제, Mac Docker Desktop의 volume mount 제한, 그리고 컨테이너 실행 방식의 복잡성이었다.

최종적으로는 `docker compose run collector` 방식 대신, **항상 실행 중인 collector 컨테이너에 `docker exec`로 producer.py를 실행하는 방식**으로 변경하여 문제를 해결하였다.

---

## 2. 발생한 문제 요약

| 단계 | 발생 오류 | 원인 | 해결 |
|---|---|---|---|
| `create_kafka_topic` | Bash command failed | Kafka CLI 접속 주소 또는 Kafka 준비 상태 문제 | Kafka 컨테이너 내부에서는 `localhost:9092` 사용 |
| `run_collector` | `docker: 'compose' is not a docker command` | Airflow 컨테이너에 Docker Compose plugin 없음 | Dockerfile.airflow에 Docker Compose 설치 |
| `run_collector` | `Unsupported config option for services` | 구버전 docker-compose가 Compose spec을 제대로 해석하지 못함 | compose version / Docker Compose v2 확인 |
| `docker-compose config` | Duplicate mount points | 동일한 dags volume이 중복 선언됨 | 중복 volume 제거 |
| `docker-compose run` | client version too old | apt로 설치된 docker-compose v1이 Docker Desktop API와 호환되지 않음 | Docker Compose v2 사용 |
| `docker compose run` | mounts denied: `/opt/retail-pipeline` not shared | Airflow 컨테이너 내부 경로를 Docker Desktop이 Mac host 경로로 인식하지 못함 | `docker compose run` 포기, `docker exec collector` 방식으로 변경 |
| Airflow task | `up_for_retry` 반복 | `run_collector` task 실패로 Airflow retry 발생 | collector 실행 방식을 `docker exec`로 변경 |

---

## 3. 상세 오류 및 해결 과정

## 3-1. Airflow 내부에서 `docker compose run collector` 실패

### 오류

```text
docker: 'compose' is not a docker command.
```

### 원인

Airflow 컨테이너 내부에는 `docker` CLI는 설치되어 있었지만, `docker compose` plugin이 설치되어 있지 않았다.  
따라서 DAG의 `BashOperator`에서 아래 명령을 실행할 수 없었다.

```
docker compose run --rm --no-deps collector
```

### 조치

`Dockerfile.airflow`에 Docker 관련 패키지를 설치하였다.

```
FROM apache/airflow:2.7.1USER rootRUN apt-get update && \    apt-get install -y --no-install-recommends docker.io docker-compose && \    apt-get clean && \    rm -rf /var/lib/apt/lists/*COPY requirements.airflow.txt /tmp/requirements.airflow.txtUSER airflowRUN pip install --no-cache-dir -r /tmp/requirements.airflow.txt
```

하지만 이후 `docker-compose` v1 버전 호환 문제가 발생하였다.

---

## 3-2. 구버전 docker-compose 호환 문제

### 오류

```
ERROR: client version 1.30 is too old. Minimum supported API version is 1.40
```

### 원인

`apt-get install docker-compose`로 설치된 docker-compose는 v1 계열이며, 현재 Docker Desktop의 Docker Engine API와 호환되지 않았다.

### 판단

Airflow 컨테이너 내부에서 Docker Compose를 실행하는 방식은 다음 문제를 계속 발생시켰다.

```
1. Docker Compose plugin 설치 문제2. Compose spec version 호환 문제3. Mac Docker Desktop volume mount 문제4. 컨테이너 내부 경로와 host 경로 불일치
```

따라서 `docker compose run` 방식 대신 다른 접근이 필요했다.

---

## 3-3. Mac Docker Desktop volume mount 문제

### 오류

```
Error response from daemon: mounts denied:The path /opt/retail-pipeline is not shared from the host and is not known to Docker.
```

### 원인

Airflow 컨테이너 안에서는 프로젝트 경로가 `/opt/retail-pipeline`으로 보인다.

하지만 실제 Docker daemon은 Mac host에서 실행되고 있기 때문에, 새 컨테이너를 만들 때 `/opt/retail-pipeline`이라는 host 경로를 찾을 수 없다.

즉 다음 구조가 문제가 되었다.

```
Airflow 컨테이너 내부 경로: /opt/retail-pipelineDocker Desktop host 기준: 해당 경로 없음
```

`docker compose run collector`는 새 컨테이너를 생성하면서 volume mount를 다시 수행하기 때문에, 이 경로 문제로 실패하였다.

---

## 4. 최종 해결 방식

## 4-1. collector 컨테이너를 대기 상태로 실행

`collector` 컨테이너는 producer를 자동 실행하지 않고, 항상 켜진 상태로 유지하도록 변경하였다.

```
collector:  build:    context: .    dockerfile: Dockerfile.collector  container_name: collector  depends_on:    - kafka  environment:    KAFKA_BOOTSTRAP_SERVERS: kafka:29092  volumes:    - ./:/app  command: ["tail", "-f", "/dev/null"]  networks:    - kafka-network
```

### 변경 이유

기존 방식:

```
Airflow → docker compose run collector
```

수정 방식:

```
Airflow → docker exec collector python /app/producer.py
```

`docker exec`는 이미 존재하는 컨테이너 안에서 명령만 실행하므로, 새 컨테이너 생성이나 volume mount 문제가 발생하지 않는다.

---

## 4-2. Airflow DAG의 run_collector 수정

기존 방식:

```
run_collector = BashOperator(    task_id="run_collector",    bash_command="""    cd /opt/retail-pipeline && docker compose run --rm --no-deps \      -e TARGET_START='{{ dag_run.conf.get("target_start", data_interval_start.strftime("%Y-%m-%d %H:%M:%S")) }}' \      -e TARGET_END='{{ dag_run.conf.get("target_end", data_interval_end.strftime("%Y-%m-%d %H:%M:%S")) }}' \      collector    """)
```

수정 방식:

```
run_collector = BashOperator(    task_id="run_collector",    bash_command="""    docker exec \      -e TARGET_START='{{ dag_run.conf.get("target_start", data_interval_start.strftime("%Y-%m-%d %H:%M:%S")) }}' \      -e TARGET_END='{{ dag_run.conf.get("target_end", data_interval_end.strftime("%Y-%m-%d %H:%M:%S")) }}' \      collector \      python /app/producer.py    """)
```

### 장점

```
1. docker compose plugin이 필요 없음2. 새 컨테이너 생성이 필요 없음3. Mac file sharing 문제가 발생하지 않음4. 기존 collector 컨테이너의 volume mount를 그대로 사용5. TARGET_START, TARGET_END를 실행 시점마다 전달 가능
```

---

## 5. Kafka topic 생성 오류 대응

## 5-1. 기존 문제

`create_kafka_topic` task에서 다음 명령이 실패하였다.

```
create_kafka_topic = BashOperator(    task_id="create_kafka_topic",    bash_command="""        docker exec kafka kafka-topics \        --bootstrap-server kafka:29092 \        --create \        --if-not-exists \        --topic retail-events \        --partitions 1 \        --replication-factor 1    """)
```

### 원인

`docker exec kafka ...`는 Kafka 컨테이너 내부에서 실행되는 명령이다.  
Kafka 컨테이너 내부에서는 `kafka:29092`보다 `localhost:9092`를 사용하는 것이 더 안정적이다.

## 5-2. 수정 방식

```
create_kafka_topic = BashOperator(    task_id="create_kafka_topic",    bash_command="""    docker exec kafka bash -lc '    kafka-topics \      --bootstrap-server localhost:9092 \      --create \      --if-not-exists \      --topic retail-events \      --partitions 1 \      --replication-factor 1    '    """)
```

---

## 6. Airflow task retry 현상

### 현상

`run_collector` task에서 `up_for_retry` 상태가 반복되었다.

### 원인

Airflow의 retry 설정 때문에 task가 실패하면 자동으로 재시도되었다.

```
default_args = {    "owner": "sorae",    "retries": 3,    "retry_delay": timedelta(seconds=10),}
```

실제 원인은 producer 코드가 아니라, `run_collector`에서 실행하던 Docker 명령 실패였다.

### 정리

```
run_collector 실패→ Airflow가 retries 설정에 따라 재시도→ up_for_retry 반복
```

`docker exec collector python /app/producer.py` 방식으로 변경한 뒤 해당 문제를 해결하였다.

---

## 7. 한국시간 / UTC 이슈

Airflow의 DAG Run, `data_interval_start`, `data_interval_end`는 기본적으로 UTC 기준으로 보일 수 있다.

현재 프로젝트는 `TARGET_START`, `TARGET_END`를 명시적으로 넘기는 방식으로 처리 구간을 지정하므로, UI에 표시되는 시간이 한국시간과 다르게 보여도 실제 처리 구간은 전달한 파라미터 기준으로 동작한다.

### 문서화 기준

```
Airflow 내부 실행 시간은 UTC 기준으로 관리될 수 있으며,본 프로젝트에서는 DAG 실행 시 TARGET_START, TARGET_END를 명시적으로 전달하여 처리 구간을 제어한다.
```

향후 개선 시 Airflow timezone을 `Asia/Seoul`로 설정할 수 있다.

예시:

```
AIRFLOW__CORE__DEFAULT_TIMEZONE: Asia/Seoul
```

---

## 8. 최종 실행 흐름

최종적으로 `setup_retail_pipeline` DAG는 다음 흐름으로 실행된다.

```
reset_tables   ↓create_kafka_topic   ↓start_stream_raw_events   ↓check_stream_alive   ↓run_collector   ↓check_raw_count
```

`run_collector`는 collector 컨테이너 내부에서 producer를 실행한다.

```
Airflow BashOperator   ↓docker exec collector   ↓TARGET_START / TARGET_END 전달   ↓python /app/producer.py   ↓Kafka retail-events topic으로 메시지 발행
```

---

## 9. 확인 명령어

## collector 컨테이너 상태 확인

```
docker compose ps collector
```

## collector 내부 producer 수동 실행

```
docker compose exec airflow-scheduler bash -lc "docker exec \  -e TARGET_START='2025-12-02 00:00:00' \  -e TARGET_END='2025-12-02 01:00:00' \  collector \  python /app/producer.py"
```

## Kafka topic 확인

```
docker exec kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --list"
```

## Kafka topic 상세 확인

```
docker exec kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --describe --topic retail-events"
```

## Spark Streaming 로그 확인

```
docker compose exec spark-master tail -200 /tmp/stream_raw_events.log
```

## Spark Streaming 프로세스 확인

```
docker compose exec spark-master ps -ef | grep stream_raw_events
```

## Raw 적재 건수 확인

```
docker exec postgres psql -U postgres -d retail_pipeline -c "SELECT COUNT(*)FROM raw_retail_events;"
```

## 특정 구간 적재 확인

```
docker exec postgres psql -U postgres -d retail_pipeline -c "SELECT COUNT(*)FROM raw_retail_eventsWHERE invoice_timestamp >= '2025-12-02 00:00:00'  AND invoice_timestamp <  '2025-12-02 01:00:00';"
```

---

## 10. 최종 정리

이번 오류의 핵심은 Airflow DAG 문제가 아니라, Airflow 컨테이너 내부에서 Docker 명령을 실행할 때 발생한 환경 차이였다.

처음에는 `docker compose run collector` 방식으로 collector를 실행하려 했지만, 다음 문제들이 연속적으로 발생하였다.

```
1. Airflow 컨테이너에 Docker Compose plugin 없음2. docker-compose v1 버전 호환 문제3. compose volume 중복 선언4. Mac Docker Desktop volume mount 경로 문제
```

따라서 최종적으로는 collector 컨테이너를 항상 실행 상태로 두고, Airflow에서 `docker exec`를 통해 producer를 실행하는 방식으로 변경하였다.

이 방식은 새 컨테이너를 생성하지 않기 때문에 Docker Compose 버전 문제와 Mac volume mount 문제를 피할 수 있으며, Airflow DAG에서 실행 시점마다 `TARGET_START`, `TARGET_END`를 전달할 수 있다.