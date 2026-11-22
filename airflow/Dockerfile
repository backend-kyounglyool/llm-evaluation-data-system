# skill_eval_elt DAG 실행용 Airflow 이미지. 프로젝트 코드는 /opt/skill-eval에 마운트(PYTHONPATH)하고
# 여기서는 DAG 태스크가 import하는 의존성만 더한다.
FROM apache/airflow:2.10.5-python3.12

RUN pip install --no-cache-dir \
      duckdb python-dotenv "snowflake-connector-python[pandas]" \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
