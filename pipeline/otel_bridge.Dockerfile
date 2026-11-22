# OTel 브리지(pipeline/otel_bridge.py) 실행용 이미지.
# 프로젝트 코드는 /opt/skill-eval에 마운트(PYTHONPATH)하고 여기서는 의존성만 넣는다.
FROM python:3.12-slim

RUN pip install --no-cache-dir "confluent-kafka>=2.4" "pydantic>=2" "pyyaml>=6" "python-dotenv>=1"

RUN useradd --system --uid 10001 bridge
USER bridge
WORKDIR /opt/skill-eval
ENV PYTHONPATH=/opt/skill-eval PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
CMD ["python", "-m", "pipeline.otel_bridge"]
