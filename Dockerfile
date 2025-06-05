FROM python:3.12-slim
LABEL authors="jacktoke"

ENV PYTHONUNBUFFERED=1

ENV DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR $DAGSTER_HOME

RUN pip install --upgrade pip

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY dagster.yaml workspace.yaml .
COPY dagster_university ./dagster_university
COPY analytics ./analytics
COPY data ./data
COPY pyproject.toml .
COPY setup.cfg .
COPY setup.py .
COPY .env .

EXPOSE 3000

CMD ["dagster-webserver", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]