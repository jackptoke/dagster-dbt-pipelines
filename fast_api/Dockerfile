FROM python:3.12.9
LABEL authors="jacktoke"

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV APP_HOME /root
WORKDIR $APP_HOME
COPY . $APP_HOME/app

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]