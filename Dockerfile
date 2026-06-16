FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY aks_diagnose.py .
USER 1000
ENTRYPOINT ["python", "aks_diagnose.py"]
