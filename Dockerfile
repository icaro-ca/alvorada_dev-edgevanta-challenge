FROM apache/airflow:2.9.1-python3.11

# Install Python dependencies as the airflow user (required by official image)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
