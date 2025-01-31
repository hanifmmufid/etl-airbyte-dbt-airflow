FROM apache/airflow:2.9.3

# Salin file requirements.txt
COPY requirements.txt /requirements.txt

# Perbarui pip ke versi terbaru
RUN pip install --upgrade pip

# Instal dependensi dari requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt --use-deprecated=legacy-resolver

