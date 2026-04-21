FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY app.py storage.py map_builder.py data_download.py fetcher.py ./

# HF Spaces expects the app to listen on port 7860
EXPOSE 7860

CMD ["python", "app.py"]
