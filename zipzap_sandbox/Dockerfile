FROM python:3.11-slim

WORKDIR /app

# Install Flask for the web app and zipzap for sandbox execution.
RUN pip install --no-cache-dir flask zipzap

COPY . /app

ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=app.py
EXPOSE 5000

CMD ["python", "app.py"]
