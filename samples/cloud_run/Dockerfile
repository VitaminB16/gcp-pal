FROM python:3.10-slim

RUN apt-get update

# Allows docker to cache installed dependencies between builds
WORKDIR /tmp

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sets the working directory in the container
WORKDIR /app

# Copies the local code to the container
COPY . .

CMD ["python", "main.py"]
