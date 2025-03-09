FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y protobuf-compiler

# Set up working directory
WORKDIR /app
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Start the app
CMD ["gunicorn", "app:app"]
