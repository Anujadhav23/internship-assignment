FROM python:3.10-slim

# Install system deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tzdata && \
    rm -rf /var/lib/apt/lists/*

# Set timezone
ENV TZ=Asia/Kolkata

# Create app directory
WORKDIR /app

# Copy project files
COPY src/ src/
COPY data/ data/
COPY docs/ docs/
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create output folder inside container
RUN mkdir -p output

# Run both scripts automatically
CMD ["bash", "-c", "python src/data_profiling.py && python src/main_pipeline.py"]
