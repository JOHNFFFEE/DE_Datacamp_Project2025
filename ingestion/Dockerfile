FROM python:3.10-slim

# Set working directory
WORKDIR /usr/app

# Copy the requirements file
COPY requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the files (ingest.py and others)
COPY . .

# Copy the config folder into the container (for credentials)
COPY ./config /usr/app/config

# Set environment variables for credentials (if needed)
ENV GOOGLE_APPLICATION_CREDENTIALS="/usr/app/config/service_account.json"

# Set the default command to run ingestion script
CMD ["python", "ingest.py"]
