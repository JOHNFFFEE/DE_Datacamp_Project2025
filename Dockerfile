# Start with a Python 3.10 slim image
FROM python:3.10-slim

# Set working directory (modify this to a valid path for your setup)
WORKDIR /app

# Copy dependency file
COPY requirements.txt . 

# Install requirements
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files (make sure to copy everything)
COPY . .

# Set environment variables for Google Cloud credentials
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/config/service_account.json"

# Install Prefect (if it's not already installed via requirements)
RUN pip install prefect

# Set the default command to run the ingestion flow
CMD ["python", "main_flow.py"]
