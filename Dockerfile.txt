# Start with a slim Python 3.9 base image
FROM python:3.11-slim-bullseye

# Install system packages, including ODBC Driver 18 for SQL Server
# (Adjust apt packages for your distro if needed)
RUN apt-get update && apt-get install -y curl gnupg2 apt-transport-https \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Set a working directory
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python script into the container
COPY script.py .

# Run the script when the container starts
CMD ["python", "script.py"]
