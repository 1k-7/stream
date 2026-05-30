# Use a lightweight Python base image
FROM python:3.10-slim

# Prevent Python from writing pyc files to disk and ensure logs are output immediately
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Create the downloads directory
RUN mkdir -p /app/downloads

# Expose the port the FastAPI server runs on
EXPOSE 8000

# Command to start the application
CMD ["python", "main.py"]
