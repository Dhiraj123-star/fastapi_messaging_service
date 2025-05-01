# Dockerfile
# Use the official Python image as a base image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install netcat-openbsd to allow the wait-for-it script to function
RUN apt-get update && apt-get install -y netcat-openbsd

# Install the required Python packages
RUN pip install -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose port 8000 for the FastAPI app
EXPOSE 8000

# Set the default command to run your application with Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
