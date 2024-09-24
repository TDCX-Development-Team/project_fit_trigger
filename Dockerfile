# Use an official Python runtime as a base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy your local project files to the container
COPY . /app

# Install dependencies from the requirements.txt file
RUN pip install --no-cache-dir -r requirements.txt

# Install PySpark
RUN pip install pyspark==3.2.0

# Install the Google Cloud Functions Framework for local testing
RUN pip install functions-framework

# Expose port 8080 to allow for local testing
EXPOSE 8080

# Set the entry point for your function (process_file in your case)
ENTRYPOINT ["functions-framework", "--target", "process_file"]
