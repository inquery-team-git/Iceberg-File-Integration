# Use an official Python runtime as a parent image
FROM python:3.9

#sudo apt-get update && sudo apt-get install -y vim
RUN apt-get update && apt-get install -y vim

RUN mkdir -p /root/.m2/repository && \
    chmod -R 777 /root/.m2
RUN apt-get update && \
    apt-get install -y default-jdk maven && \
    rm -rf /var/lib/apt/lists/*


ENV AWS_REGION=us-east-2
ENV AWS_SECRET_ACCESS_KEY=XXX
ENV AWS_ACCESS_KEY_ID=XXX

# Install Poetry
# You can specify the version of poetry you want to install
# Here, we avoid creating a virtual environment inside the Docker container
RUN pip install --no-cache-dir poetry
RUN poetry config virtualenvs.create false

# Set the working directory in the container
WORKDIR /app

# Copy only the files needed for installing dependencies to avoid cache busting
COPY pyproject.toml poetry.lock* /app/

# Install project dependencies
RUN poetry install --no-dev --no-interaction --no-ansi

# Copy the rest of your application code
COPY . /app

# Clean and compile the java code
RUN mvn clean && mvn compile

# Make port 80 available to the world outside this container
EXPOSE 80

# Run main.py when the container launches

CMD ["python", "-m", "http.server", "80"]

