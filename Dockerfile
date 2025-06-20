# syntax=docker/dockerfile:1.7

# Creating a common build which sets up the images
FROM python:3.13.5-slim-bookworm AS common_build_env

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Docker builder

FROM common_build_env AS builder


COPY --from=common_build_env /usr/local /usr/local
WORKDIR /app
COPY . .
RUN mkdir /app/lookups_data

CMD ["bash", "-c", "python3 build_local_taxa_tables.py ; python3 generate_lookups.py ; cp *.duckdb *.sqlite lookups_data/."]

# Build the lookups
FROM common_build_env AS build_lookups

RUN mkdir -p /app/lookups_data && \
    python3 build_local_taxa_tables.py /app/lookups_data && \
    python3 generate_lookups.py /app/lookups_data

# Create a server which has everything in there
FROM python:3.13.5-slim-bookworm AS server_build_lookups

COPY --from=common_build_env /usr/local /usr/local
WORKDIR /app
COPY . .
COPY --from=build_lookups /app/lookups_data /app/lookups_data

EXPOSE 80

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]

# Use pre-built local lookups
FROM python:3.13.5-slim-bookworm AS server_prebuilt_lookups

COPY --from=common_build_env /usr/local /usr/local
WORKDIR /app
COPY . . 

# Expose the port
EXPOSE 80

# Define the command to run your application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
