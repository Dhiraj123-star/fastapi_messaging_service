# FastAPI Messaging Service

A lightweight FastAPI-based microservice that demonstrates asynchronous message communication using **Kafka** and **RabbitMQ**. This project showcases the integration of modern messaging systems into a Python web service, containerized using Docker and orchestrated with Docker Compose.

## 🔧 Core Features

- **FastAPI Application**  
  A simple FastAPI app that exposes HTTP endpoints to trigger messaging events and test integrations.

- **Kafka Producer Integration**  
  Sends JSON-formatted messages to a Kafka topic using the `kafka-python` library. Useful for stream-based messaging scenarios.

- **RabbitMQ Producer/Consumer Integration**  
  - Sends messages to a RabbitMQ queue.
  - Includes basic consuming logic to demonstrate retrieval and processing of queue messages.

- **Dockerized Architecture**  
  The entire stack runs via Docker Compose, including:
  - FastAPI app
  - Kafka + Zookeeper
  - RabbitMQ server

- **Environment Configuration**  
  Uses environment variables to manage Kafka and RabbitMQ connection settings, allowing flexible deployment across environments.

- **CI/CD with GitHub Actions**  
  Automates Docker image builds and pushes to Docker Hub on every push to the main branch.

## 📦 Stack Used

- **FastAPI** – Python web framework for API development  
- **Kafka** – Distributed event streaming platform  
- **RabbitMQ** – Message broker for reliable queuing  
- **Docker & Docker Compose** – Containerized microservices  
- **GitHub Actions** – CI/CD pipeline for Docker image deployment  

## 🚀 Purpose

This project serves as a foundational building block for:
- Learning and testing asynchronous message systems (Kafka, RabbitMQ)
- Building microservice-ready Python applications
- Setting up CI/CD for Docker-based services

---

