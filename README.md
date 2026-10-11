# Notification Engine

A high-scale, production-ready notification system built with FastAPI, Kafka, Redis, and Postgres.

## Architecture

- **API (FastAPI):** Ingests events and applies Pre-Queue Rate Limiting.
- **Kafka (KRaft Mode):** Decouples ingestion from processing with P0/P1 priority topics.
- **Worker Pool:** Consumes events, applies Decider logic (Chain of Responsibility), Aggregates in Redis, and dispatches via Firebase/Twilio.
- **DLQ & Retry Topics:** Ensures guaranteed delivery without infinite loops.
- **Postgres:** Stores user preferences and notification history (async writes).

## Quick Start

1. Clone the repo
2. Copy `.env.example` to `.env`
3. Run `docker-compose up --build -d`
4. API is available at `http://localhost:8000/docs`

<img width="1600" height="1473" alt="image" src="https://github.com/user-attachments/assets/3441cb38-fe54-45fb-87dd-df9cfad2148d" />

