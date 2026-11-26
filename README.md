# Patient Management System

A microservices-based patient management backend built with **Spring Boot**. Clients talk to a single **API Gateway**, which validates JWTs against a dedicated **Auth Service** before routing requests to the **Patient Service**. When a patient is created, the Patient Service registers a billing account over **gRPC** and publishes a **Protobuf** event to **Kafka**, which the **Analytics Service** consumes. The whole stack runs locally with Docker Compose and can be deployed to AWS (ECS Fargate, RDS, MSK, ALB) with an AWS CDK stack written in Java.

> The project is a learning/portfolio system. Some parts are deliberately minimal stand-ins (see [Implementation Status](#implementation-status-and-known-limitations)): the Billing Service returns a fixed response and the Analytics Service only logs events.

## Table of Contents

1. [Key Features](#key-features)
2. [Technology Stack](#technology-stack)
3. [System Architecture](#system-architecture)
4. [Project Structure](#project-structure)
5. [Prerequisites](#prerequisites)
6. [Getting Started](#getting-started)
7. [Configuration and Environment Variables](#configuration-and-environment-variables)
8. [Database Setup](#database-setup)
9. [Running the Application](#running-the-application)
10. [API Documentation](#api-documentation)
11. [Authentication and Authorization](#authentication-and-authorization)
12. [Inter-Service Communication](#inter-service-communication)
13. [Docker and Containerization](#docker-and-containerization)
14. [AWS Deployment (CDK)](#aws-deployment-cdk)
15. [Testing](#testing)
16. [Example Workflows](#example-workflows)
17. [Implementation Status and Known Limitations](#implementation-status-and-known-limitations)
18. [Troubleshooting](#troubleshooting)
19. [Future Improvements](#future-improvements)
20. [Author and License](#author-and-license)

---

## Key Features

- **Single entry point.** Spring Cloud Gateway (WebFlux) routes all client traffic and applies a custom `JwtValidation` filter to the patient routes.
- **JWT authentication.** The Auth Service verifies BCrypt-hashed credentials and issues HMAC-signed tokens with a 1-hour lifetime. The gateway validates every protected request by calling the Auth Service's `/validate` endpoint.
- **Patient CRUD** with Bean Validation, unique-email enforcement and a centralised exception handler.
- **Transactional outbox.** `PATIENT_CREATED` events are written to an `outbox_event` table in the same transaction as the patient row, then published to Kafka by a polling relay (`FOR UPDATE SKIP LOCKED`, broker-acknowledged sends). Delivery is at-least-once.
- **Asynchronous billing with retries.** New patients start as `PENDING`. A scheduled worker calls the Billing Service over gRPC (per-call deadline), moving the patient to `ACTIVE` or `FAILED`, with exponential backoff and a retry cap.
- **Idempotent Kafka consumer** with retry/backoff and a dead-letter topic (`PATIENT.DLT`).
- **Protobuf everywhere on the wire between services** (gRPC for billing, Protobuf bytes for Kafka messages).
- **OpenAPI docs** (springdoc) for the Auth and Patient services, exposed through the gateway.
- **Infrastructure as code.** An AWS CDK (Java) stack provisions VPC, RDS PostgreSQL, MSK, ECS Fargate, Cloud Map service discovery, ALB, ECR repositories, CloudWatch log groups and a Secrets Manager secret for the JWT key.
- **REST Assured integration tests** that exercise the stack through the gateway.

## Technology Stack

| Area | Technology |
|---|---|
| Language / runtime | Java 21 (services), Java 23 (`infrastructure` and `integration-tests` modules) |
| Framework | Spring Boot 4.x (gateway/auth/analytics 4.1.0, patient 4.0.3, billing 4.0.5), Spring Cloud 2025.1.2 (gateway only) |
| API gateway | Spring Cloud Gateway Server WebFlux, `WebClient` |
| Persistence | Spring Data JPA / Hibernate, PostgreSQL 17 (H2 is on the classpath of auth and patient services) |
| Security | Spring Security (`BCryptPasswordEncoder`), JJWT 0.12.6 |
| Messaging | Apache Kafka (KRaft mode in Docker, Amazon MSK on AWS), Spring for Apache Kafka |
| RPC | gRPC 1.75.0 via `net.devh:grpc-spring-boot-starter` 3.1.0.RELEASE |
| Serialization | Protocol Buffers (`protobuf-java` 4.29.1, `protoc` 3.25.5 through `protobuf-maven-plugin`) |
| API docs | springdoc-openapi 3.0.2 (auth, patient) |
| Build | Maven (per-service Maven Wrapper), multi-stage Docker builds |
| Containers | Docker, Docker Compose |
| Cloud / IaC | AWS CDK 2.262.2 (Java): VPC, RDS, MSK, ECS Fargate, Cloud Map, ALB, ECR, CloudWatch, Secrets Manager |
| Testing | Spring Boot Test (`contextLoads` smoke tests), REST Assured 6.0.1, JUnit Jupiter 6.1.2 |
| Utilities | Lombok 1.18.42 |

## System Architecture

Each service is an independent Spring Boot application. There is no parent/aggregator POM; every directory has its own `pom.xml`.

<img width="1586" height="890" alt="Screenshot (1796)" src="https://github.com/user-attachments/assets/14cbd452-d1f7-42d9-a1a2-91b2aec80f55" />

---
**Components**

| Component | Responsibility |
|---|---|
| `api-gateway` | Routing, `StripPrefix`/`RewritePath` filters, `JwtValidation` filter (calls the Auth Service). Also routes the OpenAPI docs and `/swagger-ui.html`. |
| `auth-service` | `POST /login` (credential check, token issue) and `GET /validate` (signature and expiry check). Owns the `users` table. |
| `patient-service` | Patient CRUD, outbox writer + relay, billing retry worker, gRPC client, Kafka producer. Owns the `patient` and `outbox_event` tables. |
| `billing-service` | gRPC server implementing `BillingService.CreateBillingAccount`. Currently a stub (no persistence). |
| `analytics-service` | Kafka consumer for `PATIENT` events with dedupe, retry and dead-letter handling. Business logic is a placeholder. |
| `infrastructure` | AWS CDK stack (`PatientManagementStack`). |
| `integration-tests` | REST Assured tests that call the running stack at `http://localhost:4004`. |

**Startup and deployment.** `docker-compose.yaml` builds all five service images from their own folders, starts two PostgreSQL 17 containers and one Kafka container, and orders startup with health checks (`depends_on: condition: service_healthy` for the databases and Kafka). Only the gateway (4004), both databases (5000, 5001), Kafka's external listener (9094) and billing's gRPC port (9001) are published to the host.

## Project Structure

```text
patient-management/
├── api-gateway/            # Spring Cloud Gateway (WebFlux), JwtValidation filter
│   └── src/main/resources/ # application.yml (Docker routes), application-prod.yml (AWS/Cloud Map routes)
├── auth-service/           # Login + token validation, users table, JWT utility
│   └── src/main/resources/ # application.properties, data.sql (seed user)
├── patient-service/        # Patient CRUD, outbox, billing worker, gRPC client, Kafka producer
│   └── src/main/
│       ├── java/.../       # controller, service, repository, entity, dto, mapping, exception,
│       │                   # billing (retry worker), kafka (outbox relay), grpc (client), config
│       ├── proto/          # billing-service.proto, patient-event.proto
│       └── resources/      # application.properties, data.sql (seed patients)
├── billing-service/        # gRPC server (stub)
│   └── src/main/proto/     # billing-service.proto
├── analytics-service/      # Kafka consumer, error handling, dedupe store
│   └── src/main/proto/     # patient-event.proto
├── infrastructure/         # AWS CDK (Java): PatientManagementStack, cdk.json
├── integration-tests/      # REST Assured tests (LoginApiIntegrationTest, PatientApiIntegrationTest)
├── rest-requests/          # IntelliJ HTTP Client files for auth and patient endpoints
├── grpc-requests/          # IntelliJ HTTP Client file for the billing gRPC call
└── docker-compose.yaml     # Local stack: 2x PostgreSQL, Kafka, 5 services
```

The two `.proto` copies are not shared through a module. `billing-service.proto` exists in both `billing-service` and `patient-service`; `patient-event.proto` exists in both `patient-service` and `analytics-service`. **Keep the message and service definitions in each pair identical** (only comments differ today) whenever you change a contract.

## Prerequisites

| Use case | Requirements |
|---|---|
| Run the full stack | Docker Engine with the Compose plugin (`docker compose`), `openssl` (to generate a JWT secret) |
| Build/run a single service outside Docker | JDK 21, PostgreSQL and Kafka reachable from your machine (you can start them from Compose, see below) |
| Run integration tests | JDK 23, Maven 3.9+, the stack running at `localhost:4004` |
| Deploy to AWS | JDK 23, Maven, Node.js + AWS CDK CLI (`npm i -g aws-cdk`), AWS credentials, Docker (to push images) |

Each service directory contains `mvnw`/`mvnw.cmd`, so a system-wide Maven is not required for the services themselves.

## Getting Started

```bash
git clone <your-repository-url>
cd patient-management

# 1. Create the git-ignored .env file used by docker-compose.yaml
cat > .env <<EOF
DB_USER=<choose-a-db-user>
DB_PASSWORD=<choose-a-db-password>
JWT_SECRET=$(openssl rand -base64 32)
EOF

# 2. Build and start everything
docker compose up --build
```

When the logs settle, the gateway is available at `http://localhost:4004`. Continue with [Example Workflows](#example-workflows) to log in and create a patient.

## Configuration and Environment Variables

### Compose-level variables (`.env`)

`docker compose` reads `.env` from the project root. The file is git-ignored.

| Variable | Used by | Notes |
|---|---|---|
| `DB_USER` | both PostgreSQL containers and the auth/patient services | Postgres user |
| `DB_PASSWORD` | same | Postgres password |
| `JWT_SECRET` | auth-service | **Must be valid Base64** and decode to **at least 32 bytes**. `JwtUtil` Base64-decodes it and passes the bytes to `Keys.hmacShaKeyFor`. `openssl rand -base64 32` works. |

### Service-level variables set by Compose

| Service | Variable | Purpose |
|---|---|---|
| auth-service | `SPRING_DATASOURCE_URL`, `SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD` | PostgreSQL connection |
| auth-service | `SPRING_JPA_HIBERNATE_DDL_AUTO=update` | Hibernate creates/updates tables |
| auth-service | `SPRING_SQL_INIT_MODE=always` | Runs `data.sql` against the non-embedded database |
| auth-service | `JWT_SECRET` | Bound to property `jwt.secret` (no default; startup fails without it) |
| patient-service | `SPRING_DATASOURCE_*`, `SPRING_JPA_HIBERNATE_DDL_AUTO`, `SPRING_SQL_INIT_MODE` | As above |
| patient-service | `SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092` | Kafka producer and topic admin |
| patient-service | `BILLING_SERVICE_ADDRESS=billing-service`, `BILLING_SERVICE_GRPC_PORT=9001` | gRPC target (defaults: `localhost`, `9001`) |
| analytics-service | `SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092` | Kafka consumer/producer |
| api-gateway | `AUTH_SERVICE_URL=http://auth-service:4005` | Bound to `auth.service.url`, used by the JWT filter. **Required**: there is no default. |

### Optional tuning properties (patient-service)

These have defaults in code and can be overridden as properties or environment variables (e.g. `BILLING_RETRY_MAX_ATTEMPTS`).

| Property | Default | Meaning |
|---|---|---|
| `billing.service.timeout-ms` | `3000` | gRPC deadline per call |
| `billing.retry.max-attempts` | `10` | Attempts before a patient is marked `FAILED` |
| `billing.retry.poll-ms` | `2000` | Delay between billing worker runs |
| `outbox.relay.delay-ms` | `1000` | Delay between outbox relay runs |

### Fixed ports

| Service | Port(s) |
|---|---|
| api-gateway | 4004 |
| auth-service | 4005 |
| patient-service | 4000 |
| billing-service | 4001 (HTTP, no endpoints defined), 9001 (gRPC) |
| analytics-service | 4002 |

### Gateway profiles

- Default profile (`application.yml`): routes to Docker Compose hostnames `auth-service:4005` and `patient-service:4000`.
- `prod` profile (`application-prod.yml`, activated by `SPRING_PROFILES_ACTIVE=prod` in the CDK stack): routes to Cloud Map names `*.patient-management.local`. See the [known issue](#implementation-status-and-known-limitations) about its patient path.

## Database Setup

There is nothing to run manually with Docker Compose: each service has its own PostgreSQL 17 container (database name `db`), and schemas are created at startup.

| Container | Host port | Used by |
|---|---|---|
| `auth-service-db` | 5001 | auth-service |
| `patient-service-db` | 5000 | patient-service |

**How the schema is created.** Both services use `spring.jpa.hibernate.ddl-auto=update` (set via environment variable) together with `spring.sql.init.mode=always`. Spring Boot runs `data.sql` **before** Hibernate's schema update, so each `data.sql` begins with `CREATE TABLE IF NOT EXISTS` and inserts seed rows guarded by `WHERE NOT EXISTS`. Hibernate `update` then adds columns that `data.sql` did not define (for patients: `billing_status`, `billing_account_id`, `billing_attempts`, `billing_next_attempt_at`) and creates `outbox_event`. Because `billing_status` is declared with a column default of `'ACTIVE'`, the pre-seeded patients become `ACTIVE`, while patients created through the API start as `PENDING` (set in Java).

**Seed data**

- `auth-service`: one user, `testuser@test.com` with role `ADMIN`. The BCrypt hash in `data.sql` corresponds to the password `password123`. This is demo data; do not use it outside local development.
- `patient-service`: 15 sample patients (`John Doe`, `Jane Smith`, ...).

`USERS` lives in the auth database; `PATIENT` and `OUTBOX_EVENT` live in the patient database. There are no foreign keys between them.

**Connecting with a DB client:** host `localhost`, port `5000` (patient) or `5001` (auth), database `db`, user/password from your `.env`.

**Wiping data:** `docker compose down -v` removes the `auth-service-db-data` and `patient-service-db-data` volumes.

## Running the Application

### Everything with Docker Compose (recommended)

```bash
docker compose up --build      # build images and start all containers
docker compose up --build -d   # same, detached
docker compose logs -f patient-service
docker compose down            # stop (add -v to also delete DB volumes)
```

Startup order enforced by Compose: databases and Kafka become healthy, then `auth-service`, `billing-service`, `patient-service,` and `analytics-service` start, then `api-gateway`.

### Individual services outside Docker (optional)

Start only the infrastructure from Compose, then run a service from its directory with the Maven Wrapper:

```bash
docker compose up -d auth-service-db patient-service-db kafka billing-service
```

```bash
# auth-service (http://localhost:4005)
cd auth-service
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5001/db \
SPRING_DATASOURCE_USERNAME=<DB_USER> SPRING_DATASOURCE_PASSWORD=<DB_PASSWORD> \
SPRING_JPA_HIBERNATE_DDL_AUTO=update SPRING_SQL_INIT_MODE=always \
JWT_SECRET=<base64-secret> \
./mvnw spring-boot:run
```

```bash
# patient-service (http://localhost:4000) - billing-service is published on localhost:9001 by Compose
cd patient-service
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5000/db \
SPRING_DATASOURCE_USERNAME=<DB_USER> SPRING_DATASOURCE_PASSWORD=<DB_PASSWORD> \
SPRING_JPA_HIBERNATE_DDL_AUTO=update SPRING_SQL_INIT_MODE=always \
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
./mvnw spring-boot:run
```

```bash
# analytics-service (http://localhost:4002)
cd analytics-service
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9094 ./mvnw spring-boot:run
```

The **API Gateway is meant to run inside the Compose network**: its default routes use the hostnames `auth-service` and `patient-service`. When you run services directly on the host, call them on their own ports (the Patient Service performs no authentication itself).

## API Documentation

All client calls go through the gateway at `http://localhost:4004`.

| Method | Gateway path | Forwarded to | JWT required | Description |
|---|---|---|---|---|
| POST | `/api-gateway/auth/login` | auth-service `POST /login` | No | Exchange credentials for a token |
| GET | `/api-gateway/auth/validate` | auth-service `GET /validate` | No (the token is the input) | `200` if the Bearer token is valid, else `401` |
| GET | `/api-gateway/patients` | patient-service `GET /patients` | **Yes** | List patients |
| POST | `/api-gateway/patients` | patient-service `POST /patients` | **Yes** | Create a patient |
| PUT | `/api-gateway/patients/{id}` | patient-service `PUT /patients/{id}` | **Yes** | Update a patient |
| DELETE | `/api-gateway/patients/{id}` | patient-service `DELETE /patients/{id}` | **Yes** | Delete a patient |
| GET | `/api-docs/auth` | auth-service `/v3/api-docs` | No | OpenAPI JSON |
| GET | `/api-docs/patients` | patient-service `/v3/api-docs` | No | OpenAPI JSON |
| GET | `/swagger-ui.html` | patient-service `/swagger-ui.html` | No | Swagger UI entry path (only this exact path is routed) |

### `POST /api-gateway/auth/login`

```bash
curl -s -X POST http://localhost:4004/api-gateway/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"testuser@test.com","password":"password123"}'
```

```json
{ "token": "<jwt>" }
```

Wrong email or password returns `401` with an empty body. The request DTO declares `@NotBlank`, `@Email` and a minimum password length of 8.

### `GET /api-gateway/patients`

```bash
curl -s http://localhost:4004/api-gateway/patients -H "Authorization: Bearer <jwt>"
```

```json
[
  {
    "id": "123e4567-e89b-12d3-a456-426614174000",
    "name": "John Doe",
    "email": "john.doe@example.com",
    "address": "123 Main St, Springfield",
    "dateOfBirth": "1985-06-15",
    "billingStatus": "ACTIVE"
  }
]
```

### `POST /api-gateway/patients`

```bash
curl -s -X POST http://localhost:4004/api-gateway/patients \
  -H "Authorization: Bearer <jwt>" -H "Content-Type: application/json" \
  -d '{"name":"Jane Roe","email":"jane.roe@example.com","address":"1 Example Street","dateOfBirth":"1990-01-31"}'
```

```json
{
  "id": "2eb0cc6c-9d8f-4376-bc8b-6567e73a413e",
  "name": "Jane Roe",
  "email": "jane.roe@example.com",
  "address": "1 Example Street",
  "dateOfBirth": "1990-01-31"
}
```

Request constraints: `name` required, max 100 characters; `email` required and well-formed; `address` required; `dateOfBirth` required, format `yyyy-MM-dd`. The patient is returned as `PENDING`; it becomes `ACTIVE` shortly afterwards once the billing worker succeeds.

### `PUT /api-gateway/patients/{id}`

Same body as create. Updates name, address, email and date of birth. It does not emit a Kafka event and does not touch billing fields.

### `DELETE /api-gateway/patients/{id}`

Returns `200` with an empty body. Deleting an unknown id also returns `200` (the service calls `deleteById` without an existence check).

### Error responses (patient-service)

All handled errors are `400` with a flat JSON map:

| Cause | Example body |
|---|---|
| Bean Validation failure | `{"name":"name is required"}` (one entry per invalid field) |
| Duplicate email (create or update) | `{"email":"A patient with the email jane.roe@example.com already exists"}` |
| Unknown patient on update | `{"id":"Patient couldn't find with the id <uuid>"}` (returned as `400`, not `404`) |

Any other exception falls through to Spring Boot's default `500` error body.

### Missing or invalid token

On protected routes the gateway returns `401` immediately if the `Authorization` header is missing or does not start with `Bearer `. For a present token it calls the Auth Service; see [Known Limitations](#implementation-status-and-known-limitations) for the invalid-token path.

The `rest-requests/` and `grpc-requests/` folders contain ready-made IntelliJ HTTP Client requests; `login-user.http` stores the token in the `token` variable used by the others.

## Authentication and Authorization

What is implemented:

- **Authentication** in `auth-service`: `AuthService` looks the user up by email, compares with `BCryptPasswordEncoder.matches`, and `JwtUtil` issues a token carrying `sub` (email), a `role` claim, `iat`, and `exp` one hour later, signed with the Base64-decoded `JWT_SECRET`. JJWT picks the HMAC-SHA algorithm from the key size (a 32-byte key gives HS256).
- **Token validation**: `GET /validate` parses and verifies the signature and expiry. Any `JwtException` yields `401`.
- **Gateway enforcement**: the `JwtValidation` filter (`JwtValidationGatewayFilterFactory`) is attached only to the patient route. It rejects requests without a `Bearer` header, then calls the Auth Service with a reactive `WebClient` before forwarding.
- **Auth Service HTTP security**: `SecurityConfig` permits all requests and disables CSRF. Access control happens at the gateway; the services themselves trust their network.

What is **not** implemented: role-based authorization (the `role` claim is issued but never checked), user registration, refresh tokens, and token revocation. The only account is the seeded demo user.

## Inter-Service Communication

| From → To        | Protocol | Contract |
|---|---|---|
| Client → Gateway | HTTP/REST | Paths in [API Documentation](#api-documentation) |
| Gateway → Auth | HTTP (WebClient) | `GET /validate` with the original `Authorization` header |
| Gateway → Auth / Patient | HTTP (proxy) | Spring Cloud Gateway routes |
| Patient → Billing | gRPC (plaintext, blocking stub) | `BillingService.CreateBillingAccount(BillingRequest{patientId,name,email}) → BillingResponse{accountId,status}` |
| Patient → Kafka → Analytics | Kafka, Protobuf bytes | Topic `PATIENT`, key = patient id, value = `patient.events.PatientEvent{patientId,name,email,event_type,event_id}`; consumer group `analytics-service` |

### Billing state machine

- **Retryable gRPC codes:** `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED`, `INTERNAL`, `UNKNOWN`. Any other code marks the patient `FAILED` immediately.
- **Backoff** after the *n*-th failed attempt: `min(5 × 2^n, 300)` seconds, i.e. 10 s, 20 s, 40 s, 80 s, 160 s, then 300 s. After `billing.retry.max-attempts` (default 10) the patient is `FAILED`. On a retryable failure the worker stops processing the rest of its batch.
- **Patient creation never depends on Billing being up**; it only records `PENDING`.

### Kafka consumer behaviour (analytics-service)

- `KafkaConsumer` receives `byte[]`, parses `PatientEvent`, and skips events whose non-empty `event_id` was already processed (`InMemoryProcessedEventStore`, bounded to 10,000 ids, lost on restart).
- Failures are retried by `DefaultErrorHandler` with exponential backoff (1 s, 2 s, 4 s, 8 s; 4 retries), then published to `PATIENT.DLT`. `InvalidProtocolBufferException` is not retried and goes straight to the DLT.
- Topics are declared as `NewTopic` beans (`PATIENT` in patient-service, `PATIENT.DLT` in analytics-service), because the Compose Kafka broker has `auto.create.topics.enable=false`.

## Docker and Containerization

Each service has a two-stage `Dockerfile`: a `maven:3.9.9-eclipse-temurin-21` builder (copies `pom.xml`, runs `mvn dependency:go-offline`, then copies `src` and packages) and an `eclipse-temurin:21-jdk` runtime stage that runs `java -jar app.jar`.

Notes:

- The build context is the service folder, so build from the repository root with Compose or `docker build ./<service>`.
- `auth-service` and `api-gateway` build with `-DskipTests`. `billing-service`, `patient-service` and `analytics-service` run `mvn clean package` **including tests**.
- The Compose Kafka image is `bitnamilegacy/kafka:4.0.0-debian-12-r10`, single node, KRaft mode. Containers use `kafka:9092`; the host uses `localhost:9094`.

```bash
docker compose build                    # build all images
docker compose build patient-service    # build one image
docker compose up -d                    # start
docker compose ps                       # status and health
```

## AWS Deployment (CDK)

`infrastructure/src/main/java/com/patient/management/stack/PatientManagementStack.java` defines a single stack named `patientmanagementstack`.

What the stack creates:

- VPC with 2 AZs and a single NAT gateway; a shared ECS security group (self-referencing allow-all) and a database security group allowing 5432 from ECS.
- Two RDS PostgreSQL 17.2 `db.t3.micro` instances (20 GB, encrypted, generated credentials for user `admin_user`, `RemovalPolicy.DESTROY`).
- An MSK cluster (Kafka 3.9.x, 2 × `kafka.t3.small`) with TLS client traffic; bootstrap brokers are fetched by an `AwsCustomResource`. Services receive `SPRING_KAFKA_BOOTSTRAP_SERVERS` and `SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SSL`.
- An ECS cluster with the Cloud Map namespace `patient-management.local`; Fargate services (256 CPU / 512 MiB) for auth (4005), billing (4001, 9001), analytics (4002) and patient (4000) with CloudWatch log groups (`/ecs/<service>`, 1 week retention).
- A Secrets Manager secret `patient-management/jwt-secret` (44 generated characters, no punctuation) injected as `JWT_SECRET`, and each database password injected as `SPRING_DATASOURCE_PASSWORD`.
- The API Gateway as an `ApplicationLoadBalancedFargateService` (profile `prod`, `AUTH_SERVICE_URL=http://auth-service.patient-management.local:4005`).
- One ECR repository per service, with image tag `latest`.

Deploy outline (the repository contains no deployment script; these are the steps the stack implies):

```bash
cd infrastructure
cdk bootstrap          # once per account/region; the stack uses the default synthesizer
cdk deploy
```

The stack creates empty ECR repositories and ECS pulls `<repo>:latest`, so **build and push each service image** to the matching repository (`auth-service`, `billing-service`, `analytics-service`, `patient-service`, `api-gateway`) for the tasks to start. Tear everything down with `cdk destroy`; databases and ECR repositories are configured to be deleted. NAT gateway, MSK and RDS are billable resources.

## Testing

| Layer | Location | What it covers | How to run |
|---|---|---|---|
| Spring context smoke tests | `*/src/test/java/.../*ApplicationTests.java` | One `@SpringBootTest` `contextLoads()` per service | `./mvnw test` in each service folder |
| API integration tests | `integration-tests/` | Login success/failure and a patients call through the gateway | `cd integration-tests && mvn test` (stack running, JDK 23) |

Notes on the smoke tests: `auth-service` needs `jwt.secret` and `api-gateway` needs `auth.service.url` (neither has a default), which is why their Dockerfiles skip tests. Provide them when running locally, for example:

```bash
cd auth-service    && JWT_SECRET=$(openssl rand -base64 32) ./mvnw test
cd api-gateway     && AUTH_SERVICE_URL=http://localhost:4005 ./mvnw test
```

Integration test status:

- `LoginApiIntegrationTest` posts valid credentials (expects `200` and a token) and invalid credentials (expects `401`) to `/api-gateway/auth/login`.
- `PatientApiIntegrationTest` logs in and calls `GET /api-gateway/patient` (singular). That path predates the rename to `/api-gateway/patients` (commit `056ddd7`), and its assertion on a `patients` key predates the endpoint returning a plain JSON array. **This test needs updating before it can pass against the current gateway.**

There are no unit tests for the service/repository layers, no tests for the outbox relay, billing worker or Kafka consumer, and no tests for the CDK stack.

## Example Workflows

**1. Log in and list patients**

```bash
TOKEN=$(curl -s -X POST http://localhost:4004/api-gateway/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"testuser@test.com","password":"password123"}' | sed -E 's/.*"token":"([^"]+)".*/\1/')

curl -s http://localhost:4004/api-gateway/patients -H "Authorization: Bearer $TOKEN"
```

**2. Create a patient and watch billing and Kafka react**

```bash
curl -s -X POST http://localhost:4004/api-gateway/patients \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"Jane Roe","email":"jane.roe@example.com","address":"1 Example Street","dateOfBirth":"1990-01-31"}'

docker compose logs -f patient-service analytics-service billing-service
```

You should see, in order: the billing worker logging `Billing account 12345 created for patient <id>`, the billing server logging `createBillingAccount request received`, and the analytics service logging `Processed PATIENT_CREATED event <event-id> for patient <id>`. A follow-up `GET /api-gateway/patients` then shows `billingStatus: ACTIVE`.

**3. Observe the retry path**

```bash
docker compose stop billing-service
# create a patient: it stays PENDING; patient-service logs "Billing attempt N failed ... next try at ..."
docker compose start billing-service
# on the next due attempt the patient flips to ACTIVE
```

**4. Reject an unauthenticated call**

```bash
curl -i http://localhost:4004/api-gateway/patients     # 401, no Authorization header
```

**5. Call the billing gRPC endpoint directly**

Use `grpc-requests/billing-service/create-billing-account.http` in IntelliJ (target `localhost:9001`, method `BillingService/CreateBillingAccount`).

## Implementation Status and Known Limitations

Implemented and verified in source:

- Everything described in the sections above.

Stubs and placeholders:

- **Billing Service** always returns `accountId = "12345"` and `status = "Active"`; it has no storage and no business logic.
- **Analytics Service** parses, deduplicates and logs events; the "business logic" is a comment.
- **Deduplication** is in memory (10,000 ids), so it is lost on restart and not shared between replicas.

Known issues:

1. **Invalid-token handling in the gateway is untested.** `JwtValidationException` is a `@RestControllerAdvice` mapping `WebClientResponseException.Unauthorized` to `401`. Whether an exception raised inside a gateway filter reaches that handler was not verified; check the status returned for a tampered token before relying on it.
2. **No authorization**: roles are not enforced, and the Auth Service and Patient Service accept any request that reaches them. On AWS the ECS services share one security group that allows all traffic between members.
3. **Outbox rows are never deleted.** Published rows keep `published_at` set and accumulate.
4. **Only creation emits events.** Updates and deletions publish nothing, and `PatientNotFoundException` maps to `400` rather than `404`.
5. **Plaintext internal traffic.** gRPC uses `usePlaintext()` and gateway-to-service calls are HTTP.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Compose warns that `DB_USER`/`DB_PASSWORD`/`JWT_SECRET` are not set, or Postgres refuses to start | `.env` is missing. Create it as shown in [Getting Started](#getting-started). |
| `auth-service` exits with a Base64/`WeakKeyException`/`Could not resolve placeholder 'jwt.secret'` error | `JWT_SECRET` is unset, not valid Base64, or decodes to fewer than 32 bytes. Use `openssl rand -base64 32`. |
| `api-gateway` fails with `Could not resolve placeholder 'auth.service.url'` | `AUTH_SERVICE_URL` is not set (Compose sets it; add it when running the gateway another way). |
| `Connection refused`/`UnknownHost` for `auth-service` when running the gateway on the host | Default routes use Compose hostnames. Run the gateway through Compose. |
| `401` on every patient request | Missing `Bearer ` prefix, expired token (1-hour lifetime), or a token signed with a different `JWT_SECRET` (for example after changing `.env` without recreating `auth-service`). Log in again. |
| New patients stay `PENDING` | Billing Service is unreachable or failing. Check `docker compose logs patient-service` for `Billing attempt ... failed`; after `billing.retry.max-attempts` they become `FAILED`. |
| Kafka client cannot connect from the host | Use `localhost:9094` (external listener), not `kafka:9092`, which is only resolvable inside the Compose network. |
| Consumer or producer errors about a missing topic | The broker has auto-creation disabled. `PATIENT` is created by patient-service and `PATIENT.DLT` by analytics-service at startup, so start those services once Kafka is healthy. |
| Seed data missing | `data.sql` only runs when `SPRING_SQL_INIT_MODE=always` (set in Compose for auth and patient services). |
| Schema out of date after entity changes | `ddl-auto=update` only adds; for a clean slate run `docker compose down -v`. |
| On AWS, ECS tasks cannot pull images | The ECR repositories are created empty; push `latest` images for every service. |

## Future Improvements

These are **not implemented** today:

- Role-based authorization using the `role` claim; user registration and token refresh.
- A real Billing Service with persistence and real analytics logic.
- A durable, shared idempotency store (a table with a unique `event_id`, as noted in `InMemoryProcessedEventStore`).
- Outbox cleanup and events for updates/deletes.
- Adding unit tests for the outbox, billing worker, and consumer, plus Testcontainers-based integration tests.
- Authenticated/TLS service-to-service traffic, and a CI pipeline that builds and pushes images to ECR.
- A shared module (or schema registry) for the duplicated `.proto` files.

## Author and License

- Author: Dilrangi Sankalpana (from the git history).
- License: No license file is present in the repository; add one before accepting external contributions or reuse.
