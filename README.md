# Kafka workshop grader

The grader and live feed for the [Kafka workshop](https://github.com/Henrhh/kafka-workshop).
Participants solve levels in the template repo and run `./gradlew submit`. This app tests
their solution over Kafka, scores it in Postgres and shows every check on a live feed.

## How grading works

1. A team's `submit` writes `{attemptId, level}` to `team-<team>.submit`. ACLs only let a
   team write to its own topics, so the topic name proves who submitted.
2. The grader empties the level's input topic, waits until the team's consumer group (or
   Streams app) is up, and writes fresh, seeded input to `team-<team>.levelNN.in`.
3. It reads the team's output topics and runs the level's checks, such as counts, keys,
   values, committed offsets, late events or latency.
4. Each check, and the final verdict, goes to `workshop.events`. The team's terminal and
   the live feed both read that topic.

The graders live in `src/main/java/workshop/grader/levels`, one class per level.

## Scoring

Each level has base points (50 to 400). The first three teams to solve a level get +30%,
+20% and +10%. Each failed attempt before the solve costs 10% of the base, at most 50%.
Level 10 adds a speed bonus of up to 100 points for a low p95 latency. Solving a level
again does not change the score.

## Pages

| Path | What |
| --- | --- |
| `/` | Projector view: live feed, leaderboard, level board |
| `/join.html` | Teams pick a name and download `workshop.properties` |
| `/admin.html` | Announcements, create teams, reset a team's password |

## Run it locally

```bash
docker compose up -d                                   # Kafka and Postgres
./gradlew bootRun --args='--spring.profiles.active=dev' # admin token: dev-admin
```

Open http://localhost:8080/join.html, join, and save the file as `workshop.properties` in
the template repo. Then `./gradlew submit -Plevel=1` there.

## Tests

```bash
./gradlew test
```

`EndToEndTest` starts Kafka and Postgres in Docker and runs the reference solutions for all
10 levels through the template's real `submit` flow. The solutions are in
`src/test/java/workshop/levelNN`, next to a copy of the template's framework in
`src/test/java/workshop/framework`. Keep that copy in sync with the template.

## Configuration

All settings are environment variables. See `src/main/resources/application.yml`.

| Variable | Meaning |
| --- | --- |
| `DATABASE_URL`, `DATABASE_USERNAME`, `DATABASE_PASSWORD` | Postgres |
| `KAFKA_BOOTSTRAP_SERVERS` | How the grader reaches Kafka |
| `KAFKA_PUBLIC_BOOTSTRAP_SERVERS` | What teams get, if different |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` locally, `SASL_SSL` on the cluster |
| `KAFKA_ADMIN_USERNAME`, `KAFKA_ADMIN_PASSWORD` | A Kafka super user |
| `KAFKA_CA_CERTIFICATE` | PEM of the brokers' CA, also handed to teams |
| `KAFKA_MANAGE_ACLS` | `true` to create per-team ACLs |
| `GRADER_ADMIN_TOKEN` | Secret for the admin page. Admin is off when empty |
| `GRADER_JOIN_CODE` | Code teams need to join. Anyone can join when empty |

Run exactly one instance: running attempts and solve ranks are kept in one process.

## Deploying

See [infra/README.md](infra/README.md) for the Kafka VM and Azure Container Apps.
