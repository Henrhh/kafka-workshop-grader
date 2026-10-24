# Running the workshop on Azure

Two parts:

1. **Kafka on a VM**: three brokers in Docker on one Ubuntu VM, reached over TLS with
   SCRAM logins. `kafka/` has the compose file and scripts, `azure/kafka-vm.bicep` the VM.
2. **The grader on Azure Container Apps**, with Azure Database for PostgreSQL.
   `azure/grader.bicep` creates both, plus a container registry.

The compose file and scripts in `kafka/` were tested end to end locally: all 10 levels pass
over TLS + SCRAM with ACLs on. The Bicep files pass `bicep lint` but have not been deployed
yet, so expect to fix a detail or two on the first run.

Budget a few hours for the first setup, and do it a few days before the workshop.

## What you need

- An Azure subscription and the `az` CLI, logged in (`az login`)
- An SSH key (`~/.ssh/id_ed25519.pub`)
- Docker and Java 21 locally, to build the grader image

```bash
RG=kafka-workshop
LOCATION=westeurope
az group create -n $RG -l $LOCATION
```

## 1. Kafka VM

```bash
az deployment group create -g $RG -f infra/azure/kafka-vm.bicep \
  -p dnsLabel=<unique-label> \
     sshPublicKey="$(cat ~/.ssh/id_ed25519.pub)" \
     sshSourceAddress="$(curl -s https://ifconfig.me)"
```

The output `kafkaHost` is the VM's DNS name, for example
`<unique-label>.westeurope.cloudapp.azure.com`. Copy the kafka folder over and start it:

```bash
scp -r infra/kafka azureuser@<kafkaHost>:~/kafka
ssh azureuser@<kafkaHost>
cd ~/kafka
cp .env.example .env
docker run --rm apache/kafka:4.4.0 /opt/kafka/bin/kafka-storage.sh random-uuid   # put it in CLUSTER_ID
nano .env            # KAFKA_HOST=<kafkaHost>, CLUSTER_ID, KEYSTORE_PASSWORD, ADMIN_PASSWORD
./gen-certs.sh       # prints the CA certificate; you need it for the grader
docker compose up -d
./create-admin.sh
```

Keep `.env` and `secrets/` on the VM only. Never open ports other than 22 and 9092-9094:
the brokers trust anyone on their internal listener.

## 2. Grader

First run, with a placeholder image:

```bash
az deployment group create -g $RG -f infra/azure/grader.bicep \
  -p prefix=<short-unique-prefix> \
     kafkaBootstrapServers=<kafkaHost>:9092,<kafkaHost>:9093,<kafkaHost>:9094 \
     kafkaAdminPassword=<ADMIN_PASSWORD from .env> \
     kafkaCaCertificate="$(ssh azureuser@<kafkaHost> cat kafka/secrets/ca.pem)" \
     adminToken=<a long random string> \
     joinCode=<a short word for the slide> \
     postgresPassword=<a long random string>
```

Build and push the image to the registry from the output, then run the same command again
with `image=` set:

```bash
./gradlew bootJar
az acr login -n <registry name>
docker build --platform linux/amd64 -t <registry>/kafka-workshop-grader:1 .
docker push <registry>/kafka-workshop-grader:1
az deployment group create ... -p image=<registry>/kafka-workshop-grader:1   # plus the same parameters
```

Open `graderUrl` from the output. The live feed is at `/`, the join page at `/join.html`
and the facilitator page at `/admin.html`.

## Dry run

Before the day, join as a test team, put the file in a fork of the template and run
`./gradlew submit -Plevel=1`. You should see the checks in your terminal and on the feed.

## On the day

- Put the projector on `graderUrl`.
- Show the join link and code on a slide.
- Post announcements from `/admin.html`.
- If a team loses its `workshop.properties`, make a new one with "New password for team".

## Cost

Roughly, for a day: the VM (D2s_v5) and Postgres (B1ms) are a few euros, Container Apps
with one replica less. Delete the resource group afterwards: `az group delete -n $RG`.
