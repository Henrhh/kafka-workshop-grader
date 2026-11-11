// The grader: a container registry, Postgres and one Container App replica.
//
// First run creates everything with a placeholder image. Then build and push the real
// image and run again with image=<registry>.azurecr.io/kafka-workshop-grader:<tag>.
// See infra/README.md.

@description('Location for all resources.')
param location string = resourceGroup().location

@description('Short, lower-case, globally unique prefix, for example henrik-kafka.')
@minLength(3)
@maxLength(20)
param prefix string

@description('Grader image. Leave empty on the first run.')
param image string = ''

@description('Kafka bootstrap servers, from kafka-vm.bicep.')
param kafkaBootstrapServers string

param kafkaAdminUsername string = 'grader'

@secure()
param kafkaAdminPassword string

@description('Contents of infra/kafka/secrets/ca.pem.')
@secure()
param kafkaCaCertificate string

@secure()
param adminToken string

@description('Code teams type on the join page. Empty lets anyone with the link join.')
@secure()
param joinCode string = ''

@secure()
param postgresPassword string

var registryName = replace('${prefix}acr', '-', '')
var postgresUser = 'grader'
var placeholderImage = 'mcr.microsoft.com/k8se/quickstart:latest'
var acrPullRole = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')

resource logs 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: '${prefix}-logs'
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: registryName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
  }
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${prefix}-grader-id'
  location: location
}

resource pull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, identity.id, acrPullRole)
  scope: registry
  properties: {
    roleDefinitionId: acrPullRole
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource postgres 'Microsoft.DBforPostgreSQL/flexibleServers@2022-12-01' = {
  name: '${prefix}-db'
  location: location
  sku: {
    name: 'Standard_B1ms'
    tier: 'Burstable'
  }
  properties: {
    version: '16'
    administratorLogin: postgresUser
    administratorLoginPassword: postgresPassword
    storage: {
      storageSizeGB: 32
    }
    backup: {
      backupRetentionDays: 7
      geoRedundantBackup: 'Disabled'
    }
    highAvailability: {
      mode: 'Disabled'
    }
  }
}

resource database 'Microsoft.DBforPostgreSQL/flexibleServers/databases@2022-12-01' = {
  parent: postgres
  name: 'grader'
}

// 0.0.0.0 to 0.0.0.0 is Azure's switch for "allow Azure services", which includes Container Apps.
resource allowAzure 'Microsoft.DBforPostgreSQL/flexibleServers/firewallRules@2022-12-01' = {
  parent: postgres
  name: 'allow-azure-services'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

resource environment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: '${prefix}-env'
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logs.properties.customerId
        sharedKey: logs.listKeys().primarySharedKey
      }
    }
  }
}

resource grader 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${prefix}-grader'
  location: location
  dependsOn: [
    pull
    database
    allowAzure
  ]
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      ingress: {
        external: true
        targetPort: empty(image) ? 80 : 8080
        transport: 'auto'
      }
      registries: empty(image) ? [] : [
        {
          server: registry.properties.loginServer
          identity: identity.id
        }
      ]
      secrets: concat([
        { name: 'db-password', value: postgresPassword }
        { name: 'kafka-admin-password', value: kafkaAdminPassword }
        { name: 'kafka-ca', value: kafkaCaCertificate }
        { name: 'admin-token', value: adminToken }
      ], empty(joinCode) ? [] : [
        { name: 'join-code', value: joinCode }
      ])
    }
    template: {
      containers: [
        {
          name: 'grader'
          image: empty(image) ? placeholderImage : image
          resources: {
            cpu: json('1.0')
            memory: '2Gi'
          }
          env: concat([
            { name: 'DATABASE_URL', value: 'jdbc:postgresql://${postgres.properties.fullyQualifiedDomainName}:5432/grader?sslmode=require' }
            { name: 'DATABASE_USERNAME', value: postgresUser }
            { name: 'DATABASE_PASSWORD', secretRef: 'db-password' }
            { name: 'KAFKA_BOOTSTRAP_SERVERS', value: kafkaBootstrapServers }
            { name: 'KAFKA_SECURITY_PROTOCOL', value: 'SASL_SSL' }
            { name: 'KAFKA_ADMIN_USERNAME', value: kafkaAdminUsername }
            { name: 'KAFKA_ADMIN_PASSWORD', secretRef: 'kafka-admin-password' }
            { name: 'KAFKA_CA_CERTIFICATE', secretRef: 'kafka-ca' }
            { name: 'KAFKA_MANAGE_ACLS', value: 'true' }
            { name: 'GRADER_ADMIN_TOKEN', secretRef: 'admin-token' }
          ], empty(joinCode) ? [] : [
            { name: 'GRADER_JOIN_CODE', secretRef: 'join-code' }
          ])
          probes: empty(image) ? [] : [
            {
              type: 'Readiness'
              httpGet: {
                path: '/actuator/health'
                port: 8080
              }
              initialDelaySeconds: 20
              periodSeconds: 10
            }
          ]
        }
      ]
      // The grader keeps running attempts in memory and ranks solves in one process,
      // so it must run as exactly one replica.
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

output registry string = registry.properties.loginServer
output graderUrl string = 'https://${grader.properties.configuration.ingress.fqdn}'
