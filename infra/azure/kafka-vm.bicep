// The Kafka VM: Ubuntu with Docker, a public DNS name, and ports 9092-9094 open.
// Deploy: az deployment group create -g <rg> -f kafka-vm.bicep -p dnsLabel=<label> sshPublicKey="$(cat ~/.ssh/id_ed25519.pub)" sshSourceAddress=<your ip>

@description('Location for all resources.')
param location string = resourceGroup().location

@description('DNS label. The VM becomes <label>.<region>.cloudapp.azure.com.')
param dnsLabel string

@description('8 GB of memory fits three brokers with 1 GB heap each.')
param vmSize string = 'Standard_D2s_v5'

param adminUsername string = 'azureuser'

@description('Your SSH public key.')
param sshPublicKey string

@description('Address allowed to SSH in, for example your public IP. Use * only if you must.')
param sshSourceAddress string

var name = 'kafka-workshop'

resource publicIp 'Microsoft.Network/publicIPAddresses@2023-09-01' = {
  name: '${name}-ip'
  location: location
  sku: {
    name: 'Standard'
  }
  properties: {
    publicIPAllocationMethod: 'Static'
    dnsSettings: {
      domainNameLabel: dnsLabel
    }
  }
}

resource nsg 'Microsoft.Network/networkSecurityGroups@2023-09-01' = {
  name: '${name}-nsg'
  location: location
  properties: {
    securityRules: [
      {
        name: 'ssh'
        properties: {
          priority: 100
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourceAddressPrefix: sshSourceAddress
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '22'
        }
      }
      {
        name: 'kafka'
        properties: {
          priority: 110
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '9092-9094'
        }
      }
    ]
  }
}

resource vnet 'Microsoft.Network/virtualNetworks@2023-09-01' = {
  name: '${name}-vnet'
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [
        '10.20.0.0/16'
      ]
    }
    subnets: [
      {
        name: 'default'
        properties: {
          addressPrefix: '10.20.0.0/24'
          networkSecurityGroup: {
            id: nsg.id
          }
        }
      }
    ]
  }
}

resource nic 'Microsoft.Network/networkInterfaces@2023-09-01' = {
  name: '${name}-nic'
  location: location
  properties: {
    ipConfigurations: [
      {
        name: 'ipconfig'
        properties: {
          privateIPAllocationMethod: 'Dynamic'
          subnet: {
            id: vnet.properties.subnets[0].id
          }
          publicIPAddress: {
            id: publicIp.id
          }
        }
      }
    ]
  }
}

var cloudInit = '''#cloud-config
package_update: true
packages:
  - docker.io
  - docker-compose-v2
  - openssl
runcmd:
  - systemctl enable --now docker
  - usermod -aG docker ${ADMIN_USER}
'''

resource vm 'Microsoft.Compute/virtualMachines@2024-03-01' = {
  name: name
  location: location
  properties: {
    hardwareProfile: {
      vmSize: vmSize
    }
    osProfile: {
      computerName: name
      adminUsername: adminUsername
      customData: base64(replace(cloudInit, '\${ADMIN_USER}', adminUsername))
      linuxConfiguration: {
        disablePasswordAuthentication: true
        ssh: {
          publicKeys: [
            {
              path: '/home/${adminUsername}/.ssh/authorized_keys'
              keyData: sshPublicKey
            }
          ]
        }
      }
    }
    storageProfile: {
      imageReference: {
        publisher: 'Canonical'
        offer: 'ubuntu-24_04-lts'
        sku: 'server'
        version: 'latest'
      }
      osDisk: {
        createOption: 'FromImage'
        diskSizeGB: 64
        managedDisk: {
          storageAccountType: 'Premium_LRS'
        }
      }
    }
    networkProfile: {
      networkInterfaces: [
        {
          id: nic.id
        }
      ]
    }
  }
}

output kafkaHost string = publicIp.properties.dnsSettings.fqdn
output bootstrapServers string = '${publicIp.properties.dnsSettings.fqdn}:9092,${publicIp.properties.dnsSettings.fqdn}:9093,${publicIp.properties.dnsSettings.fqdn}:9094'
