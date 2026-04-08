@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Name of the storage account (3-24 lowercase alphanumeric)')
param storageAccountName string = 'basemaptiles${uniqueString(resourceGroup().id)}'

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    cors: {
      corsRules: [
        {
          allowedOrigins: ['*']
          allowedMethods: ['GET', 'HEAD', 'OPTIONS']
          allowedHeaders: ['*']
          exposedHeaders: ['Content-Type', 'Content-Length', 'ETag', 'Last-Modified']
          maxAgeInSeconds: 86400
        }
      ]
    }
  }
}

resource tilesContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'basemap-tiles'
  properties: {
    // Public read access on blobs so MapLibre can fetch tiles directly without auth
    publicAccess: 'Blob'
  }
}

@description('Base URL for MapLibre tile source, e.g. append /{z}/{x}/{y} for tile URLs')
output tilesBaseUrl string = '${storageAccount.properties.primaryEndpoints.blob}${tilesContainer.name}'
