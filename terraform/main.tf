terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
  required_version = ">= 1.0"
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
  client_id       = var.client_id
  client_secret   = var.client_secret
  tenant_id       = var.tenant_id
}

# Resource Group — container for all our resources
resource "azurerm_resource_group" "finstream" {
  name     = "finstream-rg"
  location = "East US"
}

# Storage Account — this is our Data Lake
resource "azurerm_storage_account" "finstream" {
  name                     = "finstreamdatalake"
  resource_group_name      = azurerm_resource_group.finstream.name
  location                 = azurerm_resource_group.finstream.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true  # enables Data Lake Gen2
}

# Bronze container
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.finstream.name
  container_access_type = "private"
}

# Silver container
resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.finstream.name
  container_access_type = "private"
}

# Gold container
resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.finstream.name
  container_access_type = "private"
}

# Event Hubs Namespace — the container for our event hub
resource "azurerm_eventhub_namespace" "finstream" {
  name                = "finstream-eventhub-ns"
  location            = azurerm_resource_group.finstream.location
  resource_group_name = azurerm_resource_group.finstream.name
  sku                 = "Standard"
  capacity            = 1
}

# Event Hub — this is where transactions stream into
resource "azurerm_eventhub" "transactions" {
  name                = "transactions"
  namespace_name      = azurerm_eventhub_namespace.finstream.name
  resource_group_name = azurerm_resource_group.finstream.name
  partition_count     = 2
  message_retention   = 1
}

# Event Hub Authorization Rule — so our app can send/receive
resource "azurerm_eventhub_authorization_rule" "finstream" {
  name                = "finstream-auth"
  namespace_name      = azurerm_eventhub_namespace.finstream.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = azurerm_resource_group.finstream.name
  listen              = true
  send                = true
  manage              = false
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "finstream" {
  name                = "finstream-databricks"
  resource_group_name = azurerm_resource_group.finstream.name
  location            = azurerm_resource_group.finstream.location
  sku                 = "premium"
}