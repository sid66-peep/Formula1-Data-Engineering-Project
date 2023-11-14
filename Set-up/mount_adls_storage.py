# Databricks notebook source
storage_account_name = "formula1learning"
client_id            = dbutils.secrets.get(scope="Formula1-scope", key="f1learning-client-id-SAS")
tenant_id            = dbutils.secrets.get(scope="Formula1-scope", key="f1learning-tenant-id-SAS")
client_secret        = dbutils.secrets.get(scope="Formula1-scope", key="f1learning-client-secret-SAS")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unmount will literally unmount the paths

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dl/demo")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl/presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl/demo")

# COMMAND ----------


