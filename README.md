## KPI Dashboard

**Setup**

1. Fork this repository.
2. From the Azure portal or CLI, create a web app with Azure SQL Database.
3. In the deployment center, authenticate and use the default actions to set up the deployment.
4. Set up the Tableau dashboard using the files in the `deps` folder named `tableau_dash_setup.twbx`.
5. Create a connected app in Tableau.
6. Set up the Azure database tables using the files in the `deps` folder named `azure_db_setup.sql`.
7. Whitelist the IP for your web app and Tableau dashboard.

**Required**

* Azure portal or CLI
* Tableau Desktop
* Azure SQL Database

**Credentials Required in WebApp**

* CONNECTED_APP_CLIENT_ID
* CONNECTED_APP_SECRET_KEY
* CONNECTED_APP_SECRET_ID
* TABLEAU_USER
* AZURE_SQL_SERVER
* AZURE_SQL_DATABASE
* AZURE_SQL_USER
* AZURE_SQL_PASSWORD
* AZURE_SQL_PORT
