# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# CELL ********************

%pip install fabric-deployment-tool --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Variables - UPDATE!

# MARKDOWN ********************

# ## Vairables to Replace
# 
# - Add the target Workspace in workspace_name. It will use the target Workspace or create it.
# - Add the Capacity name in capacity_name. It will be used only if the workspace doesn´t exist and for the capacity assignment to that workspace.
# - Add the target Eventhouse name in eventhouse_name. Can be an existing Eventhouse. If blank will use detault name.
# - Add the list of environments you want to update in a structure approach in environments. It should be an array of object with the following format {"workspace_id": "<guid>", "environment_id": "<guid>"}.

# CELL ********************

workspace_name = "Fabric Spark Monitoring"
capacity_name= ""
eventhouse_name = "fabric-spark-monitoring"

environments = [
    {"workspace_id": "", "environment_id": ""},
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Source Git Repo

# CELL ********************

##### DO NET CHANGE UNLESS SPECIFIED OTHERWISE ####
repo_owner = "microsoft" 
repo_name = "fabric-toolbox" 
branch = "main"
folder_prefix = "monitoring/fabric-spark-monitoring" 
github_token = ""
###################################################

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Process

# MARKDOWN ********************

# ## ****Deployment

# CELL ********************

import fabric_deployment_tool
import notebookutils

exclude = []
type_exclude = []

fabDeploymentTool = fabric_deployment_tool.FabDeploymentTool()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

fabDeploymentTool.set_github(repo_owner, repo_name, branch, folder_prefix, github_token)
fabDeploymentTool.run(exclude=exclude, type_exclude=type_exclude, workspace_name=workspace_name, capacity_name=capacity_name, eventhouse_name=eventhouse_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

fabDeploymentTool.fab_update_environments_spark_monitor(environments, workspace_name, "SparkMonitoring","IngestionEndpoint")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

display(fabDeploymentTool.mapping_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
