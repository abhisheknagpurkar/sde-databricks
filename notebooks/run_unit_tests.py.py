# Databricks notebook source
!cp ../requirements.txt ~/.
%pip install -r ~/requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pytest
import os
import sys

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f'/Workspace/{repo_root}')

# COMMAND ----------

# MAGIC %pwd

# COMMAND ----------

sys.dont_write_bytecode = True

# COMMAND ----------

retcode = pytest.main([".", "-p", "no:cacheprovider"])

# COMMAND ----------

assert retcode == 0, 'The pytest invocation failed. See the log above for details.'

# COMMAND ----------


