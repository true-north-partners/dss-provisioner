"""Example: Testing dss-provisioner inside a DSS notebook.

Copy this code into a DSS Jupyter notebook to test the provider.
"""

# In a DSS notebook, dataiku is pre-imported
import dataiku

from dss_provisioner.core import DSSProvider

# Create provider with internal client
provider = DSSProvider.from_client(dataiku.api_client())

# Test: List projects
print("Projects:", provider.projects.list())

# Test: List datasets in a project (replace with your project key)
# print("Datasets:", provider.datasets.list("YOUR_PROJECT_KEY"))

# Test: List recipes in a project
# print("Recipes:", provider.recipes.list("YOUR_PROJECT_KEY"))
