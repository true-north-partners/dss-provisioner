"""Example: Testing dss-provisioner inside a DSS notebook.

Copy this code into a DSS Jupyter notebook to test the provider.
"""

# In a DSS notebook, dataiku is pre-imported
import dataiku

from dss_provisioner.core import DSSProvider

# Create provider with internal client
provider = DSSProvider.from_client(dataiku.api_client())
project = provider.in_project("YOUR_PROJECT_KEY")

# Test: List projects
print("Projects:", provider.projects.list_projects())

# Test: List datasets in a project (replace with your project key)
# print("Datasets:", project.datasets.list_datasets())

# Test: List recipes in a project
# print("Recipes:", project.recipes.list_recipes())
