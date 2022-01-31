from dagster import job, op, get_dagster_logger, InputDefinition

import dagstermill as dm
from dagster.utils import script_relative_path
from urllib.request import urlretrieve

k_means_iris = dm.define_dagstermill_op(
    "k_means_iris",
    script_relative_path("iris-kmeans.ipynb"),
    output_notebook_name="iris_kmeans_output",
    input_defs=[
      InputDefinition("path", str, description="Local path to the Iris dataset"),
      InputDefinition("clusters", int, description="Number of clusters"),
    ]
)

# https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
@op
def download_file(url:str, local_path: str) -> str:
  urlretrieve(url, local_path)

  return local_path

@job(
    resource_defs={
        "output_notebook_io_manager": dm.local_output_notebook_io_manager,
    }
)
def iris_classify():
    k_means_iris(download_file())