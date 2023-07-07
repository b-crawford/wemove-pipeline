# WeMove AWS analysis pipeline

Scripts and configurations for WeMove data pipelines

## Usage

Upload a file to the relevant folder in the `weca-upload-v2` bucket.

Wait for the output to appear in the `weca-output-v2` bucket.


## Project Organization

    ├── data                    <- Local folder to store data; not committed.
    ├── src                     <- Source code.
    ├── notebooks               <- Jupyter notebooks.
    ├── .gitignore              <- Detailing folders and file types which will not be committed.
    ├── .pre-commit-config.yaml <- Pre-commit actions.
    ├── conda_env.yaml          <- Dependancy list for creating conda environment.
    └── README.md               <- The top-level README (this file).

