# hdfs Project

## Setup and Installation

This project uses Python 3.11 and `uv` for package and environment management.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/will-sh/hdfsmcp.git
    cd hdfsmcp
    ```

2.  **Ensure Python 3.11 is active:**
    This project specifies Python 3.11 in the `.python-version` file. If you use `pyenv`, it should automatically pick up this version when you enter the directory.
    If you don't have Python 3.11 installed, you can install it using your preferred method or `pyenv`:
    ```bash
    # Example using pyenv
    pyenv install 3.11
    ```

3.  **Create and activate a virtual environment using `uv`:**
    `uv` can create and manage virtual environments.
    ```bash
    uv venv
    source .venv/bin/activate  # On macOS/Linux
    # .\.venv\Scripts\activate  # On Windows
    ```

4.  **Install dependencies using `uv`:**
    `uv` will use `pyproject.toml` and `uv.lock` to install the exact dependencies.
    ```bash
    uv pip sync
    ```

After these steps, your development environment should be set up with all the necessary dependencies.
