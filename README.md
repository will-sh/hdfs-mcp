# HDFS MCP Server

HDFS MCP Server is a controller based on MCP (Model Context Protocol) that provides access to HDFS clusters through the MCP protocol. The server supports basic HDFS operations such as file upload, download, move, copy, and provides friendly error handling and connection testing capabilities.

## Requirements

- Python 3.11 or higher
- Hadoop client installed and configured
- [`uv`](https://docs.astral.sh/uv/) package manager

## Installation

1. **Clone the repository:**
    ```bash
    git clone https://github.com/will-sh/hdfsmcp.git
    cd hdfsmcp
    ```

2. **Ensure Python 3.11 is active:**
    The project specifies Python 3.11 in the `.python-version` file. If you use `pyenv`, it will automatically use this version when you enter the directory.
    If you don't have Python 3.11 installed, you can install it using:
    ```bash
    # Example using pyenv
    pyenv install 3.11
    ```

3. **Create and activate virtual environment using `uv`:**
    ```bash
    uv venv
    source .venv/bin/activate  # macOS/Linux
    # .\.venv\Scripts\activate  # Windows
    ```

4. **Install dependencies using `uv`:**
    ```bash
    uv pip sync
    ```

## Configuration

### MCP Configuration

Add the following configuration to your Cursor MCP configuration file (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "hdfs-controller": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/your/hdfsmcp",
        "run",
        "hdfs.py"
      ],
      "env": {
        "HDFS_NAMENODE": "your_namenode_hostname",
        "NAMENODE_PORT": "your_namenode_port"
      }
    }
  }
}
```

Replace the following with your actual configuration:
- `/path/to/your/hdfsmcp`: Replace with your project's actual path
- `your_namenode_hostname`: Replace with your HDFS NameNode hostname
- `your_namenode_port`: Replace with your HDFS NameNode port (if not specify the default port is 8020)

## Features

The HDFS MCP provides the following HDFS operations:

- List directory contents
- Read file contents
- Create directories
- Delete files/directories
- Upload files to HDFS
- Download files from HDFS
- Get file/directory information
- Get disk usage
- Get cluster status
- Copy/move files within HDFS

## Usage

1. Ensure Hadoop client is properly installed and configured
2. Ensure `HADOOP_HOME` environment variable is set
3. Ensure `hdfs` command is in your system PATH

## Troubleshooting

If you encounter connection issues, check:

1. HDFS NameNode accessibility
2. Port configuration
3. Network connectivity
4. Hadoop client configuration
5. Kerberos ticket is valid

## Notes

- Ensure you have sufficient permissions to access the HDFS cluster
- Large file operations may take longer, please be patient
- It's recommended to test the connection before operations