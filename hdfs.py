# hdfs_mcp_server.py
from mcp.server.fastmcp import FastMCP
import subprocess
import os
from typing import List

# Load env from mcp json
HDFS_NAMENODE=os.environ.get("HDFS_NAMENODE")
HDFS_PORT=os.environ.get("HDFS_PORT")

# Create a more robust HDFS URI for consistency
HDFS_URI = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}"

# Create MCP server
mcp = FastMCP("HDFS-Controller")

def execute_hdfs_command(cmd_args: List[str]) -> dict:
    """Execute HDFS command utility function"""
    try:
        # Start with the base hdfs command
        hdfs_cmd = ["hdfs"]

        # Use complete URI as parameter
        if cmd_args:
            hdfs_cmd.extend(cmd_args)
            # If command contains path, ensure using complete URI
            if len(cmd_args) > 2 and not cmd_args[-1].startswith('hdfs://'):
                hdfs_cmd[-1] = f"{HDFS_URI}{cmd_args[-1]}"
        else:
            return {
                "success": False,
                "output": None,
                "error": "No HDFS subcommand provided."
            }
        
        # Log the command for debugging
        print(f"Executing HDFS command: {' '.join(hdfs_cmd)}")

        # Execute the command
        result = subprocess.run(
            hdfs_cmd,
            check=True,  # Raise CalledProcessError for non-zero exit codes
            text=True,   # Capture stdout/stderr as text
            capture_output=True, # Capture both stdout and stderr
            timeout=120   # Increase timeout to 120 seconds
        )
        
        return {
            "success": True,
            "output": result.stdout.strip(),
            "error": result.stderr.strip() if result.stderr else None
        }
        
    except subprocess.CalledProcessError as e:
        # Capture stderr from the failed command
        return {
            "success": False,
            "output": None,
            "error": f"Command failed with exit code {e.returncode}:\n{e.stderr.strip() if e.stderr else str(e)}"
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "output": None,
            "error": "Command timed out after 120 seconds. Consider increasing timeout."
        }
    except FileNotFoundError:
        return {
            "success": False,
            "output": None,
            "error": "HDFS command not found. Ensure Hadoop client is installed and 'hdfs' is in your PATH."
        }
    except Exception as e:
        return {
            "success": False,
            "output": None,
            "error": f"An unexpected error occurred: {str(e)}"
        }

@mcp.tool()
def list_hdfs_directory(path: str = "/") -> str:
    """List files and subdirectories in HDFS directory
    
    Args:
        path: HDFS path, defaults to root directory "/"
    """
    result = execute_hdfs_command(["dfs", "-ls", path])
    
    if result["success"]:
        if result["output"]:
            return f"HDFS directory '{path}' contents:\n{result['output']}"
        else:
            return f"HDFS directory '{path}' is empty"
    else:
        return f"Failed to list directory: {result['error']}"

@mcp.tool()
def read_hdfs_file(file_path: str) -> str:
    """Read contents of HDFS file
    
    Args:
        file_path: HDFS file path
    """
    result = execute_hdfs_command(["dfs", "-cat", file_path])
    
    if result["success"]:
        return f"File '{file_path}' contents:\n{result['output']}"
    else:
        return f"Failed to read file: {result['error']}"

@mcp.tool()
def create_hdfs_directory(path: str) -> str:
    """Create directory in HDFS
    
    Args:
        path: HDFS directory path to create
    """
    result = execute_hdfs_command(["dfs", "-mkdir", "-p", path])
    
    if result["success"]:
        return f"Successfully created HDFS directory: {path}"
    else:
        return f"Failed to create directory: {result['error']}"

@mcp.tool()
def delete_hdfs_path(path: str, recursive: bool = False) -> str:
    """Delete HDFS file or directory
    
    Args:
        path: HDFS path to delete
        recursive: Whether to recursively delete directory, defaults to False
    """
    cmd = ["dfs", "-rm"]
    if recursive:
        cmd.append("-r")
    cmd.append(path)
    
    result = execute_hdfs_command(cmd)
    
    if result["success"]:
        return f"Successfully deleted HDFS path: {path}"
    else:
        return f"Failed to delete path: {result['error']}"

@mcp.tool()
def upload_to_hdfs(local_path: str, hdfs_path: str) -> str:
    """Upload local file to HDFS
    
    Args:
        local_path: Local file path
        hdfs_path: HDFS target path
    """
    # Check if local file exists
    if not os.path.exists(local_path):
        return f"Local file does not exist: {local_path}"
    
    result = execute_hdfs_command(["dfs", "-put", local_path, hdfs_path])
    
    if result["success"]:
        return f"Successfully uploaded file '{local_path}' to HDFS '{hdfs_path}'"
    else:
        return f"Failed to upload file: {result['error']}"

@mcp.tool()
def download_from_hdfs(hdfs_path: str, local_path: str) -> str:
    """Download file from HDFS to local
    
    Args:
        hdfs_path: HDFS file path
        local_path: Local target path
    """
    result = execute_hdfs_command(["dfs", "-get", hdfs_path, local_path])
    
    if result["success"]:
        return f"Successfully downloaded file '{hdfs_path}' to local '{local_path}'"
    else:
        return f"Failed to download file: {result['error']}"

@mcp.tool()
def get_hdfs_file_info(path: str) -> str:
    """Get detailed information about HDFS file or directory
    
    Args:
        path: HDFS path
    """
    result = execute_hdfs_command(["dfs", "-stat", "%F %u %g %b %y %n", path])
    
    if result["success"]:
        return f"Path '{path}' information:\n{result['output']}"
    else:
        return f"Failed to get file info: {result['error']}"

@mcp.tool()
def get_hdfs_disk_usage(path: str = "/") -> str:
    """Get disk usage for HDFS path
    
    Args:
        path: HDFS path, defaults to root directory "/"
    """
    result = execute_hdfs_command(["dfs", "-du", "-h", path])
    
    if result["success"]:
        return f"Path '{path}' disk usage:\n{result['output']}"
    else:
        return f"Failed to get disk usage: {result['error']}"

@mcp.tool()
def get_hdfs_cluster_status() -> str:
    """Get HDFS cluster status report"""
    # dfsadmin also works with -D after the subcommand
    result = execute_hdfs_command(["dfsadmin", "-report"])
    
    if result["success"]:
        return f"HDFS cluster status report:\n{result['output']}"
    else:
        return f"Failed to get cluster status: {result['error']}"

@mcp.tool()
def copy_within_hdfs(source_path: str, dest_path: str) -> str:
    """Copy file or directory within HDFS
    
    Args:
        source_path: Source HDFS path
        dest_path: Destination HDFS path
    """
    result = execute_hdfs_command(["dfs", "-cp", source_path, dest_path])
    
    if result["success"]:
        return f"Successfully copied '{source_path}' to '{dest_path}'"
    else:
        return f"Failed to copy: {result['error']}"

@mcp.tool()
def move_within_hdfs(source_path: str, dest_path: str) -> str:
    """Move file or directory within HDFS
    
    Args:
        source_path: Source HDFS path
        dest_path: Destination HDFS path
    """
    result = execute_hdfs_command(["dfs", "-mv", source_path, dest_path])
    
    if result["success"]:
        return f"Successfully moved '{source_path}' to '{dest_path}'"
    else:
        return f"Failed to move: {result['error']}"

@mcp.tool()
def get_hdfs_config_info() -> str:
    """Get current HDFS connection configuration information"""
    return f"Current HDFS configuration:\nNameNode: {HDFS_NAMENODE}\nPort: {HDFS_PORT}\nHDFS URI: {HDFS_URI}"

@mcp.tool()
def test_hdfs_connection() -> str:
    """Test connection to HDFS cluster"""
    result = execute_hdfs_command(["dfs", "-ls", "/"])
    
    if result["success"]:
        return f"HDFS 连接测试成功! 连接到 {HDFS_URI}"
    else:
        return f"HDFS 连接测试失败: {result['error']}"

# 运行服务器
if __name__ == "__main__":
    if not os.getenv('HADOOP_HOME') or not os.getenv('PATH') or "hdfs" not in os.getenv('PATH'):
        print("WARNING: HADOOP_HOME or HDFS command may not be correctly configured in environment variables.")
        print("Please ensure Hadoop client is installed and its 'bin' directory is in your PATH.")
        print("Refer to Hadoop documentation for proper client setup.")

    mcp.run()