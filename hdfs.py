# hdfs_mcp_server.py
from mcp.server.fastmcp import FastMCP
import subprocess
import os
import configparser
from typing import Optional, List

# 读取配置文件
config = configparser.ConfigParser()
config_file = "hdfs_config.ini"

# 默认配置
HDFS_NAMENODE = "node4.cdp1-wxiao.coelab.cloudera.com"
HDFS_PORT = "8020"

# 如果配置文件存在，读取配置
if os.path.exists(config_file):
    config.read(config_file)
    if 'HDFS' in config:
        HDFS_NAMENODE = config['HDFS'].get('namenode', HDFS_NAMENODE)
        HDFS_PORT = config['HDFS'].get('port', HDFS_PORT)

# Create a more robust HDFS URI for consistency
HDFS_URI = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}"

# 创建 MCP 服务器
mcp = FastMCP("HDFS-Controller")

def execute_hdfs_command(cmd_args: List[str]) -> dict:
    """执行 HDFS 命令的通用函数"""
    try:
        # Start with the base hdfs command
        hdfs_cmd = ["hdfs"]

        # Determine where to insert -Dfs.defaultFS
        # It should come after the subcommand like 'dfs' or 'dfsadmin'
        # but before the specific command options like '-ls' or '-report'.
        # Check if cmd_args has at least one element (the subcommand)
        if cmd_args:
            subcommand = cmd_args[0]
            # Add the subcommand first
            hdfs_cmd.append(subcommand)
            # Then add the -D property
            hdfs_cmd.extend(["-D", f"fs.defaultFS={HDFS_URI}"])
            # Then add the rest of the arguments
            hdfs_cmd.extend(cmd_args[1:])
        else:
            # If cmd_args is empty, it's an invalid call for this function.
            # This case shouldn't happen with the current tool definitions.
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
            timeout=60   # Increased timeout to 60 seconds for potentially longer operations
        )
        
        return {
            "success": True,
            "output": result.stdout.strip(),
            "error": result.stderr.strip() if result.stderr else None # Capture stderr even on success for warnings
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
            "error": "Command timed out after 60 seconds. Consider increasing timeout."
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
    """列出 HDFS 目录中的文件和子目录
    
    Args:
        path: HDFS 路径，默认为根目录 "/"
    """
    result = execute_hdfs_command(["dfs", "-ls", path])
    
    if result["success"]:
        if result["output"]:
            return f"HDFS 目录 '{path}' 的内容:\n{result['output']}"
        else:
            return f"HDFS 目录 '{path}' 为空"
    else:
        return f"列出目录失败: {result['error']}"

@mcp.tool()
def read_hdfs_file(file_path: str) -> str:
    """读取 HDFS 文件的内容
    
    Args:
        file_path: HDFS 文件路径
    """
    result = execute_hdfs_command(["dfs", "-cat", file_path])
    
    if result["success"]:
        return f"文件 '{file_path}' 的内容:\n{result['output']}"
    else:
        return f"读取文件失败: {result['error']}"

@mcp.tool()
def create_hdfs_directory(path: str) -> str:
    """在 HDFS 中创建目录
    
    Args:
        path: 要创建的 HDFS 目录路径
    """
    result = execute_hdfs_command(["dfs", "-mkdir", "-p", path])
    
    if result["success"]:
        return f"成功创建 HDFS 目录: {path}"
    else:
        return f"创建目录失败: {result['error']}"

@mcp.tool()
def delete_hdfs_path(path: str, recursive: bool = False) -> str:
    """删除 HDFS 文件或目录
    
    Args:
        path: 要删除的 HDFS 路径
        recursive: 是否递归删除目录，默认为 False
    """
    cmd = ["dfs", "-rm"]
    if recursive:
        cmd.append("-r")
    cmd.append(path)
    
    result = execute_hdfs_command(cmd)
    
    if result["success"]:
        return f"成功删除 HDFS 路径: {path}"
    else:
        return f"删除路径失败: {result['error']}"

@mcp.tool()
def upload_to_hdfs(local_path: str, hdfs_path: str) -> str:
    """将本地文件上传到 HDFS
    
    Args:
        local_path: 本地文件路径
        hdfs_path: HDFS 目标路径
    """
    # 检查本地文件是否存在
    if not os.path.exists(local_path):
        return f"本地文件不存在: {local_path}"
    
    result = execute_hdfs_command(["dfs", "-put", local_path, hdfs_path])
    
    if result["success"]:
        return f"成功上传文件 '{local_path}' 到 HDFS '{hdfs_path}'"
    else:
        return f"上传文件失败: {result['error']}"

@mcp.tool()
def download_from_hdfs(hdfs_path: str, local_path: str) -> str:
    """从 HDFS 下载文件到本地
    
    Args:
        hdfs_path: HDFS 文件路径
        local_path: 本地目标路径
    """
    result = execute_hdfs_command(["dfs", "-get", hdfs_path, local_path])
    
    if result["success"]:
        return f"成功下载文件 '{hdfs_path}' 到本地 '{local_path}'"
    else:
        return f"下载文件失败: {result['error']}"

@mcp.tool()
def get_hdfs_file_info(path: str) -> str:
    """获取 HDFS 文件或目录的详细信息
    
    Args:
        path: HDFS 路径
    """
    result = execute_hdfs_command(["dfs", "-stat", "%F %u %g %b %y %n", path])
    
    if result["success"]:
        return f"路径 '{path}' 的信息:\n{result['output']}"
    else:
        return f"获取文件信息失败: {result['error']}"

@mcp.tool()
def get_hdfs_disk_usage(path: str = "/") -> str:
    """获取 HDFS 路径的磁盘使用情况
    
    Args:
        path: HDFS 路径，默认为根目录 "/"
    """
    result = execute_hdfs_command(["dfs", "-du", "-h", path])
    
    if result["success"]:
        return f"路径 '{path}' 的磁盘使用情况:\n{result['output']}"
    else:
        return f"获取磁盘使用情况失败: {result['error']}"

@mcp.tool()
def get_hdfs_cluster_status() -> str:
    """获取 HDFS 集群状态报告"""
    # dfsadmin also works with -D after the subcommand
    result = execute_hdfs_command(["dfsadmin", "-report"])
    
    if result["success"]:
        return f"HDFS 集群状态报告:\n{result['output']}"
    else:
        return f"获取集群状态失败: {result['error']}"

@mcp.tool()
def copy_within_hdfs(source_path: str, dest_path: str) -> str:
    """在 HDFS 内部复制文件或目录
    
    Args:
        source_path: 源 HDFS 路径
        dest_path: 目标 HDFS 路径
    """
    result = execute_hdfs_command(["dfs", "-cp", source_path, dest_path])
    
    if result["success"]:
        return f"成功复制 '{source_path}' 到 '{dest_path}'"
    else:
        return f"复制失败: {result['error']}"

@mcp.tool()
def move_within_hdfs(source_path: str, dest_path: str) -> str:
    """在 HDFS 内部移动文件或目录
    
    Args:
        source_path: 源 HDFS 路径
        dest_path: 目标 HDFS 路径
    """
    result = execute_hdfs_command(["dfs", "-mv", source_path, dest_path])
    
    if result["success"]:
        return f"成功移动 '{source_path}' 到 '{dest_path}'"
    else:
        return f"移动失败: {result['error']}"

@mcp.tool()
def get_hdfs_config_info() -> str:
    """获取当前 HDFS 连接配置信息"""
    return f"当前 HDFS 配置:\nNameNode: {HDFS_NAMENODE}\n端口: {HDFS_PORT}\nHDFS URI: {HDFS_URI}"

@mcp.tool()
def test_hdfs_connection() -> str:
    """测试与 HDFS 集群的连接"""
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