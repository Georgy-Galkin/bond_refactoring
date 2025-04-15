import paramiko
import os
from pathlib import Path
from datetime import datetime

def connect_sftp_with_pem(host: str, port: int, username: str, pem_path: str):
    """
    Connects to an SFTP server using a PEM private key.

    Args:
        host (str): The SFTP server hostname or IP.
        port (int): The port number (usually 22).
        username (str): The login username.
        pem_path (str): Path to the .pem private key file.

    Returns:
        paramiko.SFTPClient: Active SFTP connection object.
    """
    try:
        key = paramiko.RSAKey.from_private_key_file(pem_path)
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, pkey=key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print(f"✅ Connected to {host} as {username}")
        return sftp
    except Exception as e:
        print(f"❌ Failed to connect to {host}: {e}")

def copy_sftp_files_by_date(sftp, remote_dir, local_dir, start_date, end_date):
    """
    Copies files from SFTP server that were modified between start_date and end_date.

    Args:
        sftp (paramiko.SFTPClient): An active SFTP connection.
        remote_dir (str): Directory on the SFTP server.
        local_dir (str): Local destination directory.
        start_date (datetime): Start datetime to filter modified files.
        end_date (datetime): End datetime to filter modified files.
    """
    Path(local_dir).mkdir(parents=True, exist_ok=True)
    files_copied = 0

    for file_attr in sftp.listdir_attr(remote_dir):
        remote_file = file_attr.filename
        mod_time = datetime.fromtimestamp(file_attr.st_mtime)

        if start_date <= mod_time <= end_date:
            remote_path = f"{remote_dir}/{remote_file}"
            local_path = os.path.join(local_dir, remote_file)
            print(f"📥 Copying: {remote_file} | Modified: {mod_time}")
            sftp.get(remote_path, local_path)
            files_copied += 1

    print(f"✅ Done. {files_copied} file(s) copied to: {local_dir}")


def download_filtered_sftp_files(
    sftp,
    remote_dir: str,
    local_dir: str,
    allowed_extensions: list,
    start_date: datetime,
    end_date: datetime
):
    """
    Downloads files from SFTP server based on extension and modification time.

    Args:
        sftp (paramiko.SFTPClient): Active SFTP connection.
        remote_dir (str): Directory on the SFTP server to scan.
        local_dir (str): Local path to download files to.
        allowed_extensions (list): List of file extensions (e.g., ['.csv', '.zip']).
        start_date (datetime): Start datetime to filter by modification date.
        end_date (datetime): End datetime to filter by modification date.
    """
    Path(local_dir).mkdir(parents=True, exist_ok=True)

    try:
        for file_attr in sftp.listdir_attr(remote_dir):
            filename = file_attr.filename
            mtime = datetime.fromtimestamp(file_attr.st_mtime)

            if not any(filename.lower().endswith(ext.lower()) for ext in allowed_extensions):
                continue
            if not (start_date <= mtime <= end_date):
                continue

            remote_path = f"{remote_dir}/{filename}"
            local_path = Path(local_dir) / filename

            sftp.get(remote_path, str(local_path))
            print(f"✅ Downloaded: {filename}  🕓 {mtime}")

    except Exception as e:
        print(f"❌ Error during download: {e}")

    
sftp = connect_sftp_with_pem('mowmft.n-df.ru',22, 'BONDUELLE', 'C:\\Users\\georg\\Desktop\\PK.pem')
download_filtered_sftp_files(
    sftp,
    remote_dir = "/Download_From_Nielsen",
    local_dir  = "C:\\Users\\georg\\Desktop\\NIELSEN\\",
    allowed_extensions = ['.zip'],
    start_date = datetime(2025, 4, 10),
    end_date = datetime(2025, 4, 15)
)
