import subprocess
from enum import Enum
from typing import Optional, Union
from pathlib import Path
import requests
from tqdm import tqdm
import logging

_logger = logging.getLogger("winrpmdepscalc")


class DownloaderType(Enum):
    POWERSHELL = "powershell"
    PYTHON = "python"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value.lower() in (item.value for item in cls)


class Downloader:
    def __init__(self, downloader_type: str = "powershell", proxy_url: Optional[str] = None, skip_ssl_verify: bool = True) -> None:
        dt = downloader_type.lower()
        if not DownloaderType.has_value(dt):
            allowed = ', '.join(d.value for d in DownloaderType)
            raise ValueError(
                f"Invalid downloader '{downloader_type}'. Allowed: {allowed}")
        self.downloader_type = DownloaderType(dt)
        if self.downloader_type == DownloaderType.PYTHON:
            self.session = requests.Session()
            if proxy_url:
                self.session.proxies = {"http": proxy_url, "https": proxy_url}
            else:
                self.session.trust_env = True
            self.session.verify = not skip_ssl_verify
        else:
            self.session = None

    def download(self, url: str, output_file: Union[str, Path]) -> None:
        if self.downloader_type == DownloaderType.POWERSHELL:
            self._download_powershell(url, output_file)
        else:
            self._download_python(url, output_file)

    def _download_powershell(self, url: str, output_file: Union[str, Path]) -> None:
        ps_script = (
            f"$wc = New-Object System.Net.WebClient; "
            f"$wc.Proxy.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials; "
            f"$wc.DownloadFile('{url}', '{output_file}');"
        )
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_script],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            _logger.error(
                f"PowerShell download failed:\n{result.stderr.strip()}")
            raise RuntimeError(
                f"PowerShell download failed:\n{result.stderr.strip()}")
        _logger.info(f"Downloaded {output_file} via PowerShell")

    def _download_python(self, url: str, output_file: Union[str, Path]) -> None:
        if not self.session:
            raise RuntimeError("Python downloader session not initialized")
        try:
            with self.session.get(url, stream=True) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0))
                with open(output_file, 'wb') as f, tqdm(
                    total=total, unit='iB', unit_scale=True, desc=Path(output_file).name
                ) as bar:
                    for chunk in resp.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))
            _logger.info(f"Downloaded {output_file} via Python requests")
        except Exception as e:
            _logger.error(f"Failed to download {url}: {e}")
            raise
