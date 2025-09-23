from pathlib import Path
import tempfile


class Config:
    def __init__(self) -> None:
        self.REPO_BASE_URL: str = "https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/"
        self.REPOMD_XML: str = "repodata/repomd.xml"
        self.TEMP_DOWNLOAD_DIR: Path = Path(tempfile.gettempdir())
        self.LOCAL_REPOMD_FILE: Path = self.TEMP_DOWNLOAD_DIR / "repomd.xml"
        self.LOCAL_XZ_FILE: Path = self.TEMP_DOWNLOAD_DIR / "primary.xml.xz"
        self.LOCAL_XML_FILE: Path = self.TEMP_DOWNLOAD_DIR / "primary.xml"
        self.PACKAGE_COLUMNS: int = 4
        self.PACKAGE_COLUMN_WIDTH: int = 30
        self.DOWNLOAD_DIR: Path = Path("rpms")
        self.SKIP_SSL_VERIFY: bool = True
        self.SUPPORT_WEAK_DEPS: bool = False
        self.ONLY_LATEST_VERSION: bool = True
        self.DOWNLOADER: str = "powershell"

    def update_from_dict(self, data: dict) -> None:
        for key, value in data.items():
            key_upper = key.upper()
            if hasattr(self, key_upper):
                if key_upper == "TEMP_DOWNLOAD_DIR":
                    temp_dir = Path(value)
                    self.TEMP_DOWNLOAD_DIR = temp_dir
                    if self.LOCAL_REPOMD_FILE.parent != temp_dir:
                        self.LOCAL_REPOMD_FILE = temp_dir / "repomd.xml"
                        self.LOCAL_XZ_FILE = temp_dir / "primary.xml.xz"
                        self.LOCAL_XML_FILE = temp_dir / "primary.xml"
                else:
                    setattr(self, key_upper, value if not isinstance(
                        getattr(self, key_upper), Path) else Path(value))

    def to_dict(self) -> dict:
        return {
            k: (str(getattr(self, k)) if isinstance(
                getattr(self, k), Path) else getattr(self, k))
            for k in dir(self) if k.isupper()
        }
