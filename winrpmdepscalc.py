import argparse
import fnmatch
import functools
import logging
import lzma
import gzip
import bz2
import os
import sys
from collections import defaultdict, deque
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Set, Tuple, Union
from urllib.parse import urljoin

import magic
import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm
import yaml
import urllib3
import subprocess


class LogColors:
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"
    BLUE = "\033[34m"
    BOLD = "\033[1m"


class ColorFormatter(logging.Formatter):
    COLOR_MAP = {
        logging.DEBUG: LogColors.CYAN,
        logging.INFO: LogColors.GREEN,
        logging.WARNING: LogColors.YELLOW,
        logging.ERROR: LogColors.RED,
        logging.CRITICAL: LogColors.RED + LogColors.BOLD,
    }

    def format(self, record):
        color = self.COLOR_MAP.get(record.levelno, LogColors.RESET)
        message = super().format(record)
        return f"{color}{message}{LogColors.RESET}"


_logger = logging.getLogger("rpm_downloader")
_logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = ColorFormatter("%(message)s")
ch.setFormatter(formatter)
_logger.addHandler(ch)


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
        self.proxy_url = proxy_url
        self.session = None
        if self.downloader_type == DownloaderType.PYTHON:
            self.session = requests.Session()
            proxies = {}
            if proxy_url:
                proxies = {"http": proxy_url, "https": proxy_url}
            else:
                proxies = {k: v for k, v in requests.utils.get_environ_proxies(
                    "").items() if k in ("http", "https")} or {}
            self.session.proxies.update(proxies)
            self.session.verify = not skip_ssl_verify

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


class Config:
    def __init__(self) -> None:

        self.REPO_BASE_URL: str = "https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/"
        self.REPOMD_XML: str = "repodata/repomd.xml"
        self.LOCAL_REPOMD_FILE: Path = Path("repomd.xml")
        self.LOCAL_XZ_FILE: Path = Path("primary.xml.xz")
        self.LOCAL_XML_FILE: Path = Path("primary.xml")
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
                setattr(self, key_upper, value if not isinstance(
                    getattr(self, key_upper), Path) else Path(value))

    def to_dict(self) -> dict:
        return {
            k: (str(getattr(self, k)) if isinstance(
                getattr(self, k), Path) else getattr(self, k))
            for k in dir(self) if k.isupper()
        }


class MetadataManager:
    NS_REPO = {"repo": "http://linux.duke.edu/metadata/repo"}
    NS_COMMON = {
        "common": "http://linux.duke.edu/metadata/common",
        "rpm": "http://linux.duke.edu/metadata/rpm"
    }

    def __init__(self, config: Config, downloader: Downloader) -> None:
        self.config = config
        self.downloader = downloader
        self.all_packages: List[str] = []
        self.requires_map: Dict[str, Set[str]] = {}
        self.provides_map: Dict[str, Set[str]] = defaultdict(set)
        self.dep_map: Dict[str, Set[str]] = {}
        self.primary_root: Optional[ET.Element] = None
        self.metadata_loaded: bool = False
        self.repomd_root: Optional[ET.Element] = None

    def check_and_refresh_metadata(self, force_refresh: bool = False) -> None:
        required_files = [
            self.config.LOCAL_REPOMD_FILE,
            self.config.LOCAL_XZ_FILE,
            self.config.LOCAL_XML_FILE
        ]
        missing = [str(f) for f in required_files if not f.exists()]
        if missing or force_refresh:
            _logger.warning(
                f"Missing or refresh forced for metadata files: {', '.join(missing)}")
            _logger.info("Refreshing metadata...")

            repomd_url = urljoin(self.config.REPO_BASE_URL,
                                 self.config.REPOMD_XML)
            self.downloader.download(repomd_url, self.config.LOCAL_REPOMD_FILE)

            self.repomd_root = self._parse_xml(self.config.LOCAL_REPOMD_FILE)
            if self.repomd_root is None:
                raise RuntimeError("Failed to parse repomd.xml")

            primary_url = self._get_primary_location_url(self.repomd_root)
            if not primary_url:
                raise RuntimeError("Primary URL not found in repomd.xml")

            self.downloader.download(primary_url, self.config.LOCAL_XZ_FILE)
            self._decompress_file(self.config.LOCAL_XZ_FILE,
                                  self.config.LOCAL_XML_FILE)

            self._reset_metadata_state()
            self.primary_root = self._parse_xml(self.config.LOCAL_XML_FILE)
            if self.primary_root is None:
                raise RuntimeError("Failed to parse primary.xml")
            self._load_metadata_maps()
            self.metadata_loaded = True
        else:
            _logger.info("All metadata files present, skipping refresh.")
            if not self.metadata_loaded:
                self.primary_root = self._parse_xml(self.config.LOCAL_XML_FILE)
                if self.primary_root is None:
                    raise RuntimeError(
                        "Failed to parse primary XML metadata on startup")
                self._load_metadata_maps()
                self.metadata_loaded = True

    def cleanup_files(self) -> None:
        files = [
            self.config.LOCAL_REPOMD_FILE,
            self.config.LOCAL_XZ_FILE,
            self.config.LOCAL_XML_FILE,
        ]
        deleted_any = False
        for f in files:
            try:
                if f.exists():
                    f.unlink()
                    _logger.info(f"Removed {f}")
                    deleted_any = True
            except Exception as e:
                _logger.error(f"Failed to remove {f}: {e}")
        if not deleted_any:
            _logger.warning("No metadata files to remove.")
        self._reset_metadata_state()
        self.primary_root = None
        self.metadata_loaded = False

    def _reset_metadata_state(self) -> None:
        self.all_packages.clear()
        self.requires_map.clear()
        self.provides_map.clear()
        self.dep_map.clear()

    def _parse_xml(self, path: Path) -> Optional[ET.Element]:
        _logger.info(f"Parsing XML file {path}")
        try:
            return ET.parse(str(path)).getroot()
        except ET.ParseError as e:
            _logger.error(f"Failed to parse XML {path}: {e}")
            return None

    def _get_primary_location_url(self, repomd_root: ET.Element) -> Optional[str]:
        for data in repomd_root.findall("repo:data", MetadataManager.NS_REPO):
            if data.attrib.get("type") == "primary":
                location = data.find("repo:location", MetadataManager.NS_REPO)
                if location is not None:
                    href = location.attrib.get("href")
                    if href:
                        return href if href.startswith("http") else urljoin(self.config.REPO_BASE_URL, href)
        return None

    def _decompress_file(self, input_path: Path, output_path: Path) -> None:
        _logger.info(f"Decompressing {input_path} to {output_path}...")
        try:
            file_type = magic.from_file(str(input_path))
            if "XZ compressed" in file_type:
                opener = lzma.open
            elif "gzip compressed" in file_type:
                opener = gzip.open
            elif "bzip2 compressed" in file_type:
                opener = bz2.open
            else:
                raise RuntimeError(
                    f"Unsupported compression format: {file_type}")

            with opener(str(input_path), 'rb') as f_in, open(output_path, 'wb') as f_out:
                f_out.write(f_in.read())
            _logger.info("Decompression complete.")
        except Exception as e:
            _logger.error(f"Failed to decompress: {e}")
            raise

    def _load_metadata_maps(self) -> None:
        if self.primary_root is None:
            self.primary_root = self._parse_xml(self.config.LOCAL_XML_FILE)
            if self.primary_root is None:
                raise RuntimeError("Failed to load primary XML metadata")

        ns = MetadataManager.NS_COMMON

        self.all_packages = sorted(
            pkg.find("common:name", ns).text
            for pkg in self.primary_root.findall("common:package", ns)
            if pkg.find("common:name", ns) is not None
        )

        self.requires_map.clear()
        self.provides_map.clear()
        pkgs_with_format = []

        for pkg in self.primary_root.findall("common:package", ns):
            name_elem = pkg.find("common:name", ns)
            if name_elem is None:
                continue
            pkg_name = name_elem.text
            fmt = pkg.find("common:format", ns)
            if fmt is None:
                self.requires_map[pkg_name] = set()
                continue
            prov = fmt.find("rpm:provides", ns)
            if prov is not None:
                for entry in prov.findall("rpm:entry", ns):
                    pname = entry.get("name")
                    if pname:
                        self.provides_map[pname].add(pkg_name)
            pkgs_with_format.append((pkg_name, fmt))

        for pkg_name, fmt in pkgs_with_format:
            req = fmt.find("rpm:requires", ns)
            req_set = {
                entry.get("name")
                for entry in req.findall("rpm:entry", ns)
            } if req is not None else set()

            if self.config.SUPPORT_WEAK_DEPS:
                weak = fmt.find("rpm:weakrequires", ns)
                if weak is not None:
                    req_set.update(entry.get("name")
                                   for entry in weak.findall("rpm:entry", ns))
            self.requires_map[pkg_name] = req_set

        self.dep_map = {
            pkg: {
                dep for req in reqs if req in self.provides_map for dep in self.provides_map[req]}
            for pkg, reqs in self.requires_map.items()
        }

    def filter_packages(self, patterns: List[str]) -> List[str]:
        patterns = [p.strip() for p in patterns if p.strip()]
        return sorted(
            pkg for pkg in self.all_packages
            if any(fnmatch.fnmatch(pkg, pat) for pat in patterns)
        )

    @functools.lru_cache(maxsize=None)
    def resolve_all_dependencies(self, pkg_name: str) -> Optional[Set[str]]:
        if pkg_name not in self.dep_map:
            return None
        to_install: Set[str] = set()
        queue = deque([pkg_name])
        while queue:
            current = queue.popleft()
            if current in to_install:
                continue
            to_install.add(current)
            for dep in self.dep_map.get(current, set()):
                if dep not in to_install:
                    queue.append(dep)
        return to_install


def print_packages_tabular(packages: List[str], columns: int = 4, column_width: int = 30) -> None:
    if not packages:
        _logger.error("No packages found.")
        return
    for i, pkg in enumerate(packages, 1):
        print(f"{LogColors.MAGENTA}{pkg:<{column_width}}{LogColors.RESET}", end='')
        if i % columns == 0:
            print()
    if len(packages) % columns != 0:
        print()


def get_package_rpm_urls(root: ET.Element, base_url: str, package_names: List[str], only_latest: bool = True) -> List[Tuple[str, str]]:
    ns = MetadataManager.NS_COMMON
    packages_by_name: Dict[str,
                           List[Dict[str, Union[str, int]]]] = defaultdict(list)

    for pkg in root.findall("common:package", ns):
        name_elem = pkg.find("common:name", ns)
        if name_elem is None or name_elem.text not in package_names:
            continue

        version = pkg.find("common:version", ns)
        location = pkg.find("common:location", ns)
        if version is None or location is None:
            continue

        href = location.attrib.get("href")
        if not href:
            continue

        try:
            packages_by_name[name_elem.text].append({
                "ver": version.attrib.get("ver", ""),
                "rel": version.attrib.get("rel", ""),
                "epoch": int(version.attrib.get("epoch", "0")),
                "href": href,
                "name": name_elem.text,
            })
        except Exception as e:
            _logger.warning(
                f"Skipping package {name_elem.text} due to version parsing error: {e}")

    rpm_urls: List[Tuple[str, str]] = []

    for pkg in package_names:
        entries = packages_by_name.get(pkg, [])
        if only_latest:
            latest = max(entries, key=lambda e: (
                e["epoch"], e["ver"], e["rel"]), default=None)
            if latest:
                rpm_urls.append((pkg, urljoin(base_url, latest["href"])))
        else:
            for e in entries:
                rpm_urls.append((pkg, urljoin(base_url, e["href"])))

    return rpm_urls


def download_packages(
    package_names: List[str],
    dep_map: Dict[str, Set[str]],
    primary_root: ET.Element,
    config: Config,
    downloader: Downloader,
    download_deps: bool = False
) -> None:
    config.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    packages_to_download = set(package_names)

    if download_deps:
        for pkg in package_names:
            if pkg in dep_map:
                packages_to_download.update(dep_map[pkg])

    _logger.info(
        f"Downloading packages: {', '.join(sorted(packages_to_download))}")

    rpm_urls: List[Tuple[str, str]] = []
    for pkg in packages_to_download:
        urls = get_package_rpm_urls(
            primary_root, config.REPO_BASE_URL, [
                pkg], only_latest=config.ONLY_LATEST_VERSION
        )
        if not urls:
            _logger.warning(f"No RPM URLs found for {pkg}")
            continue
        rpm_urls.extend(urls)

    with tqdm(total=len(rpm_urls), desc="Downloading packages", unit="pkg") as bar:
        for _, url in rpm_urls:
            dest_file = config.DOWNLOAD_DIR / Path(url).name
            if dest_file.exists():
                tqdm.write(
                    f"{LogColors.YELLOW}Already downloaded: {dest_file.name}{LogColors.RESET}")
                bar.update(1)
                continue
            try:
                downloader.download(url, dest_file)
                tqdm.write(
                    f"{LogColors.GREEN}Downloaded: {dest_file.name}{LogColors.RESET}")
            except Exception as e:
                tqdm.write(
                    f"{LogColors.RED}Failed to download {dest_file.name}: {e}{LogColors.RESET}")
            bar.update(1)


def load_config_file(config_path: Path, config: Config) -> None:
    if not config_path.exists():
        _logger.warning(
            f"Config file '{config_path}' not found, using defaults.")
        return
    try:
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        if data:
            config.update_from_dict(data)
            _logger.info(f"Loaded config from {config_path}")
        else:
            _logger.warning(
                f"Config file {config_path} is empty, using defaults.")
    except Exception as e:
        _logger.error(f"Failed to load config {config_path}: {e}")
        _logger.warning("Continuing with default configuration.")


def write_default_config(config_path: Path, config: Config) -> None:
    default_data = config.to_dict()
    try:
        with open(config_path, 'w') as f:
            yaml.safe_dump(default_data, f, sort_keys=False)
        _logger.info(f"Default config written to {config_path}")
    except Exception as e:
        _logger.error(f"Failed to write default config: {e}")


def print_config(config: Config) -> None:
    print(
        f"\n{LogColors.BOLD}{LogColors.CYAN}--- Current Configuration ---{LogColors.RESET}")
    for key in sorted(k for k in dir(config) if k.isupper()):
        val = getattr(config, key)
        print(
            f"{LogColors.YELLOW}{key:20}{LogColors.RESET} = {LogColors.GREEN}{val}{LogColors.RESET}")
    print(f"{LogColors.BOLD}{LogColors.CYAN}-----------------------------{LogColors.RESET}\n")


def edit_configuration(config: Config, config_path: Optional[Path] = None) -> None:
    keys = sorted(k for k in dir(config) if k.isupper())
    key_map = {str(i + 1): k for i, k in enumerate(keys)}

    while True:
        print_config(config)
        print(
            f"{LogColors.YELLOW}Select config key by number (Enter to exit):{LogColors.RESET}")
        for num, key in key_map.items():
            print(f"  {LogColors.CYAN}{num}{LogColors.RESET}) {key}")
        choice = input(
            f"{LogColors.CYAN}Your choice: {LogColors.RESET}").strip()
        if not choice:
            break
        if choice not in key_map:
            print(f"{LogColors.RED}Invalid choice.{LogColors.RESET}")
            continue
        key = key_map[choice]
        current_val = getattr(config, key)
        new_val = input(
            f"{LogColors.CYAN}Enter new value for {key} (current: {current_val}): {LogColors.RESET}").strip()
        try:
            if isinstance(current_val, bool):
                new_val_lower = new_val.lower()
                if new_val_lower in {"true", "1", "yes", "y"}:
                    new_val = True
                elif new_val_lower in {"false", "0", "no", "n"}:
                    new_val = False
                else:
                    print(f"{LogColors.RED}Invalid boolean value.{LogColors.RESET}")
                    continue
            elif isinstance(current_val, int):
                new_val = int(new_val)
            elif isinstance(current_val, Path):
                new_val = Path(new_val)
        except ValueError:
            print(f"{LogColors.RED}Invalid value type.{LogColors.RESET}")
            continue
        setattr(config, key, new_val)
        print(f"{LogColors.GREEN}Updated {key} to {new_val}.{LogColors.RESET}")

    if config_path:
        save_choice = input(
            f"{LogColors.YELLOW}Save changes to config file '{config_path}'? (y/N): {LogColors.RESET}").strip().lower()
        if save_choice in ("y", "yes"):
            write_default_config(config_path, config)
            print(f"{LogColors.GREEN}Configuration saved.{LogColors.RESET}")
        else:
            print(f"{LogColors.YELLOW}Changes not saved.{LogColors.RESET}")


def prompt_package_selection(metadata: MetadataManager, ask_include_deps: bool = True) -> List[str]:
    filters = input(
        f"{LogColors.CYAN}Enter package names/wildcards (comma-separated): {LogColors.RESET}").strip()
    patterns = [p.strip() for p in filters.split(',') if p.strip()]
    selected = metadata.filter_packages(patterns)
    if not selected:
        _logger.error("No packages matched.")
        return []
    if ask_include_deps:
        include_deps = input(
            f"{LogColors.CYAN}Include dependencies? (y/N): {LogColors.RESET}").strip().lower() in {'y', 'yes', '1', 'true'}
        if include_deps:
            all_pkgs = set(selected)
            for pkg in selected:
                deps = metadata.resolve_all_dependencies(pkg)
                if deps:
                    all_pkgs.update(deps)
            return sorted(all_pkgs)
    return sorted(selected)


def list_packages(metadata: MetadataManager, package_patterns: Optional[List[str]] = None) -> None:
    if not package_patterns:
        patterns_input = input(
            f"{LogColors.CYAN}Enter wildcard filters (comma-separated): {LogColors.RESET}").strip()
        package_patterns = [p.strip()
                            for p in patterns_input.split(",") if p.strip()]
    packages = metadata.filter_packages(package_patterns)
    print_packages_tabular(
        packages, metadata.config.PACKAGE_COLUMNS, metadata.config.PACKAGE_COLUMN_WIDTH)


def calc_dependencies(metadata: MetadataManager) -> None:
    selected = prompt_package_selection(metadata, ask_include_deps=False)
    if not selected:
        return
    for package_name in selected:
        if package_name not in metadata.dep_map:
            _logger.error(f"Package '{package_name}' not found.")
            continue
        deps = metadata.resolve_all_dependencies(package_name)
        if not deps:
            _logger.error(f"Cannot resolve dependencies for {package_name}.")
            continue
        _logger.info(f"Dependencies for {package_name}:")
        print_packages_tabular(sorted(
            deps), metadata.config.PACKAGE_COLUMNS, metadata.config.PACKAGE_COLUMN_WIDTH)


def refresh_metadata(metadata: MetadataManager, *_) -> None:
    metadata.check_and_refresh_metadata(force_refresh=True)


def cleanup_metadata(metadata: MetadataManager, *_) -> None:
    metadata.cleanup_files()


def list_rpm_urls(metadata: MetadataManager, package_names: Optional[List[str]] = None) -> None:
    if not package_names or len(package_names) == 0:
        selected = prompt_package_selection(metadata)
    else:
        selected = package_names
    if not selected:
        return
    urls = get_package_rpm_urls(metadata.primary_root, metadata.config.REPO_BASE_URL,
                                selected, only_latest=metadata.config.ONLY_LATEST_VERSION)
    if not urls:
        _logger.error("No RPM URLs found.")
        return
    for pkg, url in urls:
        print(f"{LogColors.MAGENTA}{pkg:<30}{LogColors.CYAN}{url}{LogColors.RESET}")


def download_packages_ui(metadata: MetadataManager, package_names: Optional[List[str]] = None) -> None:
    if not package_names or len(package_names) == 0:
        selected = prompt_package_selection(metadata)
    else:
        selected = package_names
    if not selected:
        return
    download_packages(selected, metadata.dep_map, metadata.primary_root,
                      metadata.config, metadata.downloader, download_deps=False)


def configure_settings(metadata: MetadataManager, _, config_path: Optional[Path] = None) -> None:
    edit_configuration(metadata.config, config_path)


def exit_program(*_) -> None:
    _logger.info("Goodbye!")
    sys.exit(0)


MENU_ACTIONS = {
    "1": list_packages,
    "2": calc_dependencies,
    "3": refresh_metadata,
    "4": cleanup_metadata,
    "5": list_rpm_urls,
    "6": download_packages_ui,
    "9": configure_settings,
    "0": exit_program,
}


def run_interactive_menu(metadata: MetadataManager, config_path: Path) -> None:
    while True:
        print(f"\n{LogColors.BOLD}{LogColors.BLUE}--- MENU ---{LogColors.RESET}")
        print(f"{LogColors.YELLOW}1) List packages{LogColors.RESET}")
        print(f"{LogColors.YELLOW}2) Calculate dependencies{LogColors.RESET}")
        print(f"{LogColors.YELLOW}3) Refresh metadata files{LogColors.RESET}")
        print(f"{LogColors.YELLOW}4) Cleanup metadata files{LogColors.RESET}")
        print(f"{LogColors.YELLOW}5) List RPM URLs{LogColors.RESET}")
        print(f"{LogColors.YELLOW}6) Download packages{LogColors.RESET}")
        print(f"{LogColors.YELLOW}9) Configure settings{LogColors.RESET}")
        print(f"{LogColors.YELLOW}0) Exit{LogColors.RESET}")
        choice = input(
            f"{LogColors.CYAN}Your choice: {LogColors.RESET}").strip()
        action = MENU_ACTIONS.get(choice)
        if action:
            try:
                if action == refresh_metadata:
                    action(metadata)
                elif action == configure_settings:
                    action(metadata, None, config_path)
                elif action in (list_rpm_urls, download_packages_ui):
                    action(metadata)
                else:
                    action(metadata, None)
            except Exception as e:
                _logger.error(f"Error during operation: {e}")
        else:
            _logger.error("Invalid choice.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Windows RPM Package Metadata Tool")
    parser.add_argument('-c', '--config', type=Path,
                        default=Path("config.yaml"), help="YAML config file path")
    parser.add_argument('--write-default-config', action='store_true',
                        help="Write default config.yaml and exit")
    parser.add_argument('--list-packages', action='store_true',
                        help="List packages (interactive prompt)")
    parser.add_argument('--calc-deps', action='store_true',
                        help="Calculate dependencies (interactive prompt)")
    parser.add_argument('--refresh-meta', action='store_true',
                        help="Refresh metadata files if missing")
    parser.add_argument('--cleanup-meta', action='store_true',
                        help="Cleanup metadata files")
    parser.add_argument('--list-rpm-urls', action='store_true',
                        help="List RPM URLs for packages (interactive prompt)")
    parser.add_argument('--download', action='store_true',
                        help="Download packages (interactive prompt)")
    parser.add_argument('--configure', action='store_true',
                        help="Configure settings interactively")
    parser.add_argument('--no-interactive', action='store_true',
                        help="Disable interactive menu fallback")
    return parser.parse_args()


def main() -> None:
    try:
        args = parse_args()
        config = Config()

        if args.write_default_config:
            write_default_config(args.config, config)
            return

        load_config_file(args.config, config)

        if config.SKIP_SSL_VERIFY:
            _logger.warning(
                "SSL verification disabled; HTTPS requests insecure.")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        downloader = Downloader(
            config.DOWNLOADER, skip_ssl_verify=config.SKIP_SSL_VERIFY)
        metadata = MetadataManager(config, downloader)

        needs_metadata = any([args.list_packages, args.calc_deps is not None,
                              args.refresh_meta, args.list_rpm_urls, args.download])

        if needs_metadata:
            metadata.check_and_refresh_metadata()

        if args.list_packages:
            list_packages(metadata)
            return

        if args.calc_deps:
            calc_dependencies(metadata)
            return

        if args.refresh_meta:
            refresh_metadata(metadata)
            return

        if args.cleanup_meta:
            cleanup_metadata(metadata)
            return

        if args.list_rpm_urls:
            list_rpm_urls(metadata)
            return

        if args.download:
            download_packages_ui(metadata)
            return

        if args.configure:
            configure_settings(metadata, None, args.config)
            return

        if not args.no_interactive:
            if not metadata.metadata_loaded:
                metadata.check_and_refresh_metadata()
            run_interactive_menu(metadata, args.config)
        else:
            _logger.warning(
                "No operation specified and interactive mode disabled.")

    except KeyboardInterrupt:
        _logger.warning("\nTerminated by user (Ctrl+C). Exiting...")
        sys.exit(0)


if __name__ == "__main__":
    main()
