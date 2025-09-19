import os
import fnmatch
import subprocess
import lzma
import gzip
import bz2
import xml.etree.ElementTree as ET
from collections import deque, defaultdict
from urllib.parse import urljoin
import urllib.request
import magic
from tqdm import tqdm
import requests
import urllib3
from requests_negotiate_sspi import HttpNegotiateAuth
from enum import Enum

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    FG_RED = "\033[31m"
    FG_GREEN = "\033[32m"
    FG_YELLOW = "\033[33m"
    FG_BLUE = "\033[34m"
    FG_MAGENTA = "\033[35m"
    FG_CYAN = "\033[36m"


class DownloaderType(Enum):
    POWERSHELL = "powershell"
    PYTHON = "python"

    @classmethod
    def has_value(cls, value):
        return value.lower() in (item.value for item in cls)


class Downloader:
    """Handles downloading files using different download methods."""

    allowed_downloaders = [DownloaderType.POWERSHELL, DownloaderType.PYTHON]

    def __init__(self, downloader_type="powershell"):
        d = downloader_type.lower()
        if not DownloaderType.has_value(d):
            valid = ', '.join([dt.value for dt in self.allowed_downloaders])
            raise ValueError(
                f"Invalid downloader '{downloader_type}'. Valid options are: {valid}"
            )
        self.downloader_type = DownloaderType(d)

    def download(self, url, output_file, proxy_url=None):
        if self.downloader_type == DownloaderType.POWERSHELL:
            self._download_with_powershell(url, output_file)
        elif self.downloader_type == DownloaderType.PYTHON:
            self._download_with_python(url, output_file, proxy_url)
        else:
            raise RuntimeError(
                f"Unsupported downloader {self.downloader_type}")

    def _download_with_powershell(self, url, output_file):
        """Downloads a file using PowerShell WebClient to honor system proxy settings."""
        ps_script = f"""
            $wc = New-Object System.Net.WebClient
            $wc.Proxy.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials

            $progress = [System.Management.Automation.ProgressRecord]::new(1, "Downloading", "{url}")
            $wc.DownloadProgressChanged += {{
                param($sender, $e)
                $progress.PercentComplete = $e.ProgressPercentage
                Write-Progress -ProgressRecord $progress
            }}
            $wc.DownloadFileAsync('{url}', '{output_file}')
            while ($wc.IsBusy) {{
                Start-Sleep -Milliseconds 100
            }}
        """
        tqdm.write(
            f"{Colors.FG_CYAN}Downloading {url} to {output_file} with PowerShell...{Colors.RESET}")
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_script], capture_output=True, text=True)
        if result.returncode != 0:
            tqdm.write(
                f"{Colors.FG_RED}PowerShell error:\n{result.stderr}{Colors.RESET}")
            raise RuntimeError(f"Failed to download {url} via PowerShell")

    def _download_with_python(self, url, output_file, proxy_url=None):
        """
        Downloads a file using Python requests with support for HTTP and HTTPS proxies.
        """
        proxies = {}

        if proxy_url:

            proxies["http"] = proxy_url
            proxies["https"] = proxy_url
        else:

            system_proxies = urllib.request.getproxies()
            http_proxy = system_proxies.get("http")
            https_proxy = system_proxies.get("https")
            if http_proxy:
                proxies["http"] = http_proxy
            if https_proxy:
                proxies["https"] = https_proxy

        session = requests.Session()
        session.auth = HttpNegotiateAuth()

        tqdm.write(
            f"{Colors.FG_CYAN}Using proxies: HTTP={proxies.get('http', 'None')}, HTTPS={proxies.get('https', 'None')}{Colors.RESET}")

        with session.get(url, stream=True, proxies=proxies or None, verify=False) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length", 0))
            with open(output_file, "wb") as f, tqdm(total=total_size, unit="iB", unit_scale=True, desc=f"Downloading {url}") as bar:
                for chunk in response.iter_content(1024):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))


class Config:
    REPO_BASE_URL = "https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/"
    REPOMD_XML = "repodata/repomd.xml"
    LOCAL_REPOMD_FILE = "repomd.xml"
    LOCAL_XZ_FILE = "primary.xml.xz"
    LOCAL_XML_FILE = "primary.xml"
    PACKAGE_COLUMNS = 4
    PACKAGE_COLUMN_WIDTH = 30
    DOWNLOAD_DIR = "rpms"
    SUPPORT_WEAK_DEPS = False
    ONLY_LATEST_VERSION = True
    DOWNLOADER = "powershell"

    @classmethod
    def print_config(cls):
        print(
            f"\n{Colors.BOLD}{Colors.FG_CYAN}--- Current Configuration ---{Colors.RESET}")
        for key in sorted(k for k in dir(cls) if k.isupper() and not k.startswith('_')):
            print(
                f"{Colors.FG_YELLOW}{key:20}{Colors.RESET} = {Colors.FG_GREEN}{getattr(cls, key)}{Colors.RESET}")
        print(
            f"{Colors.BOLD}{Colors.FG_CYAN}-----------------------------{Colors.RESET}\n")

    @classmethod
    def set_config(cls, key, value):
        if not hasattr(cls, key):
            print(f"{Colors.FG_RED}Config key '{key}' not found.{Colors.RESET}")
            return False
        if key == "DOWNLOADER":
            allowed = [dt.value for dt in Downloader.allowed_downloaders]
            if value.lower() not in allowed:
                print(
                    f"{Colors.FG_RED}Invalid DOWNLOADER value. Allowed: {', '.join(allowed)}{Colors.RESET}")
                return False
            setattr(cls, key, value.lower())
            print(f"{Colors.FG_GREEN}Updated {key} to: {value.lower()}{Colors.RESET}")
            return True
        else:
            setattr(cls, key, value)
            print(f"{Colors.FG_GREEN}Updated {key} to: {value}{Colors.RESET}")
            return True


class MetadataHandler:
    """Manages metadata related to packages and their dependency relationships."""

    def __init__(self):
        self.all_packages = []
        self.requires_map = {}
        self.provides_map = defaultdict(set)
        self.dep_map = {}

    def reset(self):
        """Clear all cached metadata state."""
        self.all_packages.clear()
        self.requires_map.clear()
        self.provides_map.clear()
        self.dep_map.clear()
        print(f"{Colors.FG_CYAN}Metadata state has been reset.{Colors.RESET}")

    def check_and_refresh_metadata(self):
        """Verify presence of metadata files - download and decompress if missing."""
        required_files = [Config.LOCAL_REPOMD_FILE,
                          Config.LOCAL_XZ_FILE, Config.LOCAL_XML_FILE]
        missing = [f for f in required_files if not os.path.exists(f)]

        if missing:
            print(
                f"{Colors.FG_YELLOW}Missing metadata files: {', '.join(missing)}{Colors.RESET}")
            print(f"{Colors.FG_CYAN}Refreshing metadata...{Colors.RESET}")

            downloader = Downloader(Config.DOWNLOADER)
            repomd_url = urljoin(Config.REPO_BASE_URL, Config.REPOMD_XML)
            downloader.download(repomd_url, Config.LOCAL_REPOMD_FILE)

            repomd_root = parse_xml(Config.LOCAL_REPOMD_FILE)
            if repomd_root is None:
                raise RuntimeError(
                    "Failed to parse repomd.xml, cannot continue.")

            primary_url = get_primary_location_url(
                repomd_root, Config.REPO_BASE_URL)
            if not primary_url:
                raise RuntimeError(
                    "Primary metadata URL not found in repomd.xml")

            downloader.download(primary_url, Config.LOCAL_XZ_FILE)
            decompress_file(Config.LOCAL_XZ_FILE, Config.LOCAL_XML_FILE)
        else:
            print(
                f"{Colors.FG_GREEN}All metadata files present, skipping refresh.{Colors.RESET}")

        self.reset()

    def cleanup_files(self):
        """Remove all metadata files and reset."""
        files = [Config.LOCAL_REPOMD_FILE,
                 Config.LOCAL_XZ_FILE, Config.LOCAL_XML_FILE]
        deleted_any = False
        for f in files:
            if os.path.exists(f):
                try:
                    os.remove(f)
                    print(f"{Colors.FG_GREEN}Removed {f}{Colors.RESET}")
                    deleted_any = True
                except Exception as e:
                    print(f"{Colors.FG_RED}Failed to remove {f}: {e}{Colors.RESET}")
        if not deleted_any:
            print(f"{Colors.FG_YELLOW}No metadata files to remove.{Colors.RESET}")
        self.reset()

    def build_maps(self, root):
        """Build provides, requires, and dependency maps from parsed XML metadata."""
        ns = {"common": "http://linux.duke.edu/metadata/common",
              "rpm": "http://linux.duke.edu/metadata/rpm"}
        provides = defaultdict(set)
        requires = {}

        pkgs_with_format = []
        for pkg in root.findall("common:package", ns):
            name_elem = pkg.find("common:name", ns)
            if name_elem is None:
                continue
            pkg_name = name_elem.text
            fmt = pkg.find("common:format", ns)
            if fmt is None:
                requires[pkg_name] = set()
                continue
            prov = fmt.find("rpm:provides", ns)
            if prov is not None:
                for entry in prov.findall("rpm:entry", ns):
                    pname = entry.get("name")
                    if pname:
                        provides[pname].add(pkg_name)
            pkgs_with_format.append((pkg_name, fmt))

        for pkg_name, fmt in pkgs_with_format:
            req = fmt.find("rpm:requires", ns)
            req_set = {entry.get("name") for entry in req.findall(
                "rpm:entry", ns)} if req is not None else set()
            if Config.SUPPORT_WEAK_DEPS:
                weak = fmt.find("rpm:weakrequires", ns)
                if weak is not None:
                    req_set.update(entry.get("name")
                                   for entry in weak.findall("rpm:entry", ns))
            requires[pkg_name] = req_set

        dep_map = {
            pkg: {
                dep for req in reqs if req in provides for dep in provides[req]}
            for pkg, reqs in requires.items()
        }
        return requires, provides, dep_map

    def filter_packages(self, input_str):
        """Filter stored packages based on comma-separated wildcards."""
        patterns = [p.strip() for p in input_str.split(',')]
        filtered = {
            pkg for pkg in self.all_packages for pat in patterns if fnmatch.fnmatch(pkg, pat)}
        return sorted(filtered)


def decompress_file(input_path, output_path):
    """Automatically detect compression type and decompress the file."""
    print(f"{Colors.FG_CYAN}Decompressing {input_path} to {output_path}...{Colors.RESET}")
    try:
        file_type = magic.from_file(input_path)
        open_map = {
            "XZ compressed": lzma.open,
            "gzip compressed": gzip.open,
            "bzip2 compressed": bz2.open,
        }
        for key, open_func in open_map.items():
            if key in file_type:
                with open_func(input_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
                    f_out.write(f_in.read())
                print(
                    f"{Colors.FG_GREEN}Decompressed using {key.split()[0].lower()} ({key.split()[1]}).{Colors.RESET}")
                return
        raise RuntimeError(f"Unsupported compression format: {file_type}")
    except Exception as e:
        print(f"{Colors.FG_RED}Failed to decompress {input_path}: {e}{Colors.RESET}")
        raise


def parse_xml(file_path):
    """Parse XML file and return root or None on failure."""
    print(f"{Colors.FG_CYAN}Parsing {file_path}...{Colors.RESET}")
    try:
        return ET.parse(file_path).getroot()
    except ET.ParseError as e:
        print(f"{Colors.FG_RED}Failed to parse XML '{file_path}': {e}{Colors.RESET}")
        return None


def get_primary_location_url(root, base_url):
    """Extract primary metadata location URL from repomd.xml root element."""
    ns = {"repo": "http://linux.duke.edu/metadata/repo"}
    for data in root.findall("repo:data", ns):
        if data.attrib.get("type") == "primary":
            location = data.find("repo:location", ns)
            if location is not None:
                href = location.attrib.get("href")
                if href:
                    return href if href.startswith("http") else urljoin(base_url, href)
    return None


def get_all_packages(root):
    ns = {"common": "http://linux.duke.edu/metadata/common"}
    return sorted(name.text for name in (pkg.find("common:name", ns) for pkg in root.findall("common:package", ns)) if name is not None)


def get_package_rpm_urls(root, base_url, package_names):
    """
    Retrieve RPM URLs for the given package names.
    Returns a list of tuples (package_name, rpm_url).
    """
    ns = {"common": "http://linux.duke.edu/metadata/common"}
    packages_by_name = defaultdict(list)

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

        packages_by_name[name_elem.text].append({
            "ver": version.attrib.get("ver"),
            "rel": version.attrib.get("rel"),
            "epoch": int(version.attrib.get("epoch", "0")),
            "href": href,
            "name": name_elem.text,
        })

    rpm_urls = []
    for pkg in package_names:
        entries = packages_by_name.get(pkg, [])
        if Config.ONLY_LATEST_VERSION:
            latest = max(entries, key=lambda e: (
                e["epoch"], e["ver"] or "", e["rel"] or ""), default=None)
            if latest:
                rpm_urls.append((pkg, urljoin(base_url, latest["href"])))
        else:
            rpm_urls.extend(
                (pkg, urljoin(base_url, e["href"])) for e in entries)

    return rpm_urls


def print_packages_tabular(packages, columns=None, column_width=None):
    """Print package names in a tabular format."""
    if not packages:
        print(f"{Colors.FG_RED}No packages found.{Colors.RESET}")
        return
    columns = columns or Config.PACKAGE_COLUMNS
    width = column_width or Config.PACKAGE_COLUMN_WIDTH
    for i, pkg in enumerate(packages, 1):
        print(f"{Colors.FG_MAGENTA}{pkg:<{width}}{Colors.RESET}", end="")
        if i % columns == 0:
            print()
    if len(packages) % columns:
        print()


def resolve_all_dependencies(pkg_name, dep_map):
    """Resolve all recursive dependencies of a package."""
    if pkg_name not in dep_map:
        return None
    to_install = set()
    queue = deque([pkg_name])
    while queue:
        current = queue.popleft()
        if current in to_install:
            continue
        to_install.add(current)
        queue.extend(dep for dep in dep_map.get(
            current, set()) if dep not in to_install)
    return to_install


def edit_configuration():
    """Provides an interactive menu for editing configuration variables."""
    config_keys = sorted(k for k in dir(Config) if k.isupper())
    key_map = {str(i + 1): k for i, k in enumerate(config_keys)}

    while True:
        Config.print_config()
        print(
            f"{Colors.FG_YELLOW}Select config key by number (or Enter to return):{Colors.RESET}")
        for i, key in enumerate(config_keys, 1):
            print(f"  {Colors.FG_CYAN}{i}{Colors.RESET}) {key}")
        choice = input(
            f"{Colors.FG_CYAN}Choice (number): {Colors.RESET}").strip()
        if not choice:
            break
        if choice not in key_map:
            print(
                f"{Colors.FG_RED}Invalid choice '{choice}', try again.{Colors.RESET}")
            continue
        key = key_map[choice]
        current = getattr(Config, key)
        new_value = input(
            f"{Colors.FG_CYAN}Enter new value for {key} (current: {current}): {Colors.RESET}").strip()

        try:
            if isinstance(current, bool):
                if new_value.lower() in {"true", "1", "yes", "y"}:
                    new_value = True
                elif new_value.lower() in {"false", "0", "no", "n"}:
                    new_value = False
                else:
                    print(
                        f"{Colors.FG_RED}Please enter a valid boolean (true/false).{Colors.RESET}")
                    continue
            elif isinstance(current, int):
                new_value = int(new_value)
        except ValueError:
            print(f"{Colors.FG_RED}Invalid input type for {key}.{Colors.RESET}")
            continue

        if not Config.set_config(key, new_value):
            print(f"{Colors.FG_RED}Failed to update config.{Colors.RESET}")


def get_package_selection(metadata_handler):
    """Prompt user for package filters and optional dependency expansion."""
    filters = input(
        f"{Colors.FG_CYAN}Enter package names or wildcards (comma-separated): {Colors.RESET}").strip()
    packages = []
    for f in filters.split(','):
        packages.extend(metadata_handler.filter_packages(f.strip()))
    if not packages:
        print(f"{Colors.FG_RED}No packages matched the filter.{Colors.RESET}")
        return []
    include_deps = input(
        f"{Colors.FG_CYAN}Include dependencies? (y/N): {Colors.RESET}").strip().lower() in ['y', 'yes', '1', 'true']
    all_packages = set(packages)
    if include_deps:
        for pkg in packages:
            deps = resolve_all_dependencies(pkg, metadata_handler.dep_map)
            if deps:
                all_packages.update(deps)
    return sorted(all_packages)


def download_packages(package_names, dep_map, primary_root, download_deps=False):
    """Download selected packages, optionally including dependencies."""
    os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
    to_download = set(package_names)
    if download_deps:
        for pkg in package_names:
            deps = resolve_all_dependencies(pkg, dep_map)
            if deps:
                to_download.update(deps)

    tqdm.write(
        f"{Colors.FG_CYAN}Downloading packages: {', '.join(sorted(to_download))}{Colors.RESET}")

    rpm_urls = []
    for pkg in to_download:
        urls = get_package_rpm_urls(primary_root, Config.REPO_BASE_URL, [pkg])
        if not urls:
            tqdm.write(
                f"{Colors.FG_RED}No RPM URLs found for {pkg}{Colors.RESET}")
            continue
        rpm_urls.extend(urls)

    downloader = Downloader(Config.DOWNLOADER)
    with tqdm(total=len(rpm_urls), desc="Downloading packages", unit="pkg") as progress_bar:
        for _, url in rpm_urls:
            dest_file = os.path.join(
                Config.DOWNLOAD_DIR, os.path.basename(url))
            if os.path.exists(dest_file):
                tqdm.write(
                    f"{Colors.FG_YELLOW}Already downloaded: {os.path.basename(url)}{Colors.RESET}")
                progress_bar.update(1)
                continue
            try:
                downloader.download(url, dest_file)
                tqdm.write(
                    f"{Colors.FG_GREEN}Downloaded: {os.path.basename(url)}{Colors.RESET}")
            except Exception as e:
                tqdm.write(
                    f"{Colors.FG_RED}Failed to download {os.path.basename(url)}: {e}{Colors.RESET}")
            progress_bar.update(1)


def list_packages(metadata_handler, _):
    filters = input(
        f"{Colors.FG_CYAN}Enter filter string(s) with wildcards (comma-separated): {Colors.RESET}").strip()
    filtered = []
    for f in filters.split(','):
        filtered.extend(metadata_handler.filter_packages(f.strip()))
    print_packages_tabular(sorted(set(filtered)))


def calculate_dependencies(metadata_handler, _):
    pkg = input(f"{Colors.FG_CYAN}Enter package name: {Colors.RESET}").strip()
    if pkg not in metadata_handler.dep_map:
        print(f"{Colors.FG_RED}Package '{pkg}' not found.{Colors.RESET}")
        return
    resolved = resolve_all_dependencies(pkg, metadata_handler.dep_map)
    if not resolved:
        print(f"{Colors.FG_RED}Could not resolve dependencies for {pkg}{Colors.RESET}")
        return
    print(f"{Colors.FG_GREEN}Dependencies for {pkg} (including itself):{Colors.RESET}")
    print_packages_tabular(sorted(resolved))


def refresh_metadata(metadata_handler, _):
    metadata_handler.check_and_refresh_metadata()
    primary_root = parse_xml(Config.LOCAL_XML_FILE)
    if primary_root is None:
        print(f"{Colors.FG_RED}Failed to parse primary metadata XML.{Colors.RESET}")
        return
    metadata_handler.all_packages = get_all_packages(primary_root)
    metadata_handler.requires_map, metadata_handler.provides_map, metadata_handler.dep_map = metadata_handler.build_maps(
        primary_root)


def cleanup_metadata(metadata_handler, _):
    metadata_handler.cleanup_files()


def list_rpm_urls(metadata_handler, primary_root):
    selected_packages = get_package_selection(metadata_handler)
    if not selected_packages:
        return
    rpm_urls = get_package_rpm_urls(
        primary_root, Config.REPO_BASE_URL, selected_packages)
    if not rpm_urls:
        print(f"{Colors.FG_RED}No RPM URLs found for selected packages.{Colors.RESET}")
        return
    for pkg, url in rpm_urls:
        print(f"{Colors.FG_MAGENTA}{pkg:<30}{Colors.FG_CYAN}{url}{Colors.RESET}")


def download_packages_ui(metadata_handler, primary_root):
    selected_packages = get_package_selection(metadata_handler)
    if not selected_packages:
        return
    download_packages(selected_packages, metadata_handler.dep_map,
                      primary_root, download_deps=False)


def configure_settings(*_):
    edit_configuration()


def exit_program(*_):
    print(f"{Colors.FG_GREEN}Goodbye!{Colors.RESET}")
    exit(0)


def main():
    metadata_handler = MetadataHandler()
    metadata_handler.check_and_refresh_metadata()
    primary_root = parse_xml(Config.LOCAL_XML_FILE)
    if primary_root is None:
        print(
            f"{Colors.FG_RED}Failed to parse primary metadata XML file, exiting.{Colors.RESET}")
        return
    metadata_handler.all_packages = get_all_packages(primary_root)
    metadata_handler.requires_map, metadata_handler.provides_map, metadata_handler.dep_map = metadata_handler.build_maps(
        primary_root)

    menu_options = {
        "1": list_packages,
        "2": calculate_dependencies,
        "3": refresh_metadata,
        "4": cleanup_metadata,
        "5": list_rpm_urls,
        "6": download_packages_ui,
        "9": configure_settings,
        "0": exit_program
    }

    while True:
        print(f"\n{Colors.BOLD}{Colors.FG_BLUE}--- MENU ---{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}1) List packages by wildcard or list{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}2) Calculate dependencies for package{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}3) Refresh metadata files if missing{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}4) Cleanup metadata files{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}5) List RPM URLs by wildcard or list{Colors.RESET}")
        print(
            f"{Colors.FG_YELLOW}6) Download packages by wildcard or list{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}9) Configure settings{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}0) Exit{Colors.RESET}")

        choice = input(
            f"{Colors.FG_CYAN}Enter your choice: {Colors.RESET}").strip()
        action = menu_options.get(choice)
        if action:
            try:
                action(metadata_handler, primary_root)
            except Exception as e:
                print(f"{Colors.FG_RED}Error during operation: {e}{Colors.RESET}")
        else:
            print(f"{Colors.FG_RED}Invalid choice, please try again.{Colors.RESET}")


if __name__ == "__main__":
    main()
