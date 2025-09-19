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
import requests
import magic
from tqdm import tqdm
from enum import Enum
import argparse
import sys
import yaml
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    FG_RED = "\033[31m"
    FG_GREEN = "\033[32m"
    FG_YELLOW = "\033[33m"
    FG_CYAN = "\033[36m"
    FG_MAGENTA = "\033[35m"
    FG_BLUE = "\033[34m"


class DownloaderType(Enum):
    POWERSHELL = "powershell"
    PYTHON = "python"

    @classmethod
    def has_value(cls, value):
        return value.lower() in (item.value for item in cls)


class Downloader:
    def __init__(self, downloader_type="powershell", proxy_url=None):
        dt = downloader_type.lower()
        if not DownloaderType.has_value(dt):
            allowed = ', '.join([d.value for d in DownloaderType])
            raise ValueError(
                f"Invalid downloader '{downloader_type}'. Allowed: {allowed}")
        self.downloader_type = DownloaderType(dt)
        self.proxy_url = proxy_url

    def download(self, url, output_file):
        if self.downloader_type == DownloaderType.POWERSHELL:
            self._download_powershell(url, output_file)
        else:
            self._download_python(url, output_file)

    def _download_powershell(self, url, output_file):
        ps_script = f"""
        $wc = New-Object System.Net.WebClient
        $wc.Proxy.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials
        $wc.DownloadFile('{url}', '{output_file}')
        """
        result = subprocess.run(["powershell", "-NoProfile", "-Command", ps_script],
                                capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"PowerShell download failed:\n{result.stderr}")

    def _download_python(self, url, output_file):
        proxies = {}
        if self.proxy_url:
            proxies = {"http": self.proxy_url, "https": self.proxy_url}
        else:
            system_proxies = urllib.request.getproxies()
            proxies = {k: v for k, v in system_proxies.items()
                       if k in ("http", "https")}

        session = requests.Session()
        with session.get(url, stream=True, proxies=proxies or None, verify=False) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            with open(output_file, 'wb') as f, tqdm(total=total, unit='iB', unit_scale=True, desc=os.path.basename(output_file)) as bar:
                for chunk in resp.iter_content(1024):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))


class Config:
    def __init__(self):
        self.REPO_BASE_URL = "https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/"
        self.REPOMD_XML = "repodata/repomd.xml"
        self.LOCAL_REPOMD_FILE = "repomd.xml"
        self.LOCAL_XZ_FILE = "primary.xml.xz"
        self.LOCAL_XML_FILE = "primary.xml"
        self.PACKAGE_COLUMNS = 4
        self.PACKAGE_COLUMN_WIDTH = 30
        self.DOWNLOAD_DIR = "rpms"
        self.SUPPORT_WEAK_DEPS = False
        self.ONLY_LATEST_VERSION = True
        self.DOWNLOADER = "powershell"

    def update_from_dict(self, data):
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)

    def to_dict(self):
        return {k: getattr(self, k) for k in dir(self) if k.isupper()}


class MetadataManager:
    def __init__(self, config: Config, downloader: Downloader):
        self.config = config
        self.downloader = downloader
        self.all_packages = []
        self.requires_map = {}
        self.provides_map = defaultdict(set)
        self.dep_map = {}

    def check_and_refresh_metadata(self):
        required_files = [self.config.LOCAL_REPOMD_FILE,
                          self.config.LOCAL_XZ_FILE, self.config.LOCAL_XML_FILE]
        missing = [f for f in required_files if not os.path.exists(f)]

        if missing:
            print(
                f"{Colors.FG_YELLOW}Missing metadata files: {', '.join(missing)}{Colors.RESET}")
            print(f"{Colors.FG_CYAN}Refreshing metadata...{Colors.RESET}")

            repomd_url = urljoin(self.config.REPO_BASE_URL,
                                 self.config.REPOMD_XML)
            self.downloader.download(repomd_url, self.config.LOCAL_REPOMD_FILE)

            repomd_root = self._parse_xml(self.config.LOCAL_REPOMD_FILE)
            if repomd_root is None:
                raise RuntimeError("Failed to parse repomd.xml")

            primary_url = self._get_primary_location_url(repomd_root)
            if not primary_url:
                raise RuntimeError("Primary URL not found in repomd.xml")

            self.downloader.download(primary_url, self.config.LOCAL_XZ_FILE)
            self._decompress_file(self.config.LOCAL_XZ_FILE,
                                  self.config.LOCAL_XML_FILE)
        else:
            print(
                f"{Colors.FG_GREEN}All metadata files present, skipping refresh.{Colors.RESET}")

        self._reset_metadata_state()
        self._load_metadata_maps()

    def cleanup_files(self):
        files = [self.config.LOCAL_REPOMD_FILE,
                 self.config.LOCAL_XZ_FILE, self.config.LOCAL_XML_FILE]
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
        self._reset_metadata_state()

    def _reset_metadata_state(self):
        self.all_packages.clear()
        self.requires_map.clear()
        self.provides_map.clear()
        self.dep_map.clear()

    def _parse_xml(self, path):
        print(f"{Colors.FG_CYAN}Parsing XML file {path}{Colors.RESET}")
        try:
            return ET.parse(path).getroot()
        except ET.ParseError as e:
            print(f"{Colors.FG_RED}Failed to parse XML {path}: {e}{Colors.RESET}")
            return None

    def _get_primary_location_url(self, repomd_root):
        ns = {"repo": "http://linux.duke.edu/metadata/repo"}
        for data in repomd_root.findall("repo:data", ns):
            if data.attrib.get("type") == "primary":
                location = data.find("repo:location", ns)
                if location is not None:
                    href = location.attrib.get("href")
                    if href:
                        return href if href.startswith("http") else urljoin(self.config.REPO_BASE_URL, href)
        return None

    def _decompress_file(self, input_path, output_path):
        print(
            f"{Colors.FG_CYAN}Decompressing {input_path} to {output_path}...{Colors.RESET}")
        try:
            file_type = magic.from_file(input_path)
            if "XZ compressed" in file_type:
                opener = lzma.open
            elif "gzip compressed" in file_type:
                opener = gzip.open
            elif "bzip2 compressed" in file_type:
                opener = bz2.open
            else:
                raise RuntimeError(
                    f"Unsupported compression format: {file_type}")
            with opener(input_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
                f_out.write(f_in.read())
            print(f"{Colors.FG_GREEN}Decompression complete.{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.FG_RED}Failed to decompress: {e}{Colors.RESET}")
            raise

    def _load_metadata_maps(self):
        root = self._parse_xml(self.config.LOCAL_XML_FILE)
        if root is None:
            raise RuntimeError("Failed to load primary XML metadata")

        ns = {"common": "http://linux.duke.edu/metadata/common",
              "rpm": "http://linux.duke.edu/metadata/rpm"}

        self.all_packages = sorted(
            pkg.find("common:name", ns).text for pkg in root.findall("common:package", ns) if pkg.find("common:name", ns) is not None
        )

        self.requires_map.clear()
        self.provides_map.clear()
        pkgs_with_format = []

        for pkg in root.findall("common:package", ns):
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
            req_set = {entry.get("name") for entry in req.findall(
                "rpm:entry", ns)} if req is not None else set()
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

    def filter_packages(self, patterns):
        patterns = [p.strip() for p in patterns if p.strip()]
        filtered = set()
        for pkg in self.all_packages:
            if any(fnmatch.fnmatch(pkg, pat) for pat in patterns):
                filtered.add(pkg)
        return sorted(filtered)

    def resolve_all_dependencies(self, pkg_name):
        if pkg_name not in self.dep_map:
            return None
        to_install = set()
        queue = deque([pkg_name])
        while queue:
            current = queue.popleft()
            if current in to_install:
                continue
            to_install.add(current)
            queue.extend(dep for dep in self.dep_map.get(
                current, set()) if dep not in to_install)
        return to_install


def print_packages_tabular(packages, columns=4, column_width=30):
    if not packages:
        print(f"{Colors.FG_RED}No packages found.{Colors.RESET}")
        return
    for i, pkg in enumerate(packages, start=1):
        print(f"{Colors.FG_MAGENTA}{pkg:<{column_width}}{Colors.RESET}", end='')
        if i % columns == 0:
            print()
    if len(packages) % columns != 0:
        print()


def get_package_rpm_urls(root, base_url, package_names, only_latest=True):
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
        if only_latest:

            latest = max(entries, key=lambda e: (
                e["epoch"], e["ver"] or "", e["rel"] or ""), default=None)
            if latest:
                rpm_urls.append((pkg, urljoin(base_url, latest["href"])))
        else:
            for e in entries:
                rpm_urls.append((pkg, urljoin(base_url, e["href"])))

    return rpm_urls


def download_packages(package_names, dep_map, primary_root, config: Config, downloader: Downloader, download_deps=False):
    os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)
    packages_to_download = set(package_names)
    if download_deps:
        for pkg in package_names:
            deps = resolve_all_dependencies(pkg, dep_map)
            if deps:
                packages_to_download.update(deps)

    tqdm.write(
        f"{Colors.FG_CYAN}Downloading packages: {', '.join(sorted(packages_to_download))}{Colors.RESET}")

    rpm_urls = []
    for pkg in packages_to_download:
        urls = get_package_rpm_urls(primary_root, config.REPO_BASE_URL, [
                                    pkg], only_latest=config.ONLY_LATEST_VERSION)
        if not urls:
            tqdm.write(
                f"{Colors.FG_RED}No RPM URLs found for {pkg}{Colors.RESET}")
            continue
        rpm_urls.extend(urls)

    with tqdm(total=len(rpm_urls), desc="Downloading packages", unit="pkg") as bar:
        for _, url in rpm_urls:
            dest_file = os.path.join(
                config.DOWNLOAD_DIR, os.path.basename(url))
            if os.path.exists(dest_file):
                tqdm.write(
                    f"{Colors.FG_YELLOW}Already downloaded: {os.path.basename(url)}{Colors.RESET}")
                bar.update(1)
                continue
            try:
                downloader.download(url, dest_file)
                tqdm.write(
                    f"{Colors.FG_GREEN}Downloaded: {os.path.basename(url)}{Colors.RESET}")
            except Exception as e:
                tqdm.write(
                    f"{Colors.FG_RED}Failed to download {os.path.basename(url)}: {e}{Colors.RESET}")
            bar.update(1)


def resolve_all_dependencies(pkg_name, dep_map):
    if pkg_name not in dep_map:
        return None
    to_install = set()
    queue = deque([pkg_name])
    while queue:
        current = queue.popleft()
        if current in to_install:
            continue
        to_install.add(current)
        for dep in dep_map.get(current, set()):
            if dep not in to_install:
                queue.append(dep)
    return to_install


def load_config_file(config_path, config: Config):
    if not os.path.exists(config_path):
        print(
            f"{Colors.FG_YELLOW}Config file '{config_path}' not found, using defaults.{Colors.RESET}")
        return
    try:
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        if data:
            config.update_from_dict(data)
            print(f"{Colors.FG_GREEN}Loaded config from {config_path}{Colors.RESET}")
        else:
            print(
                f"{Colors.FG_YELLOW}Config file {config_path} is empty, using defaults.{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.FG_RED}Failed to load config {config_path}: {e}{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}Continuing with default configuration.{Colors.RESET}")


def write_default_config(config_path, config: Config):
    default_data = config.to_dict()
    try:
        with open(config_path, 'w') as f:
            yaml.safe_dump(default_data, f, sort_keys=False)
        print(f"{Colors.FG_GREEN}Default config written to {config_path}{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.FG_RED}Failed to write default config: {e}{Colors.RESET}")


def print_config(config: Config):
    print(
        f"\n{Colors.BOLD}{Colors.FG_CYAN}--- Current Configuration ---{Colors.RESET}")
    for key in sorted(k for k in dir(config) if k.isupper()):
        print(f"{Colors.FG_YELLOW}{key:20}{Colors.RESET} = {Colors.FG_GREEN}{getattr(config, key)}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.FG_CYAN}-----------------------------{Colors.RESET}\n")


def edit_configuration(config: Config, config_path=None):
    keys = sorted(k for k in dir(config) if k.isupper())
    key_map = {str(i + 1): k for i, k in enumerate(keys)}

    while True:
        print_config(config)
        print(
            f"{Colors.FG_YELLOW}Select config key by number (Enter to exit):{Colors.RESET}")
        for i, key in enumerate(keys, 1):
            print(f"  {Colors.FG_CYAN}{i}{Colors.RESET}) {key}")
        choice = input(f"{Colors.FG_CYAN}Your choice: {Colors.RESET}").strip()
        if not choice:
            break
        if choice not in key_map:
            print(f"{Colors.FG_RED}Invalid choice.{Colors.RESET}")
            continue
        key = key_map[choice]
        current_val = getattr(config, key)
        new_val = input(
            f"{Colors.FG_CYAN}Enter new value for {key} (current: {current_val}): {Colors.RESET}").strip()
        try:
            if isinstance(current_val, bool):
                new_val_lower = new_val.lower()
                if new_val_lower in {"true", "1", "yes", "y"}:
                    new_val = True
                elif new_val_lower in {"false", "0", "no", "n"}:
                    new_val = False
                else:
                    print(f"{Colors.FG_RED}Invalid boolean value.{Colors.RESET}")
                    continue
            elif isinstance(current_val, int):
                new_val = int(new_val)
        except ValueError:
            print(f"{Colors.FG_RED}Invalid value type.{Colors.RESET}")
            continue
        setattr(config, key, new_val)
        print(f"{Colors.FG_GREEN}Updated {key} to {new_val}.{Colors.RESET}")

    if config_path:
        save_choice = input(
            f"{Colors.FG_YELLOW}Save changes to config file '{config_path}'? (y/N): {Colors.RESET}").strip().lower()
        if save_choice in ("y", "yes"):
            write_default_config(config_path, config)
            print(f"{Colors.FG_GREEN}Configuration saved.{Colors.RESET}")
        else:
            print(f"{Colors.FG_YELLOW}Changes not saved.{Colors.RESET}")


def prompt_package_selection(metadata: MetadataManager):
    filters = input(
        f"{Colors.FG_CYAN}Enter package names/wildcards (comma-separated): {Colors.RESET}").strip()
    patterns = [p.strip() for p in filters.split(',') if p.strip()]
    selected = metadata.filter_packages(patterns)
    if not selected:
        print(f"{Colors.FG_RED}No packages matched.{Colors.RESET}")
        return []
    include_deps = input(
        f"{Colors.FG_CYAN}Include dependencies? (y/N): {Colors.RESET}").strip().lower() in {'y', 'yes', '1', 'true'}
    all_pkgs = set(selected)
    if include_deps:
        for pkg in selected:
            deps = metadata.resolve_all_dependencies(pkg)
            if deps:
                all_pkgs.update(deps)
    return sorted(all_pkgs)


def list_packages(metadata: MetadataManager, *_):
    patterns = input(
        f"{Colors.FG_CYAN}Enter wildcard filters (comma-separated): {Colors.RESET}").strip().split(",")
    packages = metadata.filter_packages(patterns)
    print_packages_tabular(
        packages, metadata.config.PACKAGE_COLUMNS, metadata.config.PACKAGE_COLUMN_WIDTH)


def calc_dependencies(metadata: MetadataManager, *_):
    pkg = input(f"{Colors.FG_CYAN}Enter package name: {Colors.RESET}").strip()
    if not pkg or pkg not in metadata.dep_map:
        print(f"{Colors.FG_RED}Package not found.{Colors.RESET}")
        return
    deps = metadata.resolve_all_dependencies(pkg)
    if not deps:
        print(f"{Colors.FG_RED}Cannot resolve dependencies.{Colors.RESET}")
        return
    print(f"{Colors.FG_GREEN}Dependencies for {pkg}:{Colors.RESET}")
    print_packages_tabular(sorted(
        deps), metadata.config.PACKAGE_COLUMNS, metadata.config.PACKAGE_COLUMN_WIDTH)


def refresh_metadata(metadata: MetadataManager, *_):
    metadata.check_and_refresh_metadata()


def cleanup_metadata(metadata: MetadataManager, *_):
    metadata.cleanup_files()


def list_rpm_urls(metadata: MetadataManager, primary_root=None):
    if primary_root is None:
        primary_root = metadata._parse_xml(metadata.config.LOCAL_XML_FILE)
        if primary_root is None:
            print(
                f"{Colors.FG_RED}Failed to parse primary metadata XML.{Colors.RESET}")
            return

    selected = prompt_package_selection(metadata)
    if not selected:
        return

    urls = get_package_rpm_urls(
        primary_root, metadata.config.REPO_BASE_URL,
        selected, only_latest=metadata.config.ONLY_LATEST_VERSION
    )
    if not urls:
        print(f"{Colors.FG_RED}No RPM URLs found.{Colors.RESET}")
        return

    for pkg, url in urls:
        print(f"{Colors.FG_MAGENTA}{pkg:<30}{Colors.FG_CYAN}{url}{Colors.RESET}")


def download_packages_ui(metadata: MetadataManager, primary_root=None):
    if primary_root is None:
        primary_root = metadata._parse_xml(metadata.config.LOCAL_XML_FILE)
        if primary_root is None:
            print(
                f"{Colors.FG_RED}Failed to parse primary metadata XML.{Colors.RESET}")
            return

    selected = prompt_package_selection(metadata)
    if not selected:
        return

    download_packages(selected, metadata.dep_map, primary_root,
                      metadata.config, metadata.downloader, download_deps=False)


def configure_settings(metadata: MetadataManager, _, config_path=None):

    edit_configuration(metadata.config, config_path)


def exit_program(*_):
    print(f"{Colors.FG_GREEN}Goodbye!{Colors.RESET}")
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


def run_interactive_menu(metadata: MetadataManager, config_path):
    while True:
        print(f"\n{Colors.BOLD}{Colors.FG_BLUE}--- MENU ---{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}1) List packages{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}2) Calculate dependencies{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}3) Refresh metadata files{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}4) Cleanup metadata files{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}5) List RPM URLs{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}6) Download packages{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}9) Configure settings{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}0) Exit{Colors.RESET}")
        choice = input(f"{Colors.FG_CYAN}Your choice: {Colors.RESET}").strip()
        action = MENU_ACTIONS.get(choice)
        if action:
            try:

                if action == configure_settings:
                    action(metadata, None, config_path)
                elif action in (list_rpm_urls, download_packages_ui):
                    action(metadata)
                else:
                    action(metadata, None)
            except Exception as e:
                print(f"{Colors.FG_RED}Error during operation: {e}{Colors.RESET}")
        else:
            print(f"{Colors.FG_RED}Invalid choice.{Colors.RESET}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Windows RPM Package Metadata Tool")
    parser.add_argument('--config', '-c', default="config.yaml",
                        help="YAML config file path")
    parser.add_argument('--write-default-config', action='store_true',
                        help="Write default config.yaml and exit")
    parser.add_argument('--list-packages', action='store_true',
                        help="List packages by wildcard or list")
    parser.add_argument('--calc-deps', metavar='PACKAGE',
                        help="Calculate dependencies for a package")
    parser.add_argument('--refresh-meta', action='store_true',
                        help="Refresh metadata files if missing")
    parser.add_argument('--cleanup-meta', action='store_true',
                        help="Cleanup metadata files")
    parser.add_argument('--list-rpm-urls', action='store_true',
                        help="List RPM URLs for packages")
    parser.add_argument('--download', action='store_true',
                        help="Download packages")
    parser.add_argument('--configure', action='store_true',
                        help="Configure settings interactively")
    parser.add_argument('--no-interactive',
                        action='store_true', help="Run non-interactive")
    return parser.parse_args()


def main():
    try:
        args = parse_args()
        config = Config()

        if args.write_default_config:
            write_default_config(args.config, config)
            return

        load_config_file(args.config, config)
        downloader = Downloader(config.DOWNLOADER)
        metadata = MetadataManager(config, downloader)

        metadata.check_and_refresh_metadata()

        primary_root = metadata._parse_xml(metadata.config.LOCAL_XML_FILE)

        if args.list_packages:
            list_packages(metadata)
        elif args.calc_deps:
            if args.calc_deps in metadata.dep_map:
                calc_dependencies(metadata)
            else:
                print(
                    f"{Colors.FG_RED}Package '{args.calc_deps}' not found.{Colors.RESET}")
        elif args.refresh_meta:
            refresh_metadata(metadata)
        elif args.cleanup_meta:
            cleanup_metadata(metadata)
        if args.list_rpm_urls:
            list_rpm_urls(metadata, primary_root)
        elif args.download:
            download_packages_ui(metadata, primary_root)
        elif args.configure:
            configure_settings(metadata, None, args.config)
        elif not args.no_interactive:
            run_interactive_menu(metadata, args.config)
        else:
            print(
                f"{Colors.FG_YELLOW}No operation specified and interactive mode disabled.{Colors.RESET}")

    except KeyboardInterrupt:
        print(
            f"\n{Colors.FG_YELLOW}Terminated by user (Ctrl+C). Exiting...{Colors.RESET}")
        sys.exit(0)


if __name__ == "__main__":
    main()
