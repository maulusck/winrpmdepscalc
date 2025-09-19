import subprocess
import os
import lzma
import gzip
import bz2
import magic
import fnmatch
from tqdm import tqdm
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from collections import deque, defaultdict


class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"

    FG_RED = "\033[31m"
    FG_GREEN = "\033[32m"
    FG_YELLOW = "\033[33m"
    FG_BLUE = "\033[34m"
    FG_MAGENTA = "\033[35m"
    FG_CYAN = "\033[36m"
    FG_WHITE = "\033[97m"
    FG_BRIGHT_BLACK = "\033[90m"

    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_WHITE = "\033[107m"


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

    @classmethod
    def print_config(cls):
        print(
            f"\n{Colors.BOLD}{Colors.FG_CYAN}--- Current Configuration ---{Colors.RESET}")
        for attr in (a for a in dir(cls) if a.isupper() and not a.startswith('_')):
            print(
                f"{Colors.FG_YELLOW}{attr:20}{Colors.RESET} = {Colors.FG_GREEN}{getattr(cls, attr)}{Colors.RESET}")
        print(
            f"{Colors.BOLD}{Colors.FG_CYAN}-----------------------------{Colors.RESET}\n")

    @classmethod
    def set_config(cls, key, value):
        if hasattr(cls, key):
            setattr(cls, key, value)
            print(f"{Colors.FG_GREEN}Updated {key} to: {value}{Colors.RESET}")
            return True
        print(f"{Colors.FG_RED}Config key '{key}' not found.{Colors.RESET}")
        return False


class MetadataHandler:
    def __init__(self):
        self.all_packages = []
        self.requires_map = {}
        self.provides_map = defaultdict(set)
        self.dep_map = {}

    def reset_variables(self):
        self.all_packages.clear()
        self.requires_map.clear()
        self.provides_map.clear()
        self.dep_map.clear()
        print(f"{Colors.FG_CYAN}Metadata state has been reset.{Colors.RESET}")

    def check_and_refresh_metadata(self):
        files = [Config.LOCAL_REPOMD_FILE,
                 Config.LOCAL_XZ_FILE, Config.LOCAL_XML_FILE]
        missing_files = [f for f in files if not os.path.exists(f)]
        if missing_files:
            print(
                f"{Colors.FG_YELLOW}Metadata files missing or removed: {', '.join(missing_files)}{Colors.RESET}")
            print(f"{Colors.FG_CYAN}Refreshing metadata files now.{Colors.RESET}")
            repomd_url = Config.REPO_BASE_URL + Config.REPOMD_XML
            download_file_powershell(repomd_url, Config.LOCAL_REPOMD_FILE)
            repomd_root = parse_xml(Config.LOCAL_REPOMD_FILE)
            primary_url = get_primary_location_url(
                repomd_root, Config.REPO_BASE_URL)
            if not primary_url:
                raise RuntimeError(
                    "Could not find primary metadata URL in repomd.xml")
            download_file_powershell(primary_url, Config.LOCAL_XZ_FILE)
            decompress_file(Config.LOCAL_XZ_FILE, Config.LOCAL_XML_FILE)
        else:
            print(
                f"{Colors.FG_GREEN}All metadata files present, skipping refresh.{Colors.RESET}")
        self.reset_variables()

    def cleanup_files(self):
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
        self.reset_variables()

    def build_maps(self, root_element):
        ns = {"common": "http://linux.duke.edu/metadata/common",
              "rpm": "http://linux.duke.edu/metadata/rpm"}
        provides_map = defaultdict(set)
        requires_map = {}
        packages_with_format = []
        for package in root_element.findall("common:package", ns):
            name_elem = package.find("common:name", ns)
            if name_elem is None:
                continue
            pkg_name = name_elem.text
            format_elem = package.find("common:format", ns)
            if format_elem is None:
                requires_map[pkg_name] = set()
                continue
            provides = format_elem.find("rpm:provides", ns)
            if provides is not None:
                for entry in provides.findall("rpm:entry", ns):
                    pname = entry.get("name")
                    if pname:
                        provides_map[pname].add(pkg_name)
            packages_with_format.append((pkg_name, format_elem))
        for pkg_name, format_elem in packages_with_format:
            requires = format_elem.find("rpm:requires", ns)
            reqs = {entry.get("name") for entry in requires.findall(
                "rpm:entry", ns)} if requires is not None else set()
            if Config.SUPPORT_WEAK_DEPS:
                weak_requires = format_elem.find("rpm:weakrequires", ns)
                if weak_requires is not None:
                    reqs.update({entry.get("name")
                                for entry in weak_requires.findall("rpm:entry", ns)})
            requires_map[pkg_name] = reqs
        dep_map = {pkg_name: {dep for req in reqs if req in provides_map for dep in provides_map[req]}
                   for pkg_name, reqs in requires_map.items()}
        return requires_map, provides_map, dep_map

    def filter_packages_by_input(self, input_str):
        pattern = fnmatch.fnmatch
        parts = [part.strip() for part in input_str.split(',')]
        filtered_packages = {
            pkg for pkg in self.all_packages for part in parts if pattern(pkg, part)}
        return sorted(filtered_packages)


def download_file_powershell(url, output_file):
    ps_script = f"""
    $wc = New-Object System.Net.WebClient
    $wc.Proxy.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials
    $wc.DownloadFile('{url}', '{output_file}')
    """
    ps_command = ["powershell", "-NoProfile", "-Command", ps_script]
    tqdm.write(
        f"{Colors.FG_CYAN}Downloading {url} to {output_file} ...{Colors.RESET}")
    result = subprocess.run(ps_command, capture_output=True, text=True)
    if result.returncode != 0:
        tqdm.write(
            f"{Colors.FG_RED}PowerShell error output:\n{result.stderr}{Colors.RESET}")
        raise RuntimeError(f"Failed to download {url}")


def decompress_file(input_path, output_path):
    print(f"{Colors.FG_CYAN}Decompressing {input_path} to {output_path}...{Colors.RESET}")
    try:
        file_type = magic.from_file(input_path)
        if "XZ compressed" in file_type:
            with lzma.open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
                f_out.write(f_in.read())
            print(f"{Colors.FG_GREEN}Decompressed using lzma (.xz).{Colors.RESET}")
        elif "gzip compressed" in file_type:
            with gzip.open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
                f_out.write(f_in.read())
            print(f"{Colors.FG_GREEN}Decompressed using gzip (.gz).{Colors.RESET}")
        elif "bzip2 compressed" in file_type:
            with bz2.open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
                f_out.write(f_in.read())
            print(f"{Colors.FG_GREEN}Decompressed using bz2 (.bz2).{Colors.RESET}")
        else:
            print(
                f"{Colors.FG_RED}Unsupported compression format: {file_type}{Colors.RESET}")
            raise RuntimeError(f"Unsupported compression format: {file_type}")
    except Exception as e:
        print(f"{Colors.FG_RED}Failed to decompress {input_path}: {e}{Colors.RESET}")
        raise


def parse_xml(file_path):
    print(f"{Colors.FG_CYAN}Parsing {file_path}...{Colors.RESET}")
    return ET.parse(file_path).getroot()


def get_primary_location_url(root, base_url):
    ns = {"repo": "http://linux.duke.edu/metadata/repo"}
    for data in root.findall("repo:data", ns):
        if data.attrib.get("type") == "primary":
            location = data.find("repo:location", ns)
            if location is not None:
                href = location.attrib.get("href")
                if href:
                    return href if href.startswith("http") else base_url + href
    return None


def get_all_packages(root_element):
    ns = {"common": "http://linux.duke.edu/metadata/common"}
    return sorted(
        name_elem.text for name_elem in
        (pkg.find("common:name", ns)
         for pkg in root_element.findall("common:package", ns))
        if name_elem is not None
    )


def get_package_rpm_urls(root_element, base_url, package_names):
    ns = {"common": "http://linux.duke.edu/metadata/common"}
    packages_by_name = defaultdict(list)
    for package in root_element.findall("common:package", ns):
        name_elem = package.find("common:name", ns)
        if name_elem is None or name_elem.text not in package_names:
            continue
        version_elem = package.find("common:version", ns)
        location_elem = package.find("common:location", ns)
        if version_elem is None or location_elem is None:
            continue
        href = location_elem.attrib.get("href")
        if not href:
            continue
        packages_by_name[name_elem.text].append({
            "ver": version_elem.attrib.get("ver"),
            "rel": version_elem.attrib.get("rel"),
            "epoch": int(version_elem.attrib.get("epoch", "0")),
            "href": href,
            "name": name_elem.text
        })
    rpm_urls = []
    for pkg_name in package_names:
        entries = packages_by_name.get(pkg_name, [])
        if Config.ONLY_LATEST_VERSION:
            latest = max(entries, key=lambda e: (
                e["epoch"], e["ver"] or "", e["rel"] or ""), default=None)
            if latest:
                rpm_urls.append((pkg_name, urljoin(base_url, latest["href"])))
        else:
            rpm_urls.extend(
                (pkg_name, urljoin(base_url, e["href"])) for e in entries)
    return rpm_urls


def print_packages_tabular(packages, columns=None, column_width=None):
    if not packages:
        print(f"{Colors.FG_RED}No packages found.{Colors.RESET}")
        return
    columns = columns or Config.PACKAGE_COLUMNS
    column_width = column_width or Config.PACKAGE_COLUMN_WIDTH
    for i, pkg in enumerate(packages, 1):
        print(f"{Colors.FG_MAGENTA}{pkg:<{column_width}}{Colors.RESET}", end="")
        if i % columns == 0:
            print()
    if len(packages) % columns != 0:
        print()


def whatrequires(pkg_name, requires_map):
    return requires_map.get(pkg_name, set())


def whatprovides(file_name, provides_map):
    return provides_map.get(file_name, set())


def resolve_all_dependencies(pkg_name, dep_map):
    if pkg_name not in dep_map:
        return None
    to_install, queue = set(), deque([pkg_name])
    while queue:
        current = queue.popleft()
        if current in to_install:
            continue
        to_install.add(current)
        queue.extend(dep for dep in dep_map.get(
            current, set()) if dep not in to_install)
    return to_install


def configure_settings():
    config_keys = [attr for attr in dir(Config) if attr.isupper()]
    key_map = {str(i + 1): key for i, key in enumerate(config_keys)}
    while True:
        Config.print_config()
        print(f"{Colors.FG_YELLOW}Select the config key to change by number (or press Enter to return to menu):{Colors.RESET}")
        for i, key in enumerate(config_keys, 1):
            print(f"  {Colors.FG_CYAN}{i}{Colors.RESET}) {key}")
        choice = input(
            f"{Colors.FG_CYAN}Choice (number): {Colors.RESET}").strip()
        if not choice:
            break
        if choice not in key_map:
            print(
                f"{Colors.FG_RED}Invalid choice '{choice}'. Please enter a valid number.{Colors.RESET}")
            continue
        key = key_map[choice]
        current_value = getattr(Config, key)
        new_value = input(
            f"{Colors.FG_CYAN}Enter new value for {key} (current: {current_value}): {Colors.RESET}").strip()
        if isinstance(current_value, bool):
            if new_value.lower() in ["true", "1", "yes", "y"]:
                new_value = True
            elif new_value.lower() in ["false", "0", "no", "n"]:
                new_value = False
            else:
                print(
                    f"{Colors.FG_RED}Please enter a valid boolean (true/false).{Colors.RESET}")
                continue
        elif isinstance(current_value, int):
            if not new_value.isdigit():
                print(f"{Colors.FG_RED}Please enter a valid integer.{Colors.RESET}")
                continue
            new_value = int(new_value)
        Config.set_config(key, new_value)


def download_packages(package_names, dep_map, primary_root, download_deps=False):
    os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
    packages_to_download = set(package_names)
    if download_deps:
        for pkg in package_names:
            deps = resolve_all_dependencies(pkg, dep_map)
            if deps:
                packages_to_download.update(deps)
    tqdm.write(
        f"{Colors.FG_CYAN}Downloading the following packages: {', '.join(packages_to_download)}{Colors.RESET}")

    all_rpm_urls = []
    for pkg in packages_to_download:
        rpm_urls = get_package_rpm_urls(
            primary_root, Config.REPO_BASE_URL, [pkg])
        if not rpm_urls:
            tqdm.write(
                f"{Colors.FG_RED}No RPM URLs found for {pkg}{Colors.RESET}")
            continue
        all_rpm_urls.extend(rpm_urls)

    with tqdm(total=len(all_rpm_urls), desc="Downloading packages", unit="pkg", position=0, leave=True) as pbar:
        for _, url in all_rpm_urls:
            dest_path = os.path.join(
                Config.DOWNLOAD_DIR, os.path.basename(url))
            if os.path.exists(dest_path):
                tqdm.write(
                    f"{Colors.FG_YELLOW}Already downloaded: {os.path.basename(url)}{Colors.RESET}")
                pbar.update(1)
                continue
            try:
                download_file_powershell(url, dest_path)
                tqdm.write(
                    f"{Colors.FG_GREEN}Downloaded: {os.path.basename(url)}{Colors.RESET}")
            except Exception as e:
                tqdm.write(
                    f"{Colors.FG_RED}Failed to download {os.path.basename(url)}: {e}{Colors.RESET}")
            pbar.update(1)


def main():
    metadata_handler = MetadataHandler()
    metadata_handler.check_and_refresh_metadata()
    primary_root = parse_xml(Config.LOCAL_XML_FILE)
    metadata_handler.all_packages = get_all_packages(primary_root)
    metadata_handler.requires_map, metadata_handler.provides_map, metadata_handler.dep_map = metadata_handler.build_maps(
        primary_root)
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

        if choice == "1":
            filtered = []
            for filter_str in input(f"{Colors.FG_CYAN}Enter a string to filter RPM package names (use '*' for wildcards, comma-separated for multiple): {Colors.RESET}").strip().split(','):
                filtered.extend(
                    metadata_handler.filter_packages_by_input(filter_str.strip()))
            print_packages_tabular(sorted(set(filtered)))
        elif choice == "2":
            pkg = input(
                f"{Colors.FG_CYAN}Enter package name: {Colors.RESET}").strip()
            if pkg not in metadata_handler.dep_map:
                print(f"{Colors.FG_RED}Package '{pkg}' not found.{Colors.RESET}")
                continue
            resolved = resolve_all_dependencies(pkg, metadata_handler.dep_map)
            if resolved is None:
                print(
                    f"{Colors.FG_RED}Could not resolve dependencies for {pkg}{Colors.RESET}")
                continue
            print(
                f"{Colors.FG_GREEN}Dependencies for {pkg} (including package itself):{Colors.RESET}")
            print_packages_tabular(sorted(resolved))
        elif choice == "3":
            metadata_handler.check_and_refresh_metadata()
            primary_root = parse_xml(Config.LOCAL_XML_FILE)
            metadata_handler.all_packages = get_all_packages(primary_root)
            metadata_handler.requires_map, metadata_handler.provides_map, metadata_handler.dep_map = metadata_handler.build_maps(
                primary_root)
        elif choice == "4":
            metadata_handler.cleanup_files()
        elif choice == "5":
            start = input(
                f"{Colors.FG_CYAN}Enter a string to filter RPM package names (use '*' for wildcards, comma-separated for multiple): {Colors.RESET}").strip()
            package_names = metadata_handler.filter_packages_by_input(start)
            rpm_urls = get_package_rpm_urls(
                primary_root, Config.REPO_BASE_URL, package_names)
            if not rpm_urls:
                print(
                    f"{Colors.FG_RED}No RPM URLs found with the given filter.{Colors.RESET}")
            else:
                for pkg_name, url in rpm_urls:
                    print(
                        f"{Colors.FG_MAGENTA}{pkg_name:<30}{Colors.FG_CYAN}{url}{Colors.RESET}")
        elif choice == "6":
            packages = []
            for filter_str in input(f"{Colors.FG_CYAN}Enter package names or wildcard (e.g., 'vim-*,chromium,*vlc*'): {Colors.RESET}").strip().split(','):
                packages.extend(
                    metadata_handler.filter_packages_by_input(filter_str.strip()))
            download_deps = input(
                f"{Colors.FG_CYAN}Do you want to download dependencies as well? (y/N): {Colors.RESET}").strip().lower() in ['y', 'yes', '1', 'true']
            download_packages(packages, metadata_handler.dep_map,
                              primary_root, download_deps)
        elif choice == "9":
            configure_settings()
        elif choice == "0":
            print(f"{Colors.FG_GREEN}Goodbye!{Colors.RESET}")
            break
        else:
            print(f"{Colors.FG_RED}Invalid choice, please try again.{Colors.RESET}")


if __name__ == "__main__":
    main()
