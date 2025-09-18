import subprocess
import os
import lzma
import gzip
import bz2
import magic  # python-magic-bin
import fnmatch  # for wildcard matching
from urllib.parse import urljoin  # To handle URL joining properly
import xml.etree.ElementTree as ET
from collections import deque, defaultdict


# ANSI escape codes for colors and styles
class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"

    # Foreground colors
    FG_RED = "\033[31m"
    FG_GREEN = "\033[32m"
    FG_YELLOW = "\033[33m"
    FG_BLUE = "\033[34m"
    FG_MAGENTA = "\033[35m"
    FG_CYAN = "\033[36m"
    FG_WHITE = "\033[97m"
    FG_BRIGHT_BLACK = "\033[90m"

    # Background colors (if needed)
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
    SUPPORT_WEAK_DEPS = False  # default: disabled

    @classmethod
    def print_config(cls):
        print(
            f"\n{Colors.BOLD}{Colors.FG_CYAN}--- Current Configuration ---{Colors.RESET}"
        )
        for attr in dir(cls):
            if not attr.startswith("_") and attr.isupper():
                value = getattr(cls, attr)
                print(
                    f"{Colors.FG_YELLOW}{attr:20}{Colors.RESET} = {Colors.FG_GREEN}{value}{Colors.RESET}"
                )
        print(
            f"{Colors.BOLD}{Colors.FG_CYAN}-----------------------------{Colors.RESET}\n"
        )

    @classmethod
    def set_config(cls, key, value):
        if hasattr(cls, key):
            setattr(cls, key, value)
            print(f"{Colors.FG_GREEN}Updated {key} to: {value}{Colors.RESET}")
            return True
        else:
            print(f"{Colors.FG_RED}Config key '{key}' not found.{Colors.RESET}")
            return False


def download_file_powershell(url, output_file):
    ps_script = f"""
    $wc = New-Object System.Net.WebClient
    $wc.Proxy.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials
    $wc.DownloadFile('{url}', '{output_file}')
    """
    ps_command = ["powershell", "-NoProfile", "-Command", ps_script]
    print(f"{Colors.FG_CYAN}Downloading {url} to {output_file} ...{Colors.RESET}")
    result = subprocess.run(ps_command, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"{Colors.FG_RED}PowerShell error output:\n{result.stderr}{Colors.RESET}")
        raise RuntimeError(f"Failed to download {url}")


def decompress_file(input_path, output_path):
    print(
        f"{Colors.FG_CYAN}Decompressing {input_path} to {output_path}...{Colors.RESET}"
    )
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
                f"{Colors.FG_RED}Unsupported compression format: {file_type}{Colors.RESET}"
            )
            raise RuntimeError(f"Unsupported compression format: {file_type}")

    except Exception as e:
        print(f"{Colors.FG_RED}Failed to decompress {input_path}: {e}{Colors.RESET}")
        raise


def parse_xml(file_path):
    print(f"{Colors.FG_CYAN}Parsing {file_path}...{Colors.RESET}")
    tree = ET.parse(file_path)
    return tree.getroot()


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
    packages = []
    for package in root_element.findall("common:package", ns):
        name_elem = package.find("common:name", ns)
        if name_elem is not None:
            packages.append(name_elem.text)
    return sorted(packages)


def get_package_rpm_urls(root_element, base_url, package_names):
    ns = {"common": "http://linux.duke.edu/metadata/common"}
    rpm_urls = []

    for package in root_element.findall("common:package", ns):
        name_elem = package.find("common:name", ns)
        if name_elem is None or name_elem.text not in package_names:
            continue

        location_elem = package.find("common:location", ns)
        if location_elem is None:
            continue

        href = location_elem.attrib.get("href")
        if href:
            # Construct the full URL from the base URL and the href
            full_url = urljoin(
                base_url, href
            )  # This takes care of subdirectories dynamically
            rpm_urls.append((name_elem.text, full_url))

    return rpm_urls


def download_rpms(rpm_urls, download_dir):
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    for pkg_name, url in rpm_urls:
        filename = os.path.basename(url)
        dest_path = os.path.join(download_dir, filename)

        if os.path.exists(dest_path):
            print(f"{Colors.FG_YELLOW}Already downloaded: {filename}{Colors.RESET}")
            continue

        try:
            print(f"{Colors.FG_CYAN}Downloading {filename}...{Colors.RESET}")
            download_file_powershell(url, dest_path)
            print(f"{Colors.FG_GREEN}Downloaded: {filename}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.FG_RED}Failed to download {filename}: {e}{Colors.RESET}")


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


def build_maps(root_element):
    ns = {
        "common": "http://linux.duke.edu/metadata/common",
        "rpm": "http://linux.duke.edu/metadata/rpm",
    }
    provides_map = defaultdict(set)  # file/lib -> packages providing it
    requires_map = {}  # package -> set of required files/libs
    packages_with_format = []

    # First pass: build provides_map and gather package info
    for package in root_element.findall("common:package", ns):
        name_elem = package.find("common:name", ns)
        if name_elem is None:
            continue
        pkg_name = name_elem.text

        format_elem = package.find("common:format", ns)
        if format_elem is None:
            requires_map[pkg_name] = set()
            continue

        # Provides
        provides = format_elem.find("rpm:provides", ns)
        if provides is not None:
            for entry in provides.findall("rpm:entry", ns):
                pname = entry.get("name")
                if pname:
                    provides_map[pname].add(pkg_name)

        packages_with_format.append((pkg_name, format_elem))

    # Second pass: build requires_map
    for pkg_name, format_elem in packages_with_format:
        requires = format_elem.find("rpm:requires", ns)
        reqs = set()
        if requires is not None:
            for entry in requires.findall("rpm:entry", ns):
                rname = entry.get("name")
                if rname:
                    reqs.add(rname)

            # Support weak dependencies if enabled in config
            if Config.SUPPORT_WEAK_DEPS:
                weak_requires = format_elem.find("rpm:weakrequires", ns)
                if weak_requires is not None:
                    for entry in weak_requires.findall("rpm:entry", ns):
                        rname = entry.get("name")
                        if rname:
                            reqs.add(rname)

        requires_map[pkg_name] = reqs

    # Third pass: build dep_map (package -> set of packages that satisfy its requirements)
    dep_map = {}
    for pkg_name, reqs in requires_map.items():
        deps = set()
        for req in reqs:
            # Add all packages that provide this required file/lib
            if req in provides_map:
                deps.update(provides_map[req])
        dep_map[pkg_name] = deps

    return requires_map, provides_map, dep_map


def whatrequires(pkg_name, requires_map):
    return requires_map.get(pkg_name, set())


def whatprovides(file_name, provides_map):
    return provides_map.get(file_name, set())


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
        for dep_pkg in dep_map.get(current, set()):
            if dep_pkg not in to_install:
                queue.append(dep_pkg)
    return to_install


def configure_settings():
    while True:
        Config.print_config()
        print(
            f"{Colors.FG_YELLOW}Enter the config key to change (or press Enter to return to menu):{Colors.RESET}"
        )
        key = input(f"{Colors.FG_CYAN}Config key: {Colors.RESET}").strip()
        if key == "":
            break
        if not hasattr(Config, key):
            print(
                f"{Colors.FG_RED}Invalid key '{key}'. Please enter a valid config key.{Colors.RESET}"
            )
            continue
        current_value = getattr(Config, key)
        new_value = input(
            f"{Colors.FG_CYAN}Enter new value for {key} (current: {current_value}): {Colors.RESET}"
        ).strip()
        if isinstance(current_value, bool):
            if new_value.lower() in ["true", "1", "yes", "y"]:
                new_value = True
            elif new_value.lower() in ["false", "0", "no", "n"]:
                new_value = False
            else:
                print(
                    f"{Colors.FG_RED}Please enter a valid boolean (true/false).{Colors.RESET}"
                )
                continue
        elif isinstance(current_value, int):
            if not new_value.isdigit():
                print(f"{Colors.FG_RED}Please enter a valid integer.{Colors.RESET}")
                continue
            new_value = int(new_value)
        Config.set_config(key, new_value)


def cleanup_files():
    files = [Config.LOCAL_REPOMD_FILE, Config.LOCAL_XZ_FILE, Config.LOCAL_XML_FILE]
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


def check_and_refresh_metadata():
    """
    Implements workflow option 3:
    Conditional re-fetch based on metadata availability.
    """
    files = [Config.LOCAL_REPOMD_FILE, Config.LOCAL_XZ_FILE, Config.LOCAL_XML_FILE]

    missing_files = [f for f in files if not os.path.exists(f)]

    if missing_files:
        print(
            f"{Colors.FG_YELLOW}Metadata files missing or removed: {', '.join(missing_files)}{Colors.RESET}"
        )
        print(f"{Colors.FG_CYAN}Refreshing metadata files now.{Colors.RESET}")
        repomd_url = Config.REPO_BASE_URL + Config.REPOMD_XML

        download_file_powershell(repomd_url, Config.LOCAL_REPOMD_FILE)

        repomd_root = parse_xml(Config.LOCAL_REPOMD_FILE)
        primary_url = get_primary_location_url(repomd_root, Config.REPO_BASE_URL)
        if not primary_url:
            raise RuntimeError("Could not find primary metadata URL in repomd.xml")

        download_file_powershell(primary_url, Config.LOCAL_XZ_FILE)

        decompress_file(Config.LOCAL_XZ_FILE, Config.LOCAL_XML_FILE)

    else:
        print(
            f"{Colors.FG_GREEN}All metadata files present, skipping refresh.{Colors.RESET}"
        )


# --- Inside your main() function, replace download options with this single option 7 ---


def main():
    check_and_refresh_metadata()

    primary_root = parse_xml(Config.LOCAL_XML_FILE)

    all_packages = get_all_packages(primary_root)
    requires_map, provides_map, dep_map = build_maps(primary_root)

    while True:
        print(f"\n{Colors.BOLD}{Colors.FG_BLUE}--- MENU ---{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}1) List packages by starting letters{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}2) Calculate dependencies for package{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}3) Refresh metadata files if missing{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}4) Cleanup metadata files{Colors.RESET}")
        print(
            f"{Colors.FG_YELLOW}5) List RPM URLs by starting letters/string{Colors.RESET}"
        )
        # Removed old options 6 and 7 for downloads, now combined:
        print(
            f"{Colors.FG_YELLOW}7) Download packages by wildcard or list (e.g. '*zabbix*' or 'vim,wget'){Colors.RESET}"
        )
        print(f"{Colors.FG_YELLOW}9) Configure settings{Colors.RESET}")
        print(f"{Colors.FG_YELLOW}0) Exit{Colors.RESET}")

        choice = input(f"{Colors.FG_CYAN}Enter your choice: {Colors.RESET}").strip()

        if choice == "1":
            start = input(
                f"{Colors.FG_CYAN}Enter starting letters/string (empty to list all): {Colors.RESET}"
            ).strip()
            filtered = (
                [pkg for pkg in all_packages if pkg.startswith(start)]
                if start
                else all_packages
            )
            print_packages_tabular(filtered)

        elif choice == "2":
            pkg = input(f"{Colors.FG_CYAN}Enter package name: {Colors.RESET}").strip()
            if pkg not in dep_map:
                print(f"{Colors.FG_RED}Package '{pkg}' not found.{Colors.RESET}")
                continue
            resolved = resolve_all_dependencies(pkg, dep_map)
            if resolved is None:
                print(
                    f"{Colors.FG_RED}Could not resolve dependencies for {pkg}{Colors.RESET}"
                )
                continue
            print(
                f"{Colors.FG_GREEN}Dependencies for {pkg} (including package itself):{Colors.RESET}"
            )
            print_packages_tabular(sorted(resolved))

        elif choice == "3":
            check_and_refresh_metadata()

        elif choice == "4":
            cleanup_files()

        elif choice == "5":
            start = input(
                f"{Colors.FG_CYAN}Enter starting letters/string for RPM URLs (empty to list all): {Colors.RESET}"
            ).strip()
            package_names = (
                [pkg for pkg in all_packages if pkg.startswith(start)]
                if start
                else all_packages
            )
            rpm_urls = get_package_rpm_urls(
                primary_root, Config.REPO_BASE_URL, package_names
            )
            if not rpm_urls:
                print(
                    f"{Colors.FG_RED}No RPM URLs found with given filter.{Colors.RESET}"
                )
            else:
                for pkg_name, url in rpm_urls:
                    print(
                        f"{Colors.FG_MAGENTA}{pkg_name:<30}{Colors.FG_CYAN}{url}{Colors.RESET}"
                    )

        elif choice == "7":
            user_input = input(
                f"{Colors.FG_CYAN}Enter wildcard pattern (e.g. '*zabbix*') or comma-separated package names: {Colors.RESET}"
            ).strip()

            if "," in user_input:
                # Treat as exact package names list
                package_names = [
                    pkg.strip() for pkg in user_input.split(",") if pkg.strip()
                ]
                valid_packages = [pkg for pkg in package_names if pkg in all_packages]
                missing_packages = [
                    pkg for pkg in package_names if pkg not in all_packages
                ]

                if missing_packages:
                    print(
                        f"{Colors.FG_YELLOW}Warning: These packages were not found and will be skipped: {', '.join(missing_packages)}{Colors.RESET}"
                    )
                if not valid_packages:
                    print(
                        f"{Colors.FG_RED}No valid packages to download.{Colors.RESET}"
                    )
                    continue
            else:
                # Treat as wildcard pattern
                pattern = user_input
                valid_packages = [
                    pkg for pkg in all_packages if fnmatch.fnmatch(pkg, pattern)
                ]
                if not valid_packages:
                    print(
                        f"{Colors.FG_RED}No packages matched the pattern '{pattern}'.{Colors.RESET}"
                    )
                    continue

            rpm_urls = get_package_rpm_urls(
                primary_root, Config.REPO_BASE_URL, valid_packages
            )
            if not rpm_urls:
                print(
                    f"{Colors.FG_RED}No RPM URLs found for selected packages.{Colors.RESET}"
                )
            else:
                download_rpms(rpm_urls, Config.DOWNLOAD_DIR)

        elif choice == "9":
            configure_settings()

        elif choice == "0":
            print(f"{Colors.FG_GREEN}Exiting program. Goodbye!{Colors.RESET}")
            break

        else:
            print(
                f"{Colors.FG_RED}Invalid choice. Please select a valid menu option.{Colors.RESET}"
            )


if __name__ == "__main__":
    main()
