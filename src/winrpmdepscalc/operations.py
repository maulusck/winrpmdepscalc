import sys
from pathlib import Path
from typing import Optional, List, Dict, Set, Tuple, Union

from .metadata_manager import MetadataManager
from .downloader import Downloader
from .config import Config
from .utils import LogColors, _logger
import yaml
import xml.etree.ElementTree as ET
from tqdm import tqdm
import fnmatch
from collections import defaultdict
from urllib.parse import urljoin


def parse_package_names(package_names_str: Optional[str]) -> Optional[List[str]]:
    if not package_names_str:
        return None
    patterns = [p.strip() for p in package_names_str.split(",") if p.strip()]
    return patterns if patterns else None


def select_packages(metadata: MetadataManager, package_names_str: Optional[str], ask_include_deps: Optional[bool] = None) -> List[str]:
    if not package_names_str:
        package_names_str = input(
            f"{LogColors.CYAN}Enter package names/wildcards (comma-separated): {LogColors.RESET}").strip()

    patterns = parse_package_names(package_names_str)
    if not patterns:
        _logger.error("No package names provided.")
        return []

    selected = metadata.filter_packages(patterns)
    if not selected:
        _logger.error("No packages matched the provided patterns.")
        return []

    if ask_include_deps is None:
        include_deps_input = input(
            f"{LogColors.CYAN}Include dependencies? (y/N): {LogColors.RESET}").strip().lower()
        include_deps = include_deps_input in {'y', 'yes', '1', 'true'}
    else:
        include_deps = ask_include_deps

    if include_deps:
        all_pkgs = set(selected)
        for pkg in selected:
            deps = metadata.resolve_all_dependencies(pkg)
            if deps:
                all_pkgs.update(deps)
        return sorted(all_pkgs)

    return sorted(selected)


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
        urls = get_package_rpm_urls(primary_root, config.REPO_BASE_URL, [
                                    pkg], only_latest=config.ONLY_LATEST_VERSION)
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


def list_packages(metadata: MetadataManager, package_patterns: Optional[List[str]] = None) -> None:
    if not package_patterns:
        patterns_input = input(
            f"{LogColors.CYAN}Enter wildcard filters (comma-separated): {LogColors.RESET}").strip()
        package_patterns = [p.strip()
                            for p in patterns_input.split(",") if p.strip()]
    packages = metadata.filter_packages(package_patterns)
    print_packages_tabular(
        packages, metadata.config.PACKAGE_COLUMNS, metadata.config.PACKAGE_COLUMN_WIDTH)


def calc_dependencies(metadata: MetadataManager, packages_str: Optional[str] = None, include_deps: Optional[bool] = None) -> None:
    selected = select_packages(
        metadata, packages_str, ask_include_deps=include_deps)
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


def list_rpm_urls(metadata: MetadataManager, packages_str: Optional[str] = None, include_deps: Optional[bool] = None) -> None:
    selected = select_packages(
        metadata, packages_str, ask_include_deps=include_deps)
    if not selected:
        return
    urls = get_package_rpm_urls(metadata.primary_root, metadata.config.REPO_BASE_URL,
                                selected, only_latest=metadata.config.ONLY_LATEST_VERSION)
    if not urls:
        _logger.error("No RPM URLs found.")
        return
    for pkg, url in urls:
        print(f"{LogColors.MAGENTA}{pkg:<30}{LogColors.CYAN}{url}{LogColors.RESET}")


def download_packages_ui(metadata: MetadataManager, packages_str: Optional[str] = None, include_deps: Optional[bool] = None) -> None:
    selected = select_packages(
        metadata, packages_str, ask_include_deps=include_deps)
    if not selected:
        return
    download_packages(selected, metadata.dep_map, metadata.primary_root,
                      metadata.config, metadata.downloader, download_deps=include_deps)


def configure_settings(metadata: MetadataManager, _, config_path: Optional[Path] = None) -> None:
    edit_configuration(metadata.config, config_path)


def exit_program(*_) -> None:
    _logger.info("Goodbye!")
    sys.exit(0)


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
