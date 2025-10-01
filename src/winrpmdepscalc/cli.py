import argparse
import importlib.metadata
import sys
from pathlib import Path

import urllib3

from .config import Config
from .downloader import Downloader
from .metadata_manager import MetadataManager
from .operations import (
    calc_dependencies,
    cleanup_metadata,
    configure_settings,
    download_packages_ui,
    list_packages,
    list_rpm_urls,
    load_config_file,
    refresh_metadata,
    run_interactive_menu,
    write_default_config,
)
from .utils import _logger

__version__ = importlib.metadata.version("winrpmdepscalc")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Windows RPM Package Metadata Tool")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("-c", "--config", type=Path, default=Path("config.yaml"), help="YAML config file path")
    parser.add_argument("--write-default-config", action="store_true", help="Write default config.yaml and exit")
    parser.add_argument("--list-packages", action="store_true", help="List packages (interactive prompt)")
    parser.add_argument("--calc-deps", action="store_true", help="Calculate dependencies (interactive prompt)")
    parser.add_argument("--refresh-meta", action="store_true", help="Refresh metadata files if missing")
    parser.add_argument("--cleanup-meta", action="store_true", help="Cleanup metadata files")
    parser.add_argument("--list-rpm-urls", action="store_true", help="List RPM URLs for packages (interactive prompt)")
    parser.add_argument("--download", action="store_true", help="Download packages (interactive prompt)")
    parser.add_argument("--configure", action="store_true", help="Configure settings interactively")
    parser.add_argument("--no-interactive", action="store_true", help="Disable interactive menu fallback")
    parser.add_argument(
        "--include-deps", dest="include_deps", action="store_true", help="Include dependencies (default: True)"
    )
    parser.add_argument(
        "--no-include-deps", dest="include_deps", action="store_false", help="Do not include dependencies"
    )
    parser.set_defaults(include_deps=None)
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
            _logger.warning("SSL verification disabled; HTTPS requests insecure.")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        downloader = Downloader(config.DOWNLOADER, skip_ssl_verify=config.SKIP_SSL_VERIFY)
        metadata = MetadataManager(config, downloader)

        needs_metadata = any(
            [
                args.list_packages,
                args.calc_deps is not None,
                args.refresh_meta,
                args.list_rpm_urls,
                args.download,
            ]
        )

        if needs_metadata:
            metadata.check_and_refresh_metadata()

        if args.list_packages:
            list_packages(metadata)
            return

        if args.calc_deps:
            calc_dependencies(metadata, packages_str=None, include_deps=args.include_deps)
            return

        if args.refresh_meta:
            refresh_metadata(metadata)
            return

        if args.cleanup_meta:
            cleanup_metadata(metadata)
            return

        if args.list_rpm_urls:
            list_rpm_urls(metadata, packages_str=None, include_deps=args.include_deps)
            return

        if args.download:
            download_packages_ui(metadata, packages_str=None, include_deps=args.include_deps)
            return

        if args.configure:
            configure_settings(metadata, None, args.config)
            return

        if not args.no_interactive:
            if not metadata.metadata_loaded:
                metadata.check_and_refresh_metadata()
            run_interactive_menu(metadata, args.config)
        else:
            _logger.warning("No operation specified and interactive mode disabled.")

    except KeyboardInterrupt:
        _logger.warning("\nTerminated by user (Ctrl+C). Exiting...")
        sys.exit(0)


if __name__ == "__main__":
    main()
