import functools
import fnmatch
import lzma
import gzip
import bz2
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from pathlib import Path
from typing import Optional, List, Dict, Set
from urllib.parse import urljoin
import logging

_logger = logging.getLogger("winrpmdepscalc")


class MetadataManager:
    NS_REPO = {"repo": "http://linux.duke.edu/metadata/repo"}
    NS_COMMON = {
        "common": "http://linux.duke.edu/metadata/common",
        "rpm": "http://linux.duke.edu/metadata/rpm"
    }

    def __init__(self, config, downloader) -> None:
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

        decompressors = [
            ('gzip', gzip.open),
            ('bzip2', bz2.open),
            ('xz', lzma.open),
        ]

        for name, opener in decompressors:
            try:
                with opener(input_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
                    f_out.write(f_in.read())
                _logger.info(f"Decompression complete using {name}.")
                return
            except Exception:
                continue

        _logger.error("Unsupported or corrupted compression format.")
        raise RuntimeError("Unsupported or corrupted compression format.")

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
                dep for req in reqs if req in self.provides_map for dep in self.provides_map[req]
            }
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
