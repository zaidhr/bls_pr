# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Configuration
import os, json, uuid, hashlib, shutil, tempfile, datetime as dt
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, unquote

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

@dataclass(frozen=True)
class IngestionConfig:
    '''All configured values: bundle variables (databricks.yml) + ingestion settings (config/*.yaml).'''
    # databricks.yml variables
    catalog: str
    schema: str
    raw_volume: str
    # config/bls_pr_ingestion.yaml
    source_url: str
    extra_file_urls: dict
    state_volume: str
    manifest_file: str
    user_agent: str
    http_retries: int
    http_backoff_factor: float
    request_timeout_seconds: int
    download_timeout_seconds: int

    @classmethod
    def load(cls, config_path='../../config/bls_pr_ingestion.yaml', bundle_path='../../databricks.yml'):
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        with open(bundle_path) as f:
            bundle_vars = yaml.safe_load(f)['variables']
        return cls(
            catalog=bundle_vars['catalog']['default'],
            schema=bundle_vars['bronze_schema']['default'],
            raw_volume=bundle_vars['raw_volume']['default'],
            source_url=cfg['sources']['bls_directory_url'],
            extra_file_urls=cfg['sources']['extra_files'],
            state_volume=cfg['storage']['state_volume'],
            manifest_file=cfg['storage']['manifest_file'],
            user_agent=cfg['http']['user_agent'],
            http_retries=cfg['http']['retries'],
            http_backoff_factor=cfg['http']['backoff_factor'],
            request_timeout_seconds=cfg['http']['request_timeout_seconds'],
            download_timeout_seconds=cfg['http']['download_timeout_seconds'],
        )

    @property
    def target_dir(self):
        return f'/Volumes/{self.catalog}/{self.schema}/{self.raw_volume}'

    @property
    def manifest_path(self):
        return f'/Volumes/{self.catalog}/{self.schema}/{self.state_volume}/{self.manifest_file}'


config = IngestionConfig.load()
config

# COMMAND ----------

# DBTITLE 1,Create schema and volumes
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{config.catalog}`.`{config.schema}` COMMENT 'Bronze layer for BLS Productivity (PR) time series'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{config.catalog}`.`{config.schema}`.`{config.raw_volume}` COMMENT 'Raw, unmodified files from https://download.bls.gov/pub/time.series/pr/'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{config.catalog}`.`{config.schema}`.`{config.state_volume}` COMMENT 'Sync state (manifest) for the pr_raw volume'")

# COMMAND ----------

# DBTITLE 1,Helpers: discover, fingerprint, download, manifest
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({'User-Agent': config.user_agent})
    retry = Retry(total=config.http_retries, backoff_factor=config.http_backoff_factor, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=['GET', 'HEAD'])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s


class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == 'a':
            self.hrefs += [v for k, v in attrs if k.lower() == 'href' and v]


def list_remote_files(session, base_url) -> dict:
    '''Return {file_name: url} for every file directly inside base_url (skips parent link and sub-dirs).'''
    r = session.get(base_url, timeout=config.request_timeout_seconds)
    r.raise_for_status()
    parser = LinkParser()
    parser.feed(r.text)
    base_path = urlparse(base_url).path
    files = {}
    for href in parser.hrefs:
        url = urljoin(base_url, href)
        path = urlparse(url).path
        if not path.startswith(base_path) or path.endswith('/'):
            continue
        name = unquote(path[len(base_path):])
        if name and '/' not in name:
            files[name] = url
    return files


def head_metadata(session, url) -> dict:
    try:
        r = session.head(url, timeout=config.request_timeout_seconds, allow_redirects=True)
        r.raise_for_status()
        headers = r.headers
    except requests.RequestException:
        headers = {}  # HEAD not supported -> no fingerprint, rely on content hash
    lm_hdr = headers.get('Last-Modified')
    cl_hdr = headers.get('Content-Length')
    lm = parsedate_to_datetime(lm_hdr).astimezone(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ') if lm_hdr else None
    # Content-Length of a compressed response doesn't match the decoded bytes, so ignore it then
    cl = int(cl_hdr) if cl_hdr and not headers.get('Content-Encoding') else None
    # ETag is not used: BLS is load-balanced and ETags can differ per server.
    # No usable headers (e.g. dynamic API responses) -> fingerprint None: always download and compare SHA-256.
    fp = f'{lm}|{cl}' if (lm or cl is not None) else None
    return {'source_last_modified': lm, 'content_length': cl, 'source_fingerprint': fp}


def download_to_local(session, url):
    '''Stream to a local temp file while hashing. Returns (local_path, sha256, size).'''
    h, size = hashlib.sha256(), 0
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        with session.get(url, stream=True, timeout=config.download_timeout_seconds) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=1 << 20):
                tmp.write(chunk)
                h.update(chunk)
                size += len(chunk)
    return tmp.name, h.hexdigest(), size


def atomic_copy(src_path, dest_path):
    '''Copy to a temporary .partial file next to the destination, then rename over the real name.'''
    partial = os.path.join(os.path.dirname(dest_path), f'.{os.path.basename(dest_path)}.{uuid.uuid4().hex}.partial')
    shutil.copyfile(src_path, partial)
    try:
        os.replace(partial, dest_path)
    except OSError:
        # Fallback for filesystems that don't allow rename-over-existing
        if os.path.exists(dest_path):
            os.remove(dest_path)
        os.rename(partial, dest_path)


def load_manifest() -> dict:
    if os.path.exists(config.manifest_path):
        with open(config.manifest_path) as f:
            return json.load(f)
    return {}


def save_manifest(manifest: dict):
    with tempfile.NamedTemporaryFile('w', delete=False, suffix='.json') as tmp:
        json.dump(manifest, tmp, indent=2, sort_keys=True)
    try:
        atomic_copy(tmp.name, config.manifest_path)
    finally:
        os.remove(tmp.name)

# COMMAND ----------

# DBTITLE 1,Sync source directory and extra URLs to the volume
session = make_session()

remote_files = list_remote_files(session, config.source_url)
if not remote_files:
    raise RuntimeError(f'No files discovered at {config.source_url}; aborting without changes.')

remote_files.update(config.extra_file_urls)

manifest = load_manifest()
summary = {
    'new': [],
    'updated': [],
    'unchanged': [],
    'metadata_only': []
}
errors = {}

for name, url in sorted(remote_files.items()):
    try:
        meta = head_metadata(session, url)
        prev = manifest.get(name)
        dest = os.path.join(config.target_dir, name)

        # 1) Same fingerprint and file still present -> skip, no download
        if (meta['source_fingerprint'] is not None and prev
                and prev.get('source_fingerprint') == meta['source_fingerprint'] and os.path.exists(dest)):
            summary['unchanged'].append(name)
            continue

        # 2) New or changed -> download, verify, compare content
        local_file, sha, size = download_to_local(session, url)
        try:
            if meta['content_length'] is not None and size != meta['content_length']:
                raise IOError(f'Truncated download: got {size} bytes, expected {meta["content_length"]}')

            if prev and prev.get('sha256') == sha and os.path.exists(dest):
                # Bytes identical -> leave file untouched (no fingerprint = verified by hash only)
                if meta['source_fingerprint'] is None:
                    status = 'unchanged'  
                else:
                    status = 'metadata_only'
            else:
                atomic_copy(local_file, dest)
                if prev:
                    status = 'updated'
                else:
                    status = 'new'
        finally:
            os.remove(local_file)

        manifest[name] = {
            'source_url': url,
            **meta,
            'sha256': sha,
            'volume_path': dest,
            'synced_at': dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        }
        save_manifest(manifest)
        summary[status].append(name)
    except Exception as e:
        errors[name] = repr(e)

for k, v in summary.items():
    print(f'{k:>14}: {len(v):>3}  {v}')
if errors:
    raise RuntimeError(f'Failed files: {errors}')

# COMMAND ----------

# DBTITLE 1,Review volume contents
display(dbutils.fs.ls(config.target_dir))