"""Storage abstraction: one code path for AWS S3, Cloudflare R2, SeaweedFS, local disk, and
Databricks Unity Catalog.

* :class:`ObjectStore` - raw file landing zone (bytes in / bytes out), used by the generator.
* :class:`Lake`        - Delta tables addressed by ``(layer, name)``; resolves to a path on
  object storage, or to a Unity Catalog table ``<catalog>.<prefix>_<layer>.<name>``.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fintxn.config import PROJECT_ROOT, Settings, StorageConfig

if TYPE_CHECKING:
    from delta.tables import DeltaTable
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int


class ObjectStore(ABC):
    """Minimal blob interface for the raw landing zone."""

    @abstractmethod
    def list_objects(self, layer: str, prefix: str = "") -> list[ObjectInfo]: ...

    @abstractmethod
    def put_bytes(self, layer: str, key: str, data: bytes) -> None: ...

    @abstractmethod
    def get_bytes(self, layer: str, key: str) -> bytes: ...

    @abstractmethod
    def list_keys(self, layer: str, prefix: str = "") -> list[str]: ...

    @abstractmethod
    def uri(self, layer: str, key: str = "") -> str:
        """URI Spark can read (``s3a://...`` or a filesystem path)."""

    def delete_prefix(self, layer: str, prefix: str) -> int:
        raise NotImplementedError


class LocalObjectStore(ObjectStore):
    def __init__(self, root: str | Path) -> None:
        root = Path(root)
        self.root = root if root.is_absolute() else PROJECT_ROOT / root

    def _path(self, layer: str, key: str) -> Path:
        return self.root / layer / key

    def put_bytes(self, layer: str, key: str, data: bytes) -> None:
        p = self._path(layer, key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def get_bytes(self, layer: str, key: str) -> bytes:
        return self._path(layer, key).read_bytes()

    def list_objects(self, layer: str, prefix: str = "") -> list[ObjectInfo]:
        base = self.root / layer
        if not base.exists():
            return []
        out = [
            ObjectInfo(str(p.relative_to(base)), p.stat().st_size)
            for p in base.rglob("*")
            if p.is_file() and not p.name.startswith(".")
        ]
        return sorted((o for o in out if o.key.startswith(prefix)), key=lambda o: o.key)

    def list_keys(self, layer: str, prefix: str = "") -> list[str]:
        return [o.key for o in self.list_objects(layer, prefix)]

    def uri(self, layer: str, key: str = "") -> str:
        return str(self._path(layer, key)).rstrip("/")

    def delete_prefix(self, layer: str, prefix: str) -> int:
        n = 0
        for k in self.list_keys(layer, prefix):
            self._path(layer, k).unlink()
            n += 1
        return n


class S3ObjectStore(ObjectStore):
    """Any S3-API endpoint. Bucket per layer: ``<prefix>-<layer>``."""

    def __init__(self, cfg: StorageConfig) -> None:
        import boto3
        from botocore.config import Config

        self.cfg = cfg
        self.client = boto3.client(
            "s3",
            endpoint_url=cfg.endpoint,
            region_name=cfg.region,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            config=Config(
                s3={"addressing_style": "path" if cfg.path_style else "auto"},
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )

    def _extra(self) -> dict[str, Any]:
        if self.cfg.sse == "none":
            return {}
        extra: dict[str, Any] = {"ServerSideEncryption": self.cfg.sse}
        if self.cfg.sse == "aws:kms" and self.cfg.kms_key_id:
            extra["SSEKMSKeyId"] = self.cfg.kms_key_id
        return extra

    def put_bytes(self, layer: str, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.cfg.bucket(layer), Key=key, Body=data, **self._extra())

    def get_bytes(self, layer: str, key: str) -> bytes:
        body = self.client.get_object(Bucket=self.cfg.bucket(layer), Key=key)["Body"].read()
        return bytes(body)

    def list_objects(self, layer: str, prefix: str = "") -> list[ObjectInfo]:
        out: list[ObjectInfo] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.cfg.bucket(layer), Prefix=prefix):
            out.extend(ObjectInfo(o["Key"], int(o["Size"])) for o in page.get("Contents", []))
        return sorted(out, key=lambda o: o.key)

    def list_keys(self, layer: str, prefix: str = "") -> list[str]:
        return [o.key for o in self.list_objects(layer, prefix)]

    def uri(self, layer: str, key: str = "") -> str:
        return f"s3a://{self.cfg.bucket(layer)}/{key}".rstrip("/")

    def delete_prefix(self, layer: str, prefix: str) -> int:
        keys = self.list_keys(layer, prefix)
        for i in range(0, len(keys), 1000):
            batch: list[Any] = [{"Key": k} for k in keys[i : i + 1000]]
            self.client.delete_objects(Bucket=self.cfg.bucket(layer), Delete={"Objects": batch})
        return len(keys)


def get_object_store(settings: Settings) -> ObjectStore:
    st = settings.storage
    if st.backend == "s3":
        return S3ObjectStore(st)
    if st.backend == "databricks":
        # UC Volumes are FUSE-mounted, so plain file IO works on Databricks compute.
        return LocalObjectStore(st.volume_path)
    return LocalObjectStore(st.local_root)


class Lake:
    """Delta table IO addressed by ``(layer, name)``."""

    def __init__(self, spark: SparkSession, settings: Settings) -> None:
        self.spark = spark
        self.settings = settings
        self.cfg = settings.storage
        self.store = get_object_store(settings)

    # -- addressing -----------------------------------------------------------------------
    @property
    def uses_catalog(self) -> bool:
        return self.cfg.backend == "databricks"

    def schema_name(self, layer: str) -> str:
        return f"{self.cfg.catalog}.{self.cfg.schema_prefix}_{layer}"

    def location(self, layer: str, name: str) -> str:
        if self.uses_catalog:
            raise ValueError("Unity Catalog tables are addressed by name, not path")
        return self.store.uri(layer, name)

    def ref(self, layer: str, name: str) -> str:
        """SQL identifier usable in ``spark.sql``."""
        if self.uses_catalog:
            return f"{self.schema_name(layer)}.{name}"
        return f"delta.`{self.location(layer, name)}`"

    # -- io -------------------------------------------------------------------------------
    def exists(self, layer: str, name: str) -> bool:
        if self.uses_catalog:
            return bool(self.spark.catalog.tableExists(f"{self.schema_name(layer)}.{name}"))
        from delta.tables import DeltaTable

        return bool(DeltaTable.isDeltaTable(self.spark, self.location(layer, name)))

    def read(self, layer: str, name: str, version: int | None = None) -> DataFrame:
        if self.uses_catalog:
            if version is None:
                return self.spark.table(self.ref(layer, name))
            return self.spark.sql(f"SELECT * FROM {self.ref(layer, name)} VERSION AS OF {version}")
        reader = self.spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        return reader.load(self.location(layer, name))

    def write(
        self,
        df: DataFrame,
        layer: str,
        name: str,
        mode: str = "overwrite",
        partition_by: list[str] | None = None,
        replace_where: str | None = None,
        overwrite_schema: bool = False,
        merge_schema: bool = False,
        options: dict[str, str] | None = None,
    ) -> int:
        """Write ``df`` as Delta and return the resulting table version."""
        writer = df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if replace_where:
            writer = writer.option("replaceWhere", replace_where)
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        for k, v in (options or {}).items():
            writer = writer.option(k, v)
        if self.uses_catalog:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name(layer)}")
            writer.saveAsTable(self.ref(layer, name))
        else:
            writer.save(self.location(layer, name))
        return self.version(layer, name)

    def delta_table(self, layer: str, name: str) -> DeltaTable:
        from delta.tables import DeltaTable

        if self.uses_catalog:
            return DeltaTable.forName(self.spark, self.ref(layer, name))
        return DeltaTable.forPath(self.spark, self.location(layer, name))

    def last_operation_metrics(self, layer: str, name: str) -> dict[str, str]:
        row = self.delta_table(layer, name).history(1).select("operationMetrics").first()
        return dict(row["operationMetrics"] or {}) if row else {}

    def version(self, layer: str, name: str) -> int:
        row = self.delta_table(layer, name).history(1).select("version").first()
        return int(row["version"]) if row else -1

    def sql(self, query: str) -> DataFrame:
        return self.spark.sql(query)


def env_flag(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).lower() in {"1", "true", "yes"}
