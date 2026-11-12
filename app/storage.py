"""本地数据卷存储层：ZIP 分块落盘、流式解包、内容寻址 blob 与暂存区管理。

目录布局（DATA_DIR，默认仓库下 data/）::

    DATA_DIR/
      app.db                  # SQLite（路径由 DATABASE_PATH 单独指定，默认即此处）
      tmp/spool-<uuid>.zip    # 上传中的 ZIP 临时文件，分块写入、结束即删
      staging/<upload_id>/    # 单次上传的暂存目录，全部条目解包完成前对外不可见
      blobs/xx/<sha256>.bin   # 已发布的图像原字节，按 SHA-256 内容寻址、天然去重

设计要点：
- 上传 ZIP 先分块 spool 到磁盘，再用 zipfile 流式逐条解包（1 MiB 块），
  全程不把整个 ZIP / 图像读进内存，也不解码图像；
- 暂存目录与正式 blob 目录隔离：校验或写盘失败、进程终止都只会留下暂存垃圾，
  启动时统一清理，绝不会出现半包可查/可下载；
- blob 按内容寻址，同字节只占一份物理文件；发布在单个 SQLite 写事务内完成，
  行数据与 blob 文件一起对其他事务可见（事务提交前查不到、下不到）。
"""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import stat
import uuid
import zipfile

from .db import connect, read_txn, write_txn

CHUNK_SIZE = 1024 * 1024  # 1 MiB：spool / 解包 / 拷贝统一分块大小
IMAGE_ID_RE = re.compile(r"[A-Za-z0-9_-]+")
JPEG_SUFFIX = ".jpg"


class ZipPackageError(Exception):
    """ZIP 包自身不合法（结构、命名、条目类型等），整包拒绝。"""

    def __init__(self, code: str, message: str, **extra):
        super().__init__(message)
        self.code = code
        self.message = message
        self.extra = extra


class UnregisteredImageError(Exception):
    """包内存在尚未在 images 目录登记的图片 ID，整包拒绝。"""

    def __init__(self, missing: list[str]):
        super().__init__("unregistered images: " + ", ".join(missing))
        self.missing = missing


class BlobConflictError(Exception):
    """同一图片 ID 已有不同字节的图像，整包拒绝。"""

    def __init__(self, conflicts: list[dict]):
        super().__init__(f"blob conflicts: {len(conflicts)}")
        self.conflicts = conflicts


# ---------------------------------------------------------------------------
# 路径与启动维护
# ---------------------------------------------------------------------------

def data_root() -> str:
    default = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
    )
    return os.environ.get("DATA_DIR", default)


def tmp_dir() -> str:
    return os.path.join(data_root(), "tmp")


def staging_root() -> str:
    return os.path.join(data_root(), "staging")


def blobs_root() -> str:
    return os.path.join(data_root(), "blobs")


def max_uncompressed_bytes() -> int:
    """展开后总字节上限，0 表示不限制（默认），可用环境变量收紧防 ZIP 炸弹。"""
    return int(os.environ.get("MAX_UNCOMPRESSED_BYTES", "0"))


def init_storage() -> None:
    """启动时调用：建目录、清理上次崩溃残留的暂存数据、回收无引用 blob。"""
    for path in (tmp_dir(), staging_root(), blobs_root()):
        os.makedirs(path, exist_ok=True)
    _cleanup_staging()
    _gc_orphan_blobs()


def _cleanup_staging() -> None:
    """删除所有未完成上传的暂存目录与 spool 文件。

    只清理 tmp/ 与 staging/ 下的内容，绝不触碰 blobs/，
    因此其他上传已发布的文件不可能被误删。
    """
    for base in (staging_root(), tmp_dir()):
        if not os.path.isdir(base):
            continue
        for name in os.listdir(base):
            victim = os.path.join(base, name)
            if os.path.isdir(victim) and not os.path.islink(victim):
                shutil.rmtree(victim, ignore_errors=True)
            else:
                try:
                    os.remove(victim)
                except FileNotFoundError:
                    pass
                except OSError:
                    pass


def _gc_orphan_blobs() -> None:
    """回收没有任何 image_blobs 行引用的 blob（崩溃于 rename 之后、提交之前的残留）。

    有行引用的 blob 一律保留——清理未完成上传不会删掉其他上传已发布的文件。
    """
    if not os.path.isdir(blobs_root()):
        return
    conn = connect()
    try:
        with read_txn(conn):
            referenced = {
                row["sha256"]
                for row in conn.execute("SELECT sha256 FROM image_blobs")
            }
    finally:
        conn.close()

    for shard in sorted(os.listdir(blobs_root())):
        shard_dir = os.path.join(blobs_root(), shard)
        if not os.path.isdir(shard_dir) or os.path.islink(shard_dir):
            continue
        for fname in sorted(os.listdir(shard_dir)):
            sha = fname[: -len(".bin")] if fname.endswith(".bin") else fname
            if sha in referenced:
                continue
            try:
                os.remove(os.path.join(shard_dir, fname))
            except FileNotFoundError:
                pass
        try:
            os.rmdir(shard_dir)  # 空分片目录顺手删除，非空则保留
        except OSError:
            pass


# ---------------------------------------------------------------------------
# 上传 spool 与暂存解包
# ---------------------------------------------------------------------------

def new_spool_path() -> str:
    os.makedirs(tmp_dir(), exist_ok=True)
    return os.path.join(tmp_dir(), f"spool-{uuid.uuid4().hex}.zip")


def new_staging_dir() -> tuple[str, str]:
    upload_id = uuid.uuid4().hex
    path = os.path.join(staging_root(), upload_id)
    os.makedirs(path, exist_ok=False)
    return upload_id, path


def spool_stream(src, spool_path: str) -> int:
    """把上传流分块拷贝到磁盘 spool 文件，返回字节数。不按内存大小缓存。"""
    total = 0
    with open(spool_path, "wb") as dst:
        while True:
            chunk = src.read(CHUNK_SIZE)
            if not chunk:
                break
            dst.write(chunk)
            total += len(chunk)
    return total


def _validate_member(info: zipfile.ZipInfo) -> tuple[str, str]:
    """校验单个 ZIP 条目：只接受顶层 ``图片ID.jpg`` 普通文件。返回 (ID, 文件名)。"""
    name = info.filename

    if info.is_dir() or name.endswith("/"):
        raise ZipPackageError(
            "invalid_member", "仅接受顶层普通图片文件，ZIP 内不允许目录条目", member=name
        )

    # 符号链接 / FIFO / 设备等非常规文件。高 16 位为 0（如 Windows 打包的普通文件）
    # 时没有类型信息，按普通文件放行；有 unix 类型位且非 regular 才拒绝。
    mode = info.external_attr >> 16
    if mode and stat.S_IFMT(mode) and not stat.S_ISREG(mode):
        raise ZipPackageError(
            "invalid_member", "仅接受普通文件，不接受符号链接或特殊文件", member=name
        )

    if info.flag_bits & 0x1:
        raise ZipPackageError(
            "encrypted_member", "不接受加密的 ZIP 条目", member=name
        )

    if "\x00" in name or "\\" in name or name.startswith("/") or os.path.isabs(name):
        raise ZipPackageError(
            "invalid_member_name", "文件路径不合法（仅允许顶层相对路径）", member=name
        )

    parts = name.split("/")
    if len(parts) != 1 or not parts[0]:
        raise ZipPackageError(
            "nested_member", "仅接受 ZIP 顶层文件，不接受子目录或嵌套路径", member=name
        )
    filename = parts[0]

    if not filename.endswith(JPEG_SUFFIX) or filename == JPEG_SUFFIX:
        raise ZipPackageError(
            "not_jpeg", "仅接受 .jpg 后缀的图片文件（后缀小写）", member=name
        )
    image_id = filename[: -len(JPEG_SUFFIX)]
    if not IMAGE_ID_RE.fullmatch(image_id):
        raise ZipPackageError(
            "invalid_image_id",
            "图片 ID 仅限英文字母、数字、下划线和短横线",
            member=name,
            image_id=image_id,
        )
    if info.file_size <= 0:
        raise ZipPackageError(
            "empty_file", "不接受空文件", member=name, image_id=image_id
        )
    return image_id, filename


def extract_zip_to_staging(spool_path: str) -> tuple[str, list[dict]]:
    """流式解包 spool ZIP 到全新暂存目录，分块计算 SHA-256。

    返回 (upload_id, manifest)。任何条目不合法都抛 ZipPackageError，整包拒绝。
    """
    try:
        zf = zipfile.ZipFile(spool_path)
    except zipfile.BadZipFile as exc:
        raise ZipPackageError("not_a_zip", "上传文件不是合法的 ZIP 压缩包") from exc

    upload_id, staging_dir = new_staging_dir()
    manifest: list[dict] = []
    total_size = 0
    cap = max_uncompressed_bytes()
    try:
        with zf:
            infos = zf.infolist()  # 目录条目也保留，交给 _validate_member 拒绝
            if not infos:
                raise ZipPackageError("empty_zip", "ZIP 内没有任何图片文件")

            # 第一遍：全部条目命名/类型校验，包内 ID 去重
            seen_ids: set[str] = set()
            for info in infos:
                image_id, filename = _validate_member(info)
                if image_id in seen_ids:
                    raise ZipPackageError(
                        "duplicate_id",
                        "ZIP 内图片 ID 不得重复",
                        image_id=image_id,
                    )
                seen_ids.add(image_id)

            # 第二遍：分块解包到暂存区并计算 SHA-256（CRC 由 zipfile 在读取时校验）
            for info in infos:
                image_id, filename = _validate_member(info)
                target = os.path.join(staging_dir, filename)
                hasher = hashlib.sha256()
                size = 0
                try:
                    with zf.open(info) as src, open(target, "wb") as dst:
                        while True:
                            chunk = src.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            hasher.update(chunk)
                            dst.write(chunk)
                            size += len(chunk)
                            total_size += len(chunk)
                            if cap and total_size > cap:
                                raise ZipPackageError(
                                    "too_large",
                                    f"展开后总字节超过上限 {cap}，整包拒绝",
                                    limit=cap,
                                )
                except ZipPackageError:
                    raise
                except zipfile.BadZipFile as exc:  # CRC 错误 / 数据描述符损坏等
                    raise ZipPackageError(
                        "corrupt_member",
                        "ZIP 条目校验失败（CRC 或压缩数据损坏）",
                        member=info.filename,
                    ) from exc
                except NotImplementedError as exc:  # 不支持的压缩算法（如加密/新算法）
                    raise ZipPackageError(
                        "unsupported_compression",
                        "ZIP 使用了不支持的压缩或加密方式",
                        member=info.filename,
                    ) from exc

                manifest.append(
                    {
                        "image_id": image_id,
                        "filename": filename,
                        "sha256": hasher.hexdigest(),
                        "size_bytes": size,
                    }
                )
    except BaseException:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise
    return upload_id, manifest


# ---------------------------------------------------------------------------
# 发布：暂存 -> 内容寻址 blob，单事务原子生效
# ---------------------------------------------------------------------------

def blob_path(sha256: str) -> str:
    return os.path.join(blobs_root(), sha256[:2], f"{sha256}.bin")


def publish_manifest(staging_dir: str, manifest: list[dict]) -> dict:
    """把暂存清单发布为正式 blob 并写入 image_blobs。

    - 任一 ID 未在 images 登记 -> UnregisteredImageError，整包拒绝；
    - 同 ID 已存在且 SHA-256 不同 -> BlobConflictError，整包拒绝；
    - 同 ID 同 SHA-256 -> 复用既有引用，不重复写盘；
    - 全部校验通过后，blob 文件落位与引用行插入在同一个写事务内完成。
    """
    conn = connect()
    promoted: list[str] = []
    try:
        with write_txn(conn):
            ids = [e["image_id"] for e in manifest]
            placeholders = ",".join("?" * len(ids))

            registered = {
                row["image_id"]
                for row in conn.execute(
                    f"SELECT image_id FROM images WHERE image_id IN ({placeholders})", ids
                )
            }
            missing = [i for i in ids if i not in registered]
            if missing:
                raise UnregisteredImageError(missing)

            existing = {
                row["image_id"]: row
                for row in conn.execute(
                    f"SELECT image_id, sha256, size_bytes FROM image_blobs "
                    f"WHERE image_id IN ({placeholders})",
                    ids,
                )
            }
            conflicts = []
            for entry in manifest:
                row = existing.get(entry["image_id"])
                if row is not None and row["sha256"] != entry["sha256"]:
                    conflicts.append(
                        {
                            "image_id": entry["image_id"],
                            "existing_sha256": row["sha256"],
                            "incoming_sha256": entry["sha256"],
                        }
                    )
            if conflicts:
                raise BlobConflictError(conflicts)

            published, reused = 0, 0
            files: list[dict] = []
            for entry in manifest:
                if entry["image_id"] in existing:
                    reused += 1
                    files.append({**entry, "outcome": "reused"})
                    continue

                target = blob_path(entry["sha256"])
                if not os.path.exists(target):
                    os.makedirs(os.path.dirname(target), exist_ok=True)
                    # 同分区 rename，原子落位；目标位置只可能是同内容 blob。
                    # 若落位后事务回滚（如后续条目 IO 失败），下面的 except 会立即回收；
                    # 进程被杀的极端情况由启动时 _gc_orphan_blobs 兜底。
                    os.replace(os.path.join(staging_dir, entry["filename"]), target)
                    promoted.append(target)
                conn.execute(
                    "INSERT INTO image_blobs (image_id, sha256, size_bytes) "
                    "VALUES (?, ?, ?)",
                    (entry["image_id"], entry["sha256"], entry["size_bytes"]),
                )
                published += 1
                files.append({**entry, "outcome": "published"})

            return {
                "received": len(manifest),
                "published": published,
                "reused": reused,
                "files": files,
            }
    except BaseException:
        # 事务已回滚：删除本次落位但未获引用的 blob，避免半包与孤儿文件
        for path in promoted:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        raise
    finally:
        conn.close()


def remove_staging(staging_dir: str) -> None:
    shutil.rmtree(staging_dir, ignore_errors=True)


def remove_spool(spool_path: str) -> None:
    try:
        os.remove(spool_path)
    except FileNotFoundError:
        pass


# ---------------------------------------------------------------------------
# 查询 / 下载辅助
# ---------------------------------------------------------------------------

def get_storage_row(image_id: str):
    """返回 (registered, blob_row)。blob_row 为 None 表示已登记但尚未入库图像。"""
    conn = connect()
    try:
        with read_txn(conn):
            registered = conn.execute(
                "SELECT 1 FROM images WHERE image_id = ?", (image_id,)
            ).fetchone()
            row = None
            if registered is not None:
                row = conn.execute(
                    "SELECT image_id, sha256, size_bytes, stored_at "
                    "FROM image_blobs WHERE image_id = ?",
                    (image_id,),
                ).fetchone()
            return registered is not None, row
    finally:
        conn.close()
