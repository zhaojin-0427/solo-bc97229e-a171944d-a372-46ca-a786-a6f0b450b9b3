"""ZIP 入库端到端测试：整包原子、幂等复用、冲突拒绝、并发、重启、分块与清理。"""
from __future__ import annotations

import io
import os
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import app


# ---------------------------------------------------------------------------
# 构造工具
# ---------------------------------------------------------------------------

def jpg_bytes(tag: bytes, size: int = 4096) -> bytes:
    """构造以 FFD8 开头、FFD9 结尾的 JPEG 风格字节（应用本身不解码图像）。

    同 tag 同 size 字节确定；不同 tag / size 产生不同 SHA-256。
    """
    body = (b"\xff\xd8\xff\xe0JPEG_TEST:" + tag + b":")
    if len(body) < size - 2:
        body = body + b"\x00" * (size - 2 - len(body))
    return body[: size - 2] + b"\xff\xd9"


def make_zip(entries: dict[str, bytes], *, compress=zipfile.ZIP_DEFLATED) -> bytes:
    """entries: 归档内路径 -> 文件字节。"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compress) as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return buf.getvalue()


def upload_zip(client, payload: bytes, name="batch.zip"):
    return client.post(
        "/images/zip-ingests",
        files={"file": (name, payload, "application/zip")},
    )


def register(client, *ids: str):
    rows = [(i, f"WP-{i}", f"LOT-{i}") for i in ids]
    buf = io.StringIO()
    import csv

    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["image_id", "workpiece_no", "batch_no"])
    w.writerows(rows)
    return client.post(
        "/imports", files={"file": ("r.csv", buf.getvalue().encode(), "text/csv")}
    )


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


# ---------------------------------------------------------------------------
# 基本成功路径与幂等
# ---------------------------------------------------------------------------

def test_happy_path_publish_and_download(client):
    payloads = {f"IMG-{i:03d}.jpg": jpg_bytes(str(i).encode()) for i in range(3)}
    register(client, *[n[:-4] for n in payloads])

    r = upload_zip(client, make_zip(payloads))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["received"] == 3
    assert body["published"] == 3
    assert body["reused"] == 0
    assert sorted(f["outcome"] for f in body["files"]) == ["published"] * 3

    # 状态与下载
    st = client.get("/images/IMG-001/blob").json()
    assert st["stored"] is True and st["registered"] is True
    assert st["filename"] == "IMG-001.jpg"
    got = client.get("/images/IMG-001/download")
    assert got.status_code == 200
    assert got.content == payloads["IMG-001.jpg"]  # 原字节一致
    assert got.headers["X-Content-SHA256"] == st["sha256"]
    assert got.headers["content-type"] == "image/jpeg"
    assert client.get("/stats").json()["images_with_blob"] == 3


def test_same_id_same_bytes_is_reused(client):
    register(client, "IMG-A")
    data = jpg_bytes(b"a")
    z = make_zip({"IMG-A.jpg": data})
    assert upload_zip(client, z).json()["published"] == 1
    r = upload_zip(client, z)
    assert r.status_code == 200
    assert r.json()["published"] == 0 and r.json()["reused"] == 1
    # 物理字节仍可下载且一致
    assert client.get("/images/IMG-A/download").content == data


def test_same_id_different_bytes_rejects_whole_package(client):
    register(client, "IMG-A", "IMG-B")
    assert upload_zip(
        client, make_zip({"IMG-A.jpg": jpg_bytes(b"v1"), "IMG-B.jpg": jpg_bytes(b"b")})
    ).status_code == 200

    # IMG-A 换成不同字节，IMG-B 同字节：整包拒绝
    r = upload_zip(
        client, make_zip({"IMG-A.jpg": jpg_bytes(b"v2-different"), "IMG-B.jpg": jpg_bytes(b"b")})
    )
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "blob_content_conflict"
    assert detail["conflicts"][0]["image_id"] == "IMG-A"
    # IMG-B 没有被重复写，仍可下载原字节
    assert client.get("/images/IMG-B/download").content == jpg_bytes(b"b")
    assert client.get("/images/IMG-A/download").content == jpg_bytes(b"v1")


def test_unregistered_id_rejects_whole_package(client):
    register(client, "IMG-A")
    r = upload_zip(
        client, make_zip({"IMG-A.jpg": jpg_bytes(b"a"), "GHOST.jpg": jpg_bytes(b"g")})
    )
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "unregistered_images"
    assert detail["missing_image_ids"] == ["GHOST"]
    # 整包拒绝：IMG-A 也不可下载
    assert client.get("/images/IMG-A/blob").json()["stored"] is False
    assert client.get("/images/IMG-A/download").status_code == 404
    assert client.get("/stats").json()["images_with_blob"] == 0


# ---------------------------------------------------------------------------
# ZIP 条目校验
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "entries,expected_code",
    [
        # 非顶层（嵌套路径）
        ({"sub/IMG-A.jpg": b"x"}, "nested_member"),
        # 后缀 / 命名
        ({"IMG-A.png": b"x"}, "not_jpeg"),
        ({"IMG-A.JPEG": b"x"}, "not_jpeg"),
        ({".jpg": b"x"}, "not_jpeg"),
        # ID 字符集
        ({"IMG A.jpg": b"x"}, "invalid_image_id"),
        ({"IMG.A.jpg": b"x"}, "invalid_image_id"),
        ({"IMG/A.jpg": b"x"}, "nested_member"),
        ({"IMG中文.jpg": b"x"}, "invalid_image_id"),
        # 空文件
        ({"IMG-A.jpg": b""}, "empty_file"),
        # 空 ZIP
        ({}, "empty_zip"),
    ],
)
def test_invalid_members_rejected(client, entries, expected_code):
    register(client, "IMG-A")
    r = upload_zip(client, make_zip(entries))
    assert r.status_code == 400, r.text
    assert r.json()["detail"]["error"] == expected_code


def test_directory_entry_rejected(client):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("dir/", b"")
        zf.writestr("IMG-A.jpg", jpg_bytes(b"a"))
    register(client, "IMG-A")
    r = upload_zip(client, buf.getvalue())
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "invalid_member"


def test_duplicate_id_same_name_rejected(client):
    register(client, "IMG-A")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("IMG-A.jpg", jpg_bytes(b"a"))
        zf.writestr("IMG-A.jpg", jpg_bytes(b"a-again"))
    r = upload_zip(client, buf.getvalue())
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "duplicate_id"


def test_not_a_zip_rejected(client):
    r = upload_zip(client, b"this is not a zip at all")
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "not_a_zip"


def test_corrupt_member_rejected(client, tmp_path):
    register(client, "IMG-A")
    good = make_zip({"IMG-A.jpg": jpg_bytes(b"a")}, compress=zipfile.ZIP_STORED)
    # 仅破坏本地文件头之后的数据区字节，使 CRC 校验失败而 ZIP 结构仍可打开
    corrupted = bytearray(good)
    for i in range(64, 72):
        corrupted[i] ^= 0xFF
    r = upload_zip(client, bytes(corrupted))
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "corrupt_member"


def test_symlink_entry_rejected(client, tmp_path):
    register(client, "IMG-A")
    # 手工构造带符号链接模式位的 ZIP 条目
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zi = zipfile.ZipInfo("IMG-A.jpg")
        zi.create_system = 3  # unix
        zi.external_attr = (0o120777 << 16)  # symlink
        zf.writestr(zi, b"IMG-A.jpg")
    r = upload_zip(client, buf.getvalue())
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "invalid_member"


# ---------------------------------------------------------------------------
# 原子性 / 无半包 / 清理
# ---------------------------------------------------------------------------

def test_failed_upload_leaves_no_staging_or_spool(client, tmp_path):
    register(client, "IMG-A")
    # 未登记 ID -> 拒绝
    upload_zip(client, make_zip({"GHOST.jpg": jpg_bytes(b"g")}))
    data_root = tmp_path / "data"
    # staging / tmp 清空；blobs 下没有任何文件
    assert list((data_root / "staging").iterdir()) == []
    assert list((data_root / "tmp").iterdir()) == []
    blob_files = [p for p in (data_root / "blobs").rglob("*.bin")]
    assert blob_files == []


def test_cleanup_does_not_delete_other_published_blobs(client, tmp_path):
    register(client, "IMG-A", "IMG-B", "GHOST")
    upload_zip(client, make_zip({"IMG-A.jpg": jpg_bytes(b"a"), "IMG-B.jpg": jpg_bytes(b"b")}))
    published = sorted(p for p in (tmp_path / "data" / "blobs").rglob("*.bin"))
    assert len(published) == 2

    # 后续失败上传（未登记 + 异字节冲突各一次）
    upload_zip(client, make_zip({"GHOST.jpg": jpg_bytes(b"g"), "NOPE.jpg": jpg_bytes(b"n")}))
    upload_zip(client, make_zip({"IMG-A.jpg": jpg_bytes(b"changed")}))
    # 已发布的两个 blob 原样保留且可下载
    assert sorted(p for p in (tmp_path / "data" / "blobs").rglob("*.bin")) == published
    assert client.get("/images/IMG-A/download").content == jpg_bytes(b"a")
    assert client.get("/images/IMG-B/download").content == jpg_bytes(b"b")
    assert list((tmp_path / "data" / "staging").iterdir()) == []


def test_publish_failure_rolls_back_and_removes_promoted_blobs(client, tmp_path, monkeypatch):
    register(client, "IMG-A", "IMG-B")
    payload = make_zip({"IMG-A.jpg": jpg_bytes(b"a"), "IMG-B.jpg": jpg_bytes(b"b")})

    import app.storage as storage_mod

    real_replace = os.replace
    calls = {"n": 0}

    def flaky(src, dst):
        calls["n"] += 1
        if calls["n"] == 2:  # 第二个 blob 落位后、写引用行前模拟 IO 失败
            raise OSError("simulated disk failure")
        return real_replace(src, dst)

    monkeypatch.setattr(storage_mod.os, "replace", flaky)
    r = upload_zip(client, payload)
    assert r.status_code == 500
    # 事务回滚：无引用、无半包、暂存清空
    assert client.get("/stats").json()["images_with_blob"] == 0
    assert client.get("/images/IMG-A/download").status_code == 404
    assert list((tmp_path / "data" / "staging").iterdir()) == []
    # 第一个已落位的 blob 被立即回收（不留孤儿）
    assert list((tmp_path / "data" / "blobs").rglob("*.bin")) == []

    # 修复后重新上传可成功（状态干净）。只恢复 os.replace，避免 undo 撤销环境变量补丁
    monkeypatch.setattr(storage_mod.os, "replace", real_replace)
    r = upload_zip(client, payload)
    assert r.status_code == 200 and r.json()["published"] == 2
    assert client.get("/images/IMG-A/download").status_code == 200


def test_restart_keeps_published_blobs_and_cleans_staging(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "r.db"))
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))

    with TestClient(app) as c1:
        register(c1, "IMG-A", "IMG-B")
        upload_zip(c1, make_zip({"IMG-A.jpg": jpg_bytes(b"a"), "IMG-B.jpg": jpg_bytes(b"b")}))
        # 制造一个“崩溃残留”暂存目录（模拟进程在解包期间被杀）
        leftover = tmp_path / "data" / "staging" / "deadbeef"
        leftover.mkdir(parents=True)
        (leftover / "IMG-A.jpg").write_bytes(jpg_bytes(b"half"))
        (tmp_path / "data" / "tmp" / "spool-x.zip").write_bytes(b"partial")

    with TestClient(app) as c2:
        # 已成功入库的文件重启后仍可下载，字节不变
        assert c2.get("/images/IMG-A/download").content == jpg_bytes(b"a")
        assert c2.get("/images/IMG-B/blob").json()["stored"] is True
        # 残留暂存与 spool 被清理
        assert not leftover.exists()
        assert not (tmp_path / "data" / "tmp" / "spool-x.zip").exists()


def test_orphan_blob_from_crash_after_promotion_is_gced(tmp_path, monkeypatch):
    """崩溃于 rename 落位之后、事务提交之前：blob 无引用，重启回收。"""
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "r.db"))
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    with TestClient(app) as c:
        register(c, "IMG-A")
        upload_zip(c, make_zip({"IMG-A.jpg": jpg_bytes(b"a")}))
        # 手工放一个无引用 blob，模拟提交前崩溃残留
        from app import storage

        orphan_dir = tmp_path / "data" / "blobs" / "ab"
        orphan_dir.mkdir(parents=True, exist_ok=True)
        (orphan_dir / "abcdef0123456789.bin").write_bytes(jpg_bytes(b"orphan"))

    with TestClient(app):
        assert not (orphan_dir / "abcdef0123456789.bin").exists()
        # 有引用的 blob 不受影响
        with TestClient(app) as c2:
            assert c2.get("/images/IMG-A/download").status_code == 200


# ---------------------------------------------------------------------------
# 状态接口边界
# ---------------------------------------------------------------------------

def test_status_endpoints_for_unknown_and_pending_ids(client):
    # 未登记
    r = client.get("/images/NOPE/blob")
    assert r.status_code == 404
    assert client.get("/images/NOPE/download").status_code == 404

    register(client, "IMG-A")
    st = client.get("/images/IMG-A/blob").json()
    assert st == {
        "image_id": "IMG-A",
        "registered": True,
        "stored": False,
        "filename": None,
        "sha256": None,
        "size_bytes": None,
        "stored_at": None,
    }
    assert client.get("/images/IMG-A/download").status_code == 404


# ---------------------------------------------------------------------------
# 分块 / 大文件
# ---------------------------------------------------------------------------

def test_large_entry_chunked_not_buffered(client, monkeypatch):
    # 单张 3 MiB + 多张，超过分块大小，验证分块处理与原字节完整
    monkeypatch.setenv("MAX_UNCOMPRESSED_BYTES", "0")
    register(client, "BIG", "S1")
    big = jpg_bytes(b"big", size=3 * 1024 * 1024 + 17)
    small = jpg_bytes(b"s", size=128)
    r = upload_zip(client, make_zip({"BIG.jpg": big, "S1.jpg": small}))
    assert r.status_code == 200, r.text
    assert client.get("/images/BIG/download").content == big
    assert client.get("/images/S1/download").content == small
    assert client.get("/images/BIG/blob").json()["size_bytes"] == len(big)


def test_uncompressed_cap_enforced(client, monkeypatch):
    monkeypatch.setenv("MAX_UNCOMPRESSED_BYTES", "1024")
    register(client, "IMG-A")
    r = upload_zip(client, make_zip({"IMG-A.jpg": jpg_bytes(b"a", size=4096)}))
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "too_large"


def test_content_dedup_shares_blob_file(client, tmp_path):
    # 不同 ID、相同字节 -> 物理 blob 只存一份
    register(client, "IMG-A", "IMG-B")
    same = jpg_bytes(b"same")
    upload_zip(client, make_zip({"IMG-A.jpg": same, "IMG-B.jpg": same}))
    blobs = list((tmp_path / "data" / "blobs").rglob("*.bin"))
    assert len(blobs) == 1
    assert blobs[0].read_bytes() == same


# ---------------------------------------------------------------------------
# 并发
# ---------------------------------------------------------------------------

def test_concurrent_uploads_serialize_safely(client):
    ids = [f"CID-{i:02d}" for i in range(8)]
    register(client, *ids)

    def worker(i):
        c = TestClient(app)
        try:
            # 一半线程重传同一批公共 ID 同字节（应复用），一半传各自 ID
            if i % 2 == 0:
                payload = make_zip({"CID-00.jpg": jpg_bytes(b"common")})
            else:
                payload = make_zip({f"CID-{i:02d}.jpg": jpg_bytes(str(i).encode())})
            return upload_zip(c, payload).status_code
        finally:
            c.close()

    # 先把公共字节发布一次
    upload_zip(client, make_zip({"CID-00.jpg": jpg_bytes(b"common")}))
    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = list(pool.map(worker, range(8)))
    assert all(code == 200 for code in codes), codes

    # 每个 ID 都可下载，且 CID-00 字节未被覆盖
    assert client.get("/images/CID-00/download").content == jpg_bytes(b"common")
    for i in (1, 3, 5, 7):
        assert client.get(f"/images/CID-{i:02d}/download").content == jpg_bytes(str(i).encode())


def test_concurrent_conflicting_upload_whole_reject(client):
    register(client, "IMG-A")
    upload_zip(client, make_zip({"IMG-A.jpg": jpg_bytes(b"v1")}))

    def worker(data: bytes):
        c = TestClient(app)
        try:
            return upload_zip(c, make_zip({"IMG-A.jpg": data})).status_code
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        codes = list(pool.map(worker, [jpg_bytes(b"v1"), jpg_bytes(b"v2"), jpg_bytes(b"v3"), jpg_bytes(b"v1")]))
    # 同字节成功（复用），异字节 409；绝不会出现异字节覆盖
    assert set(codes) <= {200, 409}
    assert 409 in codes
    assert client.get("/images/IMG-A/download").content == jpg_bytes(b"v1")


def test_zip_ingest_concurrent_with_csv_import_and_assign(client):
    """解包/入库期间 CSV 导入与分区操作仍可用（长耗时解包不持写锁）。"""
    register(client, "IMG-A")
    barrier = threading.Event()

    def slow_ingest():
        # 较大的包，制造解包窗口；在另一个线程里做 CSV 导入与分区
        payload = make_zip({"IMG-A.jpg": jpg_bytes(b"a", size=2 * 1024 * 1024)})
        barrier.set()
        return upload_zip(client, payload)

    t = threading.Thread(target=slow_ingest)
    t.start()
    barrier.wait(timeout=5)
    # 与 ZIP 入库并发执行原有操作
    r_import = register(client, "IMG-Z", "IMG-Y")
    r_assign = client.post(
        "/partitions/assign", json={"image_id": "IMG-Z", "partition": "train"}
    )
    t.join()
    assert r_import.status_code == 200
    assert r_assign.status_code == 200
    assert client.get("/images/IMG-Z").json()["partition"] == "train"
    assert client.get("/images/IMG-A/download").content == jpg_bytes(
        b"a", size=2 * 1024 * 1024
    )
