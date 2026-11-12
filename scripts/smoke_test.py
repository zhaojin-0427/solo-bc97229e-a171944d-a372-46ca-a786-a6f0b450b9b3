"""对运行中的服务做端到端冒烟验证（默认 http://127.0.0.1:8000）。

用法：python3 scripts/smoke_test.py [base_url] [sample_data_dir]
覆盖：导入 -> 分组 -> 指定分区 -> 继承 -> 冲突拒绝 -> 幂等 -> 下载 -> 并发一致性
      -> ZIP 图像入库（分块、校验、复用、整包拒绝、查询/下载、并发）。
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
import sys
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
SAMPLES = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sample_data"
)
ZIPS = os.path.join(SAMPLES, "zips")

client = httpx.Client(base_url=BASE, timeout=30)
failures = []


def check(name, cond, extra=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f"  {extra}" if extra else ""))
    if not cond:
        failures.append(name)


def upload(path):
    with open(path, "rb") as f:
        return client.post("/imports", files={"file": (os.path.basename(path), f, "text/csv")})


def assign(image_id, partition):
    return client.post("/partitions/assign", json={"image_id": image_id, "partition": partition})


def get_image(image_id):
    return client.get(f"/images/{image_id}")


# 0. 健康检查
check("health", client.get("/health").json()["status"] == "ok")

# 1. 导入 batch1：IMG001-004 沿关联链成一组，IMG005 独立成组
r = upload(f"{SAMPLES}/batch1.csv")
check("导入 batch1", r.status_code == 200 and r.json()["inserted"] == 5, r.text[:200])
g1 = get_image("IMG001").json()["group_id"]
check(
    "关联链归为一组",
    len({get_image(i).json()["group_id"] for i in ("IMG001", "IMG002", "IMG003", "IMG004")}) == 1
    and get_image("IMG005").json()["group_id"] != g1,
)

# 2. 同内容重传：幂等
r = upload(f"{SAMPLES}/batch1.csv")
check("同内容重传不新增", r.status_code == 200 and r.json()["inserted"] == 0 and r.json()["skipped_duplicates"] == 5)

# 3. 异内容重传：整批拒绝
r = upload(f"{SAMPLES}/batch5_changed_content.csv")
check("异内容整批拒绝(409)", r.status_code == 409 and r.json()["detail"]["error"] == "content_conflict")

# 4. 按任一图片 ID 指定整组到 train
r = assign("IMG003", "train")
check("按任一图片ID指定整组", r.status_code == 200 and r.json()["images_in_group"] == 4)
check("整组生效", get_image("IMG001").json()["partition"] == "train")

# 5. 已指定不可改动
r = assign("IMG001", "val")
check("已指定不可改动(409)", r.status_code == 409 and r.json()["detail"]["error"] == "already_assigned")
r = assign("IMG001", "train")
check("同分区重复指定幂等", r.status_code == 200 and r.json()["changed"] is False)

# 6. 导入 batch2：IMG006 经 LOT-2 关联 train 组 -> 继承 train；IMG007 新组
r = upload(f"{SAMPLES}/batch2.csv")
check("导入 batch2", r.status_code == 200 and r.json()["inserted"] == 2, r.text[:200])
check("新图继承关联组分区", get_image("IMG006").json()["partition"] == "train")
check("无关联新图未分区", get_image("IMG007").json()["partition"] is None)

# 7. IMG005 组指定为 val
check("指定 val", assign("IMG005", "val").status_code == 200)

# 8. 导入 batch3：IMG008 连接 val 组(WP-C)与未分区组(IMG007/LOT-4) -> 合并并继承 val
r = upload(f"{SAMPLES}/batch3_link.csv")
check("导入 batch3 连接 val 与未分区组", r.status_code == 200 and r.json()["components"][0]["partition"] == "val", r.text[:200])
check("被合并组继承 val", get_image("IMG007").json()["partition"] == "val")

# 9. 导入 batch4：IMG009 同时关联 val(WP-C) 与 train(LOT-2) -> 整批拒绝
r = upload(f"{SAMPLES}/batch4_conflict.csv")
detail = r.json().get("detail", {})
check(
    "连通两个不同分区整批拒绝(409)",
    r.status_code == 409 and detail.get("error") == "partition_conflict"
    and set(detail["component"]["partitions"]) == {"train", "val"},
    r.text[:300],
)
check("冲突导入未留脏数据", get_image("IMG009").status_code == 404)

# 10. 未分区列表应为空（IMG007 已随组合并进 val）
un = client.get("/images/unpartitioned").json()
check("未分区列表", un["count"] == 0, f"count={un['count']}")

# 11. 分区 CSV 下载：train 5 张、val 3 张
train_rows = list(csv.DictReader(io.StringIO(client.get("/downloads/train").text)))
val_rows = list(csv.DictReader(io.StringIO(client.get("/downloads/val").text)))
check("train CSV 完整", sorted(r["image_id"] for r in train_rows) == ["IMG001", "IMG002", "IMG003", "IMG004", "IMG006"])
check("val CSV 完整", sorted(r["image_id"] for r in val_rows) == ["IMG005", "IMG007", "IMG008"])

# 12. HTTP 级并发：10 线程混合导入/指定分区/下载，结果必须一致
def worker(i):
    c = httpx.Client(base_url=BASE, timeout=30)
    try:
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(["image_id", "workpiece_no", "batch_no"])
        # 每个线程的新图都关联到 train 组（LOT-1），应全部继承 train
        w.writerow([f"CONC-{i:02d}", f"WP-CONC-{i}", "LOT-1"])
        r1 = c.post("/imports", files={"file": ("c.csv", buf.getvalue().encode(), "text/csv")})
        r2 = c.post("/partitions/assign", json={"image_id": "IMG002", "partition": "train"})
        r3 = c.get("/downloads/train")
        return r1.status_code, r2.status_code, r3.status_code
    finally:
        c.close()

with ThreadPoolExecutor(max_workers=10) as pool:
    outcomes = list(pool.map(worker, range(10)))
check("并发导入全部成功", all(o[0] == 200 for o in outcomes), str(outcomes))
check("并发指定分区均合法", all(o[1] in (200, 409) for o in outcomes))
check("并发下载均成功", all(o[2] == 200 for o in outcomes))
conc_ok = all(
    get_image(f"CONC-{i:02d}").json()["partition"] == "train" for i in range(10)
)
check("并发新图全部继承 train", conc_ok)
train_rows = list(csv.DictReader(io.StringIO(client.get("/downloads/train").text)))
check("并发后 train CSV 完整一致", len(train_rows) == 15, f"rows={len(train_rows)}")

# ---------------------------------------------------------------------------
# 13. ZIP 图像入库
# ---------------------------------------------------------------------------

def upload_zip(name):
    with open(os.path.join(ZIPS, name), "rb") as f:
        return client.post(
            "/images/zip-ingests",
            files={"file": (name, f, "application/zip")},
        )


def zip_bytes(name, member=None):
    with zipfile.ZipFile(os.path.join(ZIPS, name)) as zf:
        if member is None:
            member = zf.namelist()[0]
        return zf.read(member)


# 入库前：已登记但无图像
st = client.get("/images/IMG001/blob").json()
check("入库前状态 stored=false", st["registered"] is True and st["stored"] is False)
check("入库前下载 404", client.get("/images/IMG001/download").status_code == 404)

# 13.1 正常入库 5 张
r = upload_zip("images_batch1.zip")
check("ZIP 首批入库", r.status_code == 200 and r.json()["published"] == 5, r.text[:200])
check(
    "状态接口可查",
    client.get("/images/IMG003/blob").json()["stored"] is True
    and client.get("/images/IMG003/blob").json()["filename"] == "IMG003.jpg",
)

# 13.2 下载原字节，SHA-256 与本地一致
r = client.get("/images/IMG001/download")
local = zip_bytes("images_batch1.zip", "IMG001.jpg")
check(
    "下载原字节一致且 SHA-256 正确",
    r.status_code == 200
    and r.content == local
    and r.headers["X-Content-SHA256"] == hashlib.sha256(local).hexdigest(),
)

# 13.3 同 ID 同字节重传：全部复用
r = upload_zip("images_batch1_resend.zip")
check("同字节重传复用", r.status_code == 200 and r.json()["reused"] == 5 and r.json()["published"] == 0)

# 13.4 同 ID 异字节：整包拒绝，原文件仍可下载
r = upload_zip("images_batch1_conflict.zip")
detail = r.json().get("detail", {})
check(
    "异字节整包拒绝(409)",
    r.status_code == 409 and detail.get("error") == "blob_content_conflict"
    and detail["conflicts"][0]["image_id"] == "IMG001",
    r.text[:200],
)
check("冲突后原字节不变", client.get("/images/IMG001/download").content == local)

# 13.5 未登记 ID：整包拒绝
r = upload_zip("images_unregistered.zip")
detail = r.json().get("detail", {})
check(
    "未登记 ID 整包拒绝(409)",
    r.status_code == 409
    and detail.get("error") == "unregistered_images"
    and detail["missing_image_ids"] == ["IMG999"],
    r.text[:200],
)
check("整包拒绝不留半包", client.get("/images/IMG002/blob").json()["stored"] is True)

# 13.6 命名/结构不合法：400
r = upload_zip("images_invalid_naming.zip")
check("非法 ZIP 条目 400", r.status_code == 400 and r.json()["detail"]["error"] in
      ("nested_member", "invalid_image_id", "not_jpeg"), r.text[:200])

# 13.7 非 ZIP 文件：400
r = client.post(
    "/images/zip-ingests",
    files={"file": ("fake.zip", b"not a zip", "application/zip")},
)
check("非 ZIP 400", r.status_code == 400 and r.json()["detail"]["error"] == "not_a_zip")

# 13.8 未登记 ID 的状态/下载接口 404
check("未知 ID 状态 404", client.get("/images/NOPE/blob").status_code == 404)
check("未知 ID 下载 404", client.get("/images/NOPE/download").status_code == 404)

# 13.9 后续批次入库（IMG006~IMG008 已在前面的 CSV 流程登记）
r = upload_zip("images_batch2.zip")
check("后续批次入库", r.status_code == 200 and r.json()["published"] == 3, r.text[:200])
check("stats 已入库计数", client.get("/stats").json()["images_with_blob"] == 8)

# 13.10 HTTP 级并发：同字节重传与各自新包混合，全部合法且无覆盖
def zip_worker(i):
    c = httpx.Client(base_url=BASE, timeout=30)
    try:
        if i % 2 == 0:
            with open(os.path.join(ZIPS, "images_batch1_resend.zip"), "rb") as f:
                return c.post(
                    "/images/zip-ingests",
                    files={"file": ("r.zip", f, "application/zip")},
                ).status_code
        # 奇数线程传一个只含已登记 IMG005 的小 ZIP
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("IMG005.jpg", zip_bytes("images_batch1.zip", "IMG005.jpg"))
        return c.post(
            "/images/zip-ingests",
            files={"file": ("s.zip", buf.getvalue(), "application/zip")},
        ).status_code
    finally:
        c.close()

with ThreadPoolExecutor(max_workers=8) as pool:
    codes = list(pool.map(zip_worker, range(8)))
check("并发 ZIP 上传全部合法", all(code == 200 for code in codes), str(codes))
check("并发后 IMG001 字节未被覆盖", client.get("/images/IMG001/download").content == local)

stats = client.get("/stats").json()
print("最终状态:", stats)
# 5(batch1) + 2(batch2) + 1(batch3) + 10(并发) = 18；冲突与异内容导入被拒绝不计入
check("最终图片数=18", stats["images"] == 18)
check("最终已入库图像=8", stats["images_with_blob"] == 8)

print()
if failures:
    print(f"共 {len(failures)} 项失败: {failures}")
    sys.exit(1)
print("全部冒烟检查通过 ✔")
