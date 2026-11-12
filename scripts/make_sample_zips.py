"""生成 ZIP 入库演示用样例（纯标准库，无需第三方依赖）。

运行：python3 scripts/make_sample_zips.py
产物：sample_data/zips/*.zip，图片 ID 与 sample_data/batch1.csv 等 CSV 登记的 ID 对齐。

JPEG 字节由 SOI/APP0/COM/EOI 标记手工拼成，是结构合法的最小 JPEG；
每个 ID 的 COM 段写入不同内容，因此 SHA-256 各不相同，应用本身从不解码图像。
"""
from __future__ import annotations

import io
import os
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT, "sample_data", "zips")


def minimal_jpeg(comment: bytes, fill: int = 2048) -> bytes:
    """拼一个结构合法的最小 JPEG：SOI + APP0(JFIF) + COM(注释) + 填充 + EOI。"""
    app0 = b"\xff\xe0" + (len(b"JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00") + 2).to_bytes(2, "big") \
        + b"JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
    com_payload = b"quality-inspection-demo:" + comment
    com = b"\xff\xfe" + (len(com_payload) + 2).to_bytes(2, "big") + com_payload
    body = b"\xff\xd8" + app0 + com
    body += b"\x00" * max(0, fill - len(body) - 2)
    return body + b"\xff\xd9"


def write_zip(path: str, entries: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries.items():
            zf.writestr(name, data)


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    # 1. 首批 5 张，ID 与 batch1.csv 完全对齐（先导入 CSV 再上传即可成功）
    batch1 = {f"IMG00{i}.jpg": minimal_jpeg(f"IMG00{i}".encode()) for i in range(1, 6)}
    write_zip(os.path.join(OUT_DIR, "images_batch1.zip"), batch1)

    # 2. 同 ID 同字节重传：内容与 batch1 完全一致，演示幂等复用
    write_zip(os.path.join(OUT_DIR, "images_batch1_resend.zip"), batch1)

    # 3. 同 ID 异字节：IMG001 字节变化，演示整包拒绝
    conflict = dict(batch1)
    conflict["IMG001.jpg"] = minimal_jpeg(b"IMG001-changed-content")
    write_zip(os.path.join(OUT_DIR, "images_batch1_conflict.zip"), conflict)

    # 4. 含未登记 ID：IMG999 不在任何 CSV 中，演示整包拒绝
    unregistered = dict(batch1)
    unregistered["IMG999.jpg"] = minimal_jpeg(b"IMG999-unregistered")
    write_zip(os.path.join(OUT_DIR, "images_unregistered.zip"), unregistered)

    # 5. 命名不合法：嵌套路径 + 非法字符 + 错误后缀，演示 400 校验
    write_zip(
        os.path.join(OUT_DIR, "images_invalid_naming.zip"),
        {
            "nested/IMG001.jpg": minimal_jpeg(b"nested"),
            "IMG 002.jpg": minimal_jpeg(b"space"),
            "IMG003.png": b"not-a-jpeg",
        },
    )

    # 6. 后续批次：IMG006~IMG008，配合 batch2.csv / batch3_link.csv 演示
    later = {
        "IMG006.jpg": minimal_jpeg(b"IMG006"),
        "IMG007.jpg": minimal_jpeg(b"IMG007"),
        "IMG008.jpg": minimal_jpeg(b"IMG008"),
    }
    write_zip(os.path.join(OUT_DIR, "images_batch2.zip"), later)

    print("样例 ZIP 已生成到", OUT_DIR)
    for name in sorted(os.listdir(OUT_DIR)):
        size = os.path.getsize(os.path.join(OUT_DIR, name))
        print(f"  {name}  ({size} bytes)")


if __name__ == "__main__":
    main()
