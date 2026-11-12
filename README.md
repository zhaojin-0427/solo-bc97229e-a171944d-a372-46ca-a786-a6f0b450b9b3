# 质检数据集划分服务

面向质检算法团队的训练/验证集划分与图像入库 API。接收标注系统导出的 CSV（图片ID、工件号、拍摄批次），
管理目录信息并保证**同一工件或同批拍摄的图片不会同时进入训练集和验证集**；
另接收相机导出的 ZIP，把图像原字节按已登记的图片 ID 补入本地数据卷，供训练任务按划分清单取图。

## 快速开始

```bash
docker compose up --build
```

服务启动后打开交互式 API 文档：**http://localhost:8000/docs**（Swagger UI，可直接试调所有接口，
包括 ZIP 上传与图像下载）。

SQLite 数据库与图像原字节统一落在命名卷 `split_data`（容器内 `/data`）中，
容器重启/重建后目录关系、分区与已入库图像均不丢失。

## 划分规则

- **关联成组**：同工件号或同拍摄批次的图片，沿关联链（连通分量）归为同一组。
- **整组划分**：按组内任一图片 ID 调用 `POST /partitions/assign`，整组指定到 `train` 或 `val`；
  已指定的组不可改动（返回 409），重复指定同一分区幂等成功。
- **分区继承**：后续导入的新图若关联到已分组，自动继承该组分区。
- **冲突拒绝**：一次导入若把两个不同分区的组连通，整批拒绝（409）并说明冲突涉及哪些组。
- **幂等导入**：同图片 ID 同内容重传不新增；同 ID 异内容整批拒绝（409）。
- **整批原子**：任何一行字段为空（400）或任何冲突，整批不落库。

## 一致性设计

- **连通分析在写事务外完成**：分析阶段只持有只读快照（SQLite WAL 模式，读写互不阻塞），
  期间其他导入和指定分区操作可正常提交。
- **提交时核对最新已确认状态**：进入短写事务后先比对全局状态版本号 `state_version`，
  若分析期间出现了新关联或分区变更，则放弃本次计划、基于最新状态重新分析（有限次重试），
  保证不漏掉新关联、不覆盖分区。
- **失败/重启安全**：所有变更在单个写事务（`BEGIN IMMEDIATE`）中原子提交，失败即整体回滚；
  数据库文件持久化在卷中，进程重启后图片关系与分区保持一致。
- **下载一致性**：每次 CSV 下载在单个只读快照事务中读取，拿到的是完整一致状态
  （响应头 `X-State-Version` 标识该快照对应的版本）。

## ZIP 图像入库

`POST /images/zip-ingests` 接收一个 ZIP，把相机导出的图像补入**已登记**的图片目录。

### 包内规则（任一不满足，整包拒绝，返回 400）

- 只接受 ZIP **顶层**的 `图片ID.jpg` 普通文件：不接受目录、嵌套路径、符号链接/特殊文件；
- 图片 ID 仅限英文字母、数字、下划线和短横线（`[A-Za-z0-9_-]+`），后缀必须为小写 `.jpg`；
- 包内图片 ID 不得重复；不接受空文件、加密条目与非 ZIP 文件。

### 入库语义（整包原子）

- **先登记后入库**：任一包内 ID 尚未经 CSV 导入登记，整包拒绝（409 `unregistered_images`）；
- **同 ID 同字节重传**：SHA-256 一致时复用既有引用，不重复写盘（响应 `reused`）；
- **同 ID 异字节**：整包拒绝（409 `blob_content_conflict`），已有文件保持不变；
- **并发上传**：发布在单个 `BEGIN IMMEDIATE` 写事务内串行化，多包并发同样遵守上述规则；
- **全部成功才可见**：所有 blob 落位与引用行在同一事务提交，提交前查不到、下不到，不存在半包；
- **失败/终止安全**：校验或写盘失败只回收本次暂存目录；进程被杀后由启动清理删除 `tmp/`、`staging/`
  残留，**不会触碰 `blobs/` 中其他上传已发布的文件**；无引用的孤儿 blob 仅在启动时回收。

### 字节与内存

- 上传流先分块 spool 到数据卷临时文件，再用 `zipfile` 按 1 MiB 分块流式解包，
  **ZIP 展开后可以远大于内存**；全程只做字节拷贝与 SHA-256，**不解码图像**；
- 图像原字节按 SHA-256 内容寻址存于 `DATA_DIR/blobs/<前2位>/<sha256>.bin`，同字节天然去重；
- SQLite 的 `image_blobs` 表只保存 `image_id → SHA-256/大小` 引用；
- 可选环境变量 `MAX_UNCOMPRESSED_BYTES`（默认 `0` 不限制）限制单包展开总字节，防 ZIP 炸弹。

### 数据卷布局

```
/data（命名卷 split_data）
├── app.db                 # SQLite：目录/分区 + image_blobs 引用与 SHA-256
├── tmp/spool-<uuid>.zip   # 上传中的 ZIP 临时文件，请求结束即删
├── staging/<upload_id>/   # 单次上传暂存区，提交前对外不可见，失败/重启即清
└── blobs/xx/<sha256>.bin  # 已发布图像原字节（内容寻址，去重）
```

### 查询与下载

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/images/{image_id}/blob` | 入库状态：未登记 404；已登记未入库 `stored=false`；已入库返回 SHA-256/大小/时间 |
| GET | `/images/{image_id}/download` | 流式下载原字节（`image/jpeg`，带 `ETag` 与 `X-Content-SHA256`），未入库 404 |

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/imports` | 导入 CSV（multipart 文件上传），整批原子生效 |
| POST | `/images/zip-ingests` | 上传 ZIP 批量入库图像原字节（仅顶层 `图片ID.jpg`），整包原子 |
| GET | `/images/{image_id}/blob` | 按图片 ID 查询图像入库状态与 SHA-256 |
| GET | `/images/{image_id}/download` | 按图片 ID 下载图像原字节 |
| POST | `/partitions/assign` | 按任一图片 ID 将整组指定到 `train`/`val` |
| GET | `/images/unpartitioned` | 未分区图片列表（JSON） |
| GET | `/downloads/{partition}` | 下载分区图片清单 CSV（`train`/`val`/`unpartitioned`） |
| GET | `/images/{image_id}` | 查询单张图片及其组、分区 |
| GET | `/groups` | 全部连通组及分区 |
| GET | `/stats` | 总体统计、已入库图像数与状态版本号 |

### CSV 格式

UTF-8（支持带 BOM），必需三列，列名支持中英文：

| 字段 | 可用列名 |
| --- | --- |
| 图片ID | `image_id`、`图片ID` |
| 工件号 | `workpiece_no`、`工件号` |
| 拍摄批次 | `batch_no`、`拍摄批次` |

## 演示流程（sample_data/）

在 Swagger UI 的 `POST /imports` 中点 *Try it out* 上传文件，或用 curl：

```bash
# 1. 导入首批：IMG001~IMG004 沿关联链成一组，IMG005 独立成组
curl -F "file=@sample_data/batch1.csv" http://localhost:8000/imports

# 2. 同内容重传：全部幂等跳过，inserted=0
curl -F "file=@sample_data/batch1.csv" http://localhost:8000/imports

# 3. 异内容重传：409 content_conflict，整批拒绝
curl -F "file=@sample_data/batch5_changed_content.csv" http://localhost:8000/imports

# 4. 按任一图片 ID 把整组（4 张）指定到 train
curl -X POST http://localhost:8000/partitions/assign \
     -H "Content-Type: application/json" -d '{"image_id":"IMG003","partition":"train"}'

# 5. 再导入：IMG006 经批次 LOT-2 关联 train 组 -> 自动继承 train；IMG007 新组未分区
curl -F "file=@sample_data/batch2.csv" http://localhost:8000/imports

# 6. IMG005 组指定为 val
curl -X POST http://localhost:8000/partitions/assign \
     -H "Content-Type: application/json" -d '{"image_id":"IMG005","partition":"val"}'

# 7. IMG008 连接 val 组与未分区组 -> 合并，整组继承 val
curl -F "file=@sample_data/batch3_link.csv" http://localhost:8000/imports

# 8. IMG009 同时关联 val(WP-C) 与 train(LOT-2) -> 409 partition_conflict，整批拒绝并说明冲突
curl -F "file=@sample_data/batch4_conflict.csv" http://localhost:8000/imports

# 9. 下载分区清单，供训练流水线选图
curl -OJ http://localhost:8000/downloads/train
curl -OJ http://localhost:8000/downloads/val
curl http://localhost:8000/images/unpartitioned

# 10. 生成演示用 ZIP（样例图片 ID 与上面 CSV 登记的 ID 对齐）
python3 scripts/make_sample_zips.py

# 11. ZIP 入库 5 张图像（IMG001~IMG005 已在第 1 步登记）
curl -F "file=@sample_data/zips/images_batch1.zip" http://localhost:8000/images/zip-ingests

# 12. 查询入库状态、下载原字节
curl http://localhost:8000/images/IMG001/blob
curl -OJ http://localhost:8000/images/IMG001/download

# 13. 同字节重传 -> 全部 reused；异字节 -> 409 整包拒绝；含未登记 ID -> 409 整包拒绝
curl -F "file=@sample_data/zips/images_batch1_resend.zip" http://localhost:8000/images/zip-ingests
curl -F "file=@sample_data/zips/images_batch1_conflict.zip" http://localhost:8000/images/zip-ingests
curl -F "file=@sample_data/zips/images_unregistered.zip" http://localhost:8000/images/zip-ingests
```

## 本地开发与测试

```bash
pip install -r requirements.txt
pip install httpx pytest
python3 -m pytest tests/            # 单元/集成测试（分组、幂等、冲突、并发、回滚、重启、ZIP 入库）

# 生成演示 ZIP（可选）
python3 scripts/make_sample_zips.py

# 启动本地服务并对跑端到端冒烟（含 HTTP 级并发与 ZIP 上传/下载验证）
uvicorn app.main:app &
python3 scripts/smoke_test.py http://127.0.0.1:8000 sample_data/
```

## 目录结构

```
app/
  main.py        # FastAPI 路由、CSV 解析校验、ZIP 入库/状态/下载接口
  core.py        # 连通分析（只读快照）+ 版本核对提交、指定分区
  db.py          # SQLite 连接、WAL、事务、schema（含 image_blobs）
  storage.py     # 分块 spool、流式解包、SHA-256、暂存区、内容寻址 blob、启动清理
sample_data/     # 演示用 CSV 与 zips/ 演示 ZIP（由 scripts/make_sample_zips.py 生成）
scripts/smoke_test.py
scripts/make_sample_zips.py
tests/test_api.py
tests/test_zip_ingest.py
Dockerfile / docker-compose.yml / requirements.txt
```
