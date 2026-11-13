"""并发等待、超时收尾与断连可靠性的专项测试。

覆盖修复需求：
  1. 40 笔上传并发等待时，health/批次/映射查询 1 秒内返回（共享线程池不被占用）；
  2. 等待超时返回 409，失败批次不残留 PENDING，独占未发布映射与暂存被清除，
     其他活跃批次与已发布映射不受影响，占用方结束后无需重启即可重传成功；
  3. 请求断连后，已开始的后台作业继续完成发布或回滚，再释放执行名额与连接；
  4. 不扩大 anyio 共享线程池：批次作业跑在固定大小的专用执行器中；
  5. 慢转换延时点不持有写锁（延时期间他批次可完成保留并发布）。
"""
from __future__ import annotations

import socket
import threading
import time
import uuid
from urllib.parse import urlparse

import httpx
import pydicom
import pytest

from tests.conftest import upload

WAITERS = 40
SLOW_SECONDS = 3.0
LOCK_WAIT = 2


@pytest.fixture(autouse=True)
def _short_lock_wait(monkeypatch, request):
    """默认把同源映射等待窗口缩短到 2 秒（不改生产默认 60 秒）。

    autouse 保证在 ``client`` 夹具之前完成环境变量设置与 settings 重建：
    服务线程惰性建立的 SQLite 连接会读取当前 settings。标记
    ``default_lock_wait`` 的测试跳过本覆盖，保留 60 秒默认值。
    数据目录仍由 ``client`` 夹具负责，这里只设置等待/并发参数。
    """
    if request.node.get_closest_marker("default_lock_wait"):
        yield
        return
    from app import config as config_module
    from app import jobs as jobs_module
    monkeypatch.setenv("DICOM_REMAP_LOCK_WAIT", str(LOCK_WAIT))
    monkeypatch.setenv("DICOM_REMAP_WORKERS", "8")
    config_module.reload_settings()
    # 执行器惰性创建；重置以确保新 worker 数生效
    jobs_module._executor = None
    yield
    config_module.reload_settings()
    jobs_module._executor = None


def _make_dicom(sample_dicom, tmp_path, suffix: str):
    return sample_dicom(tmp_path / f"f_{suffix}.dcm")


def _multipart_body(project_id: str, filename: str, content: bytes,
                    slow_seconds: float | None) -> tuple[bytes, str]:
    boundary = f"----discboundary{uuid.uuid4().hex}"
    parts: list[bytes] = []

    def field(name: str, value: str) -> None:
        parts.append(
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n"
            f"{value}\r\n".encode()
        )

    field("project_id", project_id)
    if slow_seconds is not None:
        field("slow_conversion_seconds", str(slow_seconds))
    parts.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; "
        f"filename=\"{filename}\"\r\nContent-Type: application/dicom\r\n\r\n".encode()
    )
    parts.append(content)
    parts.append(b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    return b"".join(parts), boundary


@pytest.mark.default_lock_wait
def test_query_sla_under_40_concurrent_waiting_uploads(
    client, project_id, sample_dicom, tmp_path
):
    """40 笔上传并发等待期间，查询接口必须在 1 秒内返回已提交/在途状态。"""
    # 各自携带唯一 SOP，避免相互阻塞，但全部进入专用执行器排队/慢转换
    paths = [
        _make_dicom(sample_dicom, tmp_path, f"u{i}_{uuid.uuid4().int % 10**8}")
        for i in range(WAITERS)
    ]

    results: list[httpx.Response] = []
    errors: list[Exception] = []
    query_samples: list[tuple[str, float]] = []
    barrier = threading.Barrier(WAITERS + 1, timeout=30)

    def waiter(path):
        try:
            barrier.wait()
            results.append(upload(client, project_id, [path], slow_seconds=SLOW_SECONDS))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=waiter, args=(p,)) for p in paths]
    for t in threads:
        t.start()
    barrier.wait()  # 与所有上传线程同时放行，最大化并发峰值

    # 上传进行中反复打查询接口：全部必须在 1 秒 SLA 内返回
    deadline = time.monotonic() + SLOW_SECONDS + 2.0
    while time.monotonic() < deadline:
        for path, call in (
            ("/api/health", lambda: client.get("/api/health", timeout=1.0)),
            ("/api/batches",
             lambda: client.get("/api/batches", params={"project_id": project_id}, timeout=1.0)),
            ("/api/mappings",
             lambda: client.get("/api/mappings", params={"project_id": project_id}, timeout=1.0)),
        ):
            start = time.monotonic()
            resp = call()
            elapsed = time.monotonic() - start
            assert resp.status_code == 200, f"{path} 返回 {resp.status_code}"
            assert elapsed < 1.0, f"{path} 耗时 {elapsed:.3f}s 超过 1 秒 SLA"
            query_samples.append((path, elapsed))
        time.sleep(0.15)

    for t in threads:
        t.join(timeout=30)
    assert not errors
    assert len(results) == WAITERS
    assert sorted(r.status_code for r in results) == [201] * WAITERS

    # 至少在上传仍在途时采到了查询样本
    assert query_samples
    # 同源映射唯一：每个文件各有唯一 SOP，映射行数等于唯一源数（无分叉）
    mappings = client.get("/api/mappings", params={"project_id": project_id}).json()
    sops = [m for m in mappings if m["id_kind"] == "sop_uid"]
    assert len({m["source_value"] for m in sops}) == len(sops)
    assert client.get("/api/health").json()["inflight_jobs"] == 0


def test_timeout_returns_409_and_cleans_up_everything(
    client, project_id, sample_dicom, tmp_path
):
    """等待超时 -> 409；失败批次不残留 PENDING，映射/暂存清除，可立即重传。"""
    from app import db
    from app.config import settings

    holder = _make_dicom(sample_dicom, tmp_path, "holder")
    hds = pydicom.dcmread(str(holder))
    conflict_sop = str(hds.SOPInstanceUID)

    # 占用方：保留同源映射后进入慢转换，长时间不发布。
    # 使用独立 httpx 客户端，避免与轮询/断言共用连接产生跨线程干扰。
    base = str(client.base_url)
    holder_resp_box: list[httpx.Response] = []

    def hold():
        with httpx.Client(timeout=30) as holder_client:
            holder_resp_box.append(
                holder_client.post(
                    f"{base}/api/batches",
                    data={"project_id": project_id,
                          "slow_conversion_seconds": str(SLOW_SECONDS + 1.5)},
                    files={"files": (holder.name, holder.read_bytes(), "application/dicom")},
                )
            )

    holder_thread = threading.Thread(target=hold)
    holder_thread.start()

    # 等占用方进入慢转换（映射已 PENDING 保留、写锁已释放）
    stabilized = False
    for _ in range(50):
        rows = client.get("/api/batches", params={"project_id": project_id}).json()
        if any(b["status"] == "PENDING" for b in rows):
            stabilized = True
            break
        time.sleep(0.1)
    assert stabilized, "占用方批次未进入 PENDING"

    # 等待方：覆盖为与占用方完全相同的四个标识，确保命中同一条 PENDING 映射
    waiter = _make_dicom(sample_dicom, tmp_path, "waiter")
    wds = pydicom.dcmread(str(waiter))
    wds.PatientID = str(hds.PatientID)
    wds.StudyInstanceUID = str(hds.StudyInstanceUID)
    wds.SeriesInstanceUID = str(hds.SeriesInstanceUID)
    wds.SOPInstanceUID = str(hds.SOPInstanceUID)
    wds.file_meta.MediaStorageSOPInstanceUID = str(hds.SOPInstanceUID)
    wds.save_as(str(waiter), enforce_file_format=True)

    started = time.monotonic()
    resp = upload(client, project_id, [waiter])
    waited = time.monotonic() - started
    assert resp.status_code == 409, resp.text
    # 不能缩短等待窗口：409 至少等满 LOCK_WAIT 秒（给出调度余量）
    assert waited >= LOCK_WAIT - 0.5

    # 等待方批次必须为 FAILED（不得留在 PENDING），且暂存目录已清理
    batches = client.get("/api/batches", params={"project_id": project_id}).json()
    waiter_rows = [b for b in batches if b["status"] == "FAILED"
                   and "映射保留失败" in (b["error"] or "")]
    assert waiter_rows, "等待方批次未记录为 FAILED"
    waiter_batch_id = waiter_rows[0]["id"]
    assert not (settings.staging_dir / waiter_batch_id).exists()

    # 数据库中不存在由等待方创建的 PENDING 映射行；同源映射仍只有一条
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT * FROM id_mappings WHERE created_batch_id=? AND status='PENDING'",
            (waiter_batch_id,),
        ).fetchall()
        assert rows == []
        same = conn.execute(
            "SELECT COUNT(*) AS c FROM id_mappings "
            "WHERE project_id=? AND id_kind='sop_uid' AND source_value=?",
            (project_id, conflict_sop),
        ).fetchone()
        assert same["c"] == 1
    finally:
        conn.close()

    # 等占用方结束：必须发布成功（活跃批次与已发布映射保留）
    holder_thread.join(timeout=30)
    assert holder_resp_box and holder_resp_box[0].status_code == 201
    holder_id = holder_resp_box[0].json()["id"]

    # 占用方结束后无需重启即可重传成功（复用已发布映射）
    retry = upload(client, project_id, [waiter])
    assert retry.status_code == 201, retry.text

    # 最终：同源映射唯一且已发布，两个批次都 PUBLISHED，无任何 PENDING 遗留
    conn = db.connect()
    try:
        final_rows = conn.execute(
            "SELECT status, COUNT(*) AS c FROM id_mappings "
            "WHERE project_id=? AND id_kind='sop_uid' AND source_value=? GROUP BY status",
            (project_id, conflict_sop),
        ).fetchall()
        assert [(r["status"], r["c"]) for r in final_rows] == [("PUBLISHED", 1)]
        statuses = {
            r["status"]
            for r in conn.execute(
                "SELECT status FROM batches WHERE id IN (?, ?)",
                (holder_id, retry.json()["id"]),
            ).fetchall()
        }
        assert statuses == {"PUBLISHED"}
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM batches WHERE status='PENDING'"
        ).fetchone()["c"] == 0
    finally:
        conn.close()


def test_client_disconnect_background_job_completes(
    client, project_id, sample_dicom, tmp_path
):
    """请求断连后，已开始的后台作业必须完成发布，并释放执行名额与连接。"""
    path = _make_dicom(sample_dicom, tmp_path, f"disc_{uuid.uuid4().int % 10**8}")
    content = path.read_bytes()
    body, boundary = _multipart_body(project_id, path.name, content, slow_seconds=3.0)

    parsed = urlparse(str(client.base_url))
    host, port = parsed.hostname, parsed.port

    # 用原始 socket 发完整个请求体后立即关闭连接：服务端作业已开始却拿不到响应读者
    sock = socket.create_connection((host, port), timeout=10)
    request = (
        f"POST /api/batches HTTP/1.1\r\nHost: {host}:{port}\r\n"
        f"Content-Type: multipart/form-data; boundary={boundary}\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
    ).encode() + body
    sock.sendall(request)
    # 不读取任何响应，直接断开，模拟客户端在处理中途消失
    sock.close()

    # 记录断连前的“全部”批次 ID（含其他场景可能遗留的 FAILED 批次），
    # 之后只把真正新增的批次作为本次断连的产物来判定
    before = {
        b["id"] for b in
        client.get("/api/batches", params={"project_id": project_id}).json()
    }

    # 轮询等待后台作业自行完成发布（不依赖客户端连接）
    new_batch_id = None
    for _ in range(80):
        rows = client.get("/api/batches", params={"project_id": project_id}).json()
        fresh = [b for b in rows if b["id"] not in before]
        # 只检查断连之后新建的批次：其他测试/场景遗留的 FAILED 与本次无关
        assert all(b["status"] != "FAILED" for b in fresh)
        published = [b for b in fresh if b["status"] == "PUBLISHED"]
        if published:
            new_batch_id = published[0]["id"]
            break
        time.sleep(0.25)
    assert new_batch_id is not None, "断连后后台作业未在预期时间内完成发布"

    detail = client.get(f"/api/batches/{new_batch_id}").json()
    assert detail["status"] == "PUBLISHED"
    dl = client.get(f"/api/batches/{new_batch_id}/download")
    assert dl.status_code == 200 and dl.content

    # 执行名额已释放：后续上传无需重启即可立即成功，在途计数归零
    after = _make_dicom(sample_dicom, tmp_path, f"after_{uuid.uuid4().int % 10**8}")
    resp = upload(client, project_id, [after])
    assert resp.status_code == 201
    assert client.get("/api/health").json()["inflight_jobs"] == 0


@pytest.mark.default_lock_wait
def test_dedicated_executor_isolated_from_anyio_pool(
    client, project_id, sample_dicom, tmp_path
):
    """专用执行器固定 8 线程；占满时共享 anyio 池仍能服务查询，且不被扩容。"""
    import asyncio

    import anyio
    from app import jobs
    from app.config import settings

    async def _shared_pool_tokens() -> int:
        return anyio.to_thread.current_default_thread_limiter().total_tokens

    # anyio 共享池默认容量保持 40，本修复绝不修改它
    assert asyncio.run(_shared_pool_tokens()) == 40
    assert settings.workers == 8

    # 提交超过 worker 数的慢作业，占满专用执行器并形成排队
    paths = [
        _make_dicom(sample_dicom, tmp_path, f"iso{i}_{uuid.uuid4().int % 10**8}")
        for i in range(settings.workers + 3)
    ]
    box: list[httpx.Response] = []
    err: list[Exception] = []

    def worker(p):
        try:
            box.append(upload(client, project_id, [p], slow_seconds=2.0))
        except Exception as exc:  # pragma: no cover
            err.append(exc)

    threads = [threading.Thread(target=worker, args=(p,)) for p in paths]
    for t in threads:
        t.start()

    # 等待执行器被占满（在途数达到 worker 数即说明名额已满）
    saturated = False
    for _ in range(50):
        if jobs.inflight_count() >= settings.workers:
            saturated = True
            break
        time.sleep(0.1)
    assert saturated

    # 共享池仍可服务查询，且 1 秒 SLA 不受影响、容量未被扩大
    for _ in range(5):
        start = time.monotonic()
        r = client.get("/api/health", timeout=1.0)
        assert r.status_code == 200 and time.monotonic() - start < 1.0
        assert asyncio.run(_shared_pool_tokens()) == 40
        time.sleep(0.1)

    for t in threads:
        t.join(timeout=30)
    assert not err
    assert sorted(r.status_code for r in box) == [201] * len(paths)


def test_slow_conversion_does_not_hold_write_lock(
    client, project_id, sample_dicom, tmp_path
):
    """慢转换延时点在保留事务提交之后：延时期间他批次可完成保留并发布。"""
    slow = _make_dicom(sample_dicom, tmp_path, f"slow_{uuid.uuid4().int % 10**8}")
    other = _make_dicom(sample_dicom, tmp_path, f"other_{uuid.uuid4().int % 10**8}")

    slow_box: list[httpx.Response] = []

    def slow_worker():
        slow_box.append(upload(client, project_id, [slow], slow_seconds=2.5))

    t = threading.Thread(target=slow_worker)
    t.start()

    # 等慢批次完成保留进入延时
    time.sleep(0.8)

    # 另一批次（不同源标识）应立即完成，而不是等慢批次结束
    start = time.monotonic()
    resp = upload(client, project_id, [other])
    elapsed = time.monotonic() - start
    assert resp.status_code == 201, resp.text
    assert elapsed < 2.0, f"他批次被慢转换阻塞了 {elapsed:.2f}s，说明写锁未释放"

    t.join(timeout=30)
    assert slow_box[0].status_code == 201
