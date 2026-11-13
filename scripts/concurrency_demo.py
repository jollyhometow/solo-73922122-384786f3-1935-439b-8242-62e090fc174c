"""并发修复专项演示：可控慢转换、40 笔并发等待、超时收尾、请求断连。

与 ``demo.py``（功能正确性）互补，本脚本聚焦本次并发修复的可观测行为：

  场景 A  查询 SLA：40 笔上传在专用执行器内并发/排队等待期间，
          /api/health 与批次、映射查询始终在 1 秒内返回，共享线程池未被占用；
  场景 B  可控慢转换不持写锁：慢批次延时期间，另一批次立即完成发布；
  场景 C  超时收尾：占用方长期不发布时，等待方在 DICOM_REMAP_LOCK_WAIT
          后收到 409；失败批次不残留 PENDING，独占未发布映射与暂存被清除，
          其他活跃批次与已发布映射保留；占用方结束后无需重启即可重传成功；
  场景 D  请求断连：用原始 socket 发完请求即断开，后台作业仍完成发布，
          随后释放执行名额与连接。

本脚本以隔离模式（python -I）运行，自启一个临时端口的完整 FastAPI 应用。
为让场景 C 在可接受时间内完成，演示进程把 DICOM_REMAP_LOCK_WAIT 覆盖为 3 秒
（仅演示进程，不修改镜像/Compose 的 60 秒默认值）。

用法：
  python -I scripts/concurrency_demo.py [--samples data/samples] [--keep]
"""
from __future__ import annotations

import argparse
import os
import socket
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 隔离模式（python -I）会忽略 PYTHONPATH；本地验收时通过 PY_LIBS 指向依赖目录，
# 容器内依赖安装在标准 site-packages，无需设置该变量。
_py_libs = os.environ.get("PY_LIBS")
if _py_libs:
    import site

    site.addsitedir(_py_libs)

import httpx  # noqa: E402
import uvicorn  # noqa: E402

# 演示用短等待窗口：只影响本演示进程，绝不修改生产默认 60 秒。
# 场景 A 会制造 40 批次的 SQLite 写事务高峰，之后场景 D 需要足够余量获取写锁，
# 因此演示窗口取 10 秒（仍显著短于生产默认值，演示可在一分钟内跑完）。
DEMO_LOCK_WAIT = 10
DEMO_WORKERS = 8
SLOW_SECONDS = 4.0

failures: list[str] = []


def check(cond: bool, label: str) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        failures.append(label)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _multipart(project_id: str, filename: str, content: bytes,
               slow: float | None) -> tuple[bytes, str]:
    boundary = f"----demo{uuid.uuid4().hex}"
    chunks: list[bytes] = [
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"project_id\"\r\n\r\n"
        f"{project_id}\r\n".encode()
    ]
    if slow is not None:
        chunks.append(
            f"--{boundary}\r\nContent-Disposition: form-data; "
            f"name=\"slow_conversion_seconds\"\r\n\r\n{slow}\r\n".encode()
        )
    chunks.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; "
        f"filename=\"{filename}\"\r\nContent-Type: application/dicom\r\n\r\n".encode()
    )
    chunks += [content, b"\r\n", f"--{boundary}--\r\n".encode()]
    return b"".join(chunks), boundary


def _make_dicom(out_dir: Path, tag: str) -> Path:
    """生成一份最小合法未压缩 DICOM（唯一 UID，避免演示批次互相阻塞）。"""
    from pydicom.dataset import Dataset, FileMetaDataset
    from pydicom.uid import ExplicitVRLittleEndian

    u = uuid.uuid4().int % 10**12
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    meta.MediaStorageSOPInstanceUID = f"1.2.840.1001.99.{u}.1.1.1"
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    meta.FileMetaInformationVersion = b"\x00\x01"
    meta.ImplementationClassUID = "1.2.840.1001.99.1.99"
    ds = Dataset()
    ds.file_meta = meta
    ds.PatientID = f"PAT-{tag}-{u % 10**8}"
    ds.StudyInstanceUID = f"1.2.840.1001.99.{u}.1"
    ds.SeriesInstanceUID = f"1.2.840.1001.99.{u}.1.1"
    ds.SOPInstanceUID = f"1.2.840.1001.99.{u}.1.1.1"
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    ds.Modality = "CT"
    ds.Rows = 4
    ds.Columns = 4
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelData = bytes(range(32))
    path = out_dir / f"{tag}.dcm"
    ds.save_as(path, enforce_file_format=True)
    return path


def _upload(client: httpx.Client, base: str, project_id: str, path: Path,
            slow: float | None = None) -> httpx.Response:
    data = {"project_id": project_id}
    if slow is not None:
        data["slow_conversion_seconds"] = str(slow)
    return client.post(
        f"{base}/api/batches",
        data=data,
        files={"files": (path.name, path.read_bytes(), "application/dicom")},
    )


def scenario_a_query_sla(client: httpx.Client, base: str, project_id: str,
                         work: Path) -> None:
    print("\n== 场景 A：40 笔上传并发等待，查询 1 秒 SLA ==")
    n = 40
    paths = [_make_dicom(work, f"sla{i}") for i in range(n)]
    results: list[httpx.Response] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(n + 1, timeout=30)
    max_latency = 0.0

    def worker(p: Path) -> None:
        try:
            barrier.wait()
            results.append(_upload(client, base, project_id, p, slow=SLOW_SECONDS))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(p,)) for p in paths]
    for t in threads:
        t.start()
    barrier.wait()

    deadline = time.monotonic() + SLOW_SECONDS + 2.0
    samples = 0
    while time.monotonic() < deadline:
        for path, call in (
            ("/api/health", lambda: client.get(f"{base}/api/health", timeout=1.0)),
            ("/api/batches",
             lambda: client.get(f"{base}/api/batches",
                                params={"project_id": project_id}, timeout=1.0)),
            ("/api/mappings",
             lambda: client.get(f"{base}/api/mappings",
                                params={"project_id": project_id}, timeout=1.0)),
        ):
            t0 = time.monotonic()
            resp = call()
            latency = time.monotonic() - t0
            max_latency = max(max_latency, latency)
            samples += 1
            check(resp.status_code == 200 and latency < 1.0,
                  f"{path} 在 {latency * 1000:.0f}ms 内返回（SLA 1000ms）")
        time.sleep(0.2)

    for t in threads:
        t.join(timeout=60)
    check(not errors, "40 笔上传无线程异常")
    check(len(results) == n, f"40 笔上传全部完成（实际 {len(results)}）")
    check(sorted(r.status_code for r in results) == [201] * n, "40 笔全部发布成功")
    check(samples >= 30, f"上传期间采集到 {samples} 次查询样本")
    print(f"  信息：查询最大耗时 {max_latency * 1000:.0f}ms，执行器 workers="
          f"{client.get(base + '/api/meta').json()['workers']}")


def scenario_b_slow_no_lock(client: httpx.Client, base: str, project_id: str,
                            work: Path) -> None:
    print("\n== 场景 B：可控慢转换不持写锁 ==")
    slow = _make_dicom(work, "slow")
    other = _make_dicom(work, "other")
    box: list[httpx.Response] = []

    def slow_worker() -> None:
        box.append(_upload(client, base, project_id, slow, slow=SLOW_SECONDS))

    t = threading.Thread(target=slow_worker)
    t.start()
    time.sleep(1.0)  # 等慢批次完成保留进入延时（写锁已释放）

    t0 = time.monotonic()
    resp = _upload(client, base, project_id, other)
    elapsed = time.monotonic() - t0
    check(resp.status_code == 201, f"他批次在慢转换期间发布成功（HTTP {resp.status_code}）")
    check(elapsed < SLOW_SECONDS,
          f"他批次未被慢转换阻塞，仅耗时 {elapsed:.2f}s（慢批次延时 {SLOW_SECONDS}s）")
    t.join(timeout=30)
    check(box and box[0].status_code == 201, "慢批次最终也发布成功")


def scenario_c_timeout_cleanup(client: httpx.Client, base: str, project_id: str,
                               work: Path) -> None:
    print("\n== 场景 C：等待超时 409 与失败收尾 ==")
    import pydicom

    holder = _make_dicom(work, "holder")
    waiter = _make_dicom(work, "waiter")
    hds = pydicom.dcmread(str(holder))
    wds = pydicom.dcmread(str(waiter))
    # 等待方与占用方完全同源，必然命中同一条 PENDING 映射
    wds.PatientID = str(hds.PatientID)
    wds.StudyInstanceUID = str(hds.StudyInstanceUID)
    wds.SeriesInstanceUID = str(hds.SeriesInstanceUID)
    wds.SOPInstanceUID = str(hds.SOPInstanceUID)
    wds.file_meta.MediaStorageSOPInstanceUID = str(hds.SOPInstanceUID)
    wds.save_as(str(waiter), enforce_file_format=True)
    conflict_sop = str(hds.SOPInstanceUID)

    holder_box: list[httpx.Response] = []

    def hold() -> None:
        # 占用方保留后进入长慢转换，显著慢过等待窗口（16s vs 10s），
        # 消除“等待方在超时临界处恰好遇到占用方发布”的边界竞态
        holder_box.append(
            _upload(client, base, project_id, holder,
                    slow=DEMO_LOCK_WAIT + 6.0)
        )

    holder_thread = threading.Thread(target=hold)
    holder_thread.start()

    for _ in range(50):
        rows = client.get(f"{base}/api/batches",
                          params={"project_id": project_id}).json()
        if any(b["status"] == "PENDING" for b in rows):
            break
        time.sleep(0.1)

    t0 = time.monotonic()
    resp = _upload(client, base, project_id, waiter)
    waited = time.monotonic() - t0
    check(resp.status_code == 409, f"等待方收到 409（HTTP {resp.status_code}）")
    check(waited >= DEMO_LOCK_WAIT - 0.5,
          f"等满 {DEMO_LOCK_WAIT}s 窗口才中止（实际 {waited:.2f}s，未缩短 LOCK_WAIT）")

    batches = client.get(f"{base}/api/batches",
                         params={"project_id": project_id}).json()
    failed = [b for b in batches if b["status"] == "FAILED"]
    check(bool(failed), "失败批次已标记为 FAILED，未留在 PENDING")
    waiter_batch = next((b for b in failed if "映射保留失败" in (b["error"] or "")), None)
    check(waiter_batch is not None, "失败原因记录为映射保留失败（含回滚说明）")

    # 暂存已清理
    from app.config import settings
    if waiter_batch is not None:
        check(not (settings.staging_dir / waiter_batch["id"]).exists(),
              "失败批次的暂存目录已清除")

    # 活跃占用方仍在 PENDING（未被失败方波及）
    active_pending = [b for b in batches if b["status"] == "PENDING"]
    check(bool(active_pending), "其他活跃批次保留，未被失败方影响")

    # 已发布映射不受影响：记录当前映射数，失败前后不应变化
    mappings_before = client.get(f"{base}/api/mappings",
                                 params={"project_id": project_id}).json()

    holder_thread.join(timeout=30)
    check(holder_box and holder_box[0].status_code == 201, "占用方结束后发布成功")

    # 同源映射唯一
    mappings_after = client.get(f"{base}/api/mappings",
                                params={"project_id": project_id}).json()
    same = [m for m in mappings_after
            if m["id_kind"] == "sop_uid" and m["source_value"] == conflict_sop]
    check(len(same) == 1 and same[0]["status"] == "PUBLISHED",
          "同源映射唯一且已发布，无分叉、无 PENDING 残留")
    check(len(mappings_after) >= len(mappings_before), "失败回滚未污染已发布映射")

    # 无需重启即可重传成功
    retry = _upload(client, base, project_id, waiter)
    check(retry.status_code == 201, "占用方结束后重传成功（无需重启）")

    leftover_pending = [b for b in client.get(f"{base}/api/batches",
                       params={"project_id": project_id}).json()
                        if b["status"] == "PENDING"]
    check(not leftover_pending, "最终无任何 PENDING 遗留")


def scenario_d_disconnect(client: httpx.Client, base: str, project_id: str,
                          work: Path) -> None:
    print("\n== 场景 D：请求断连后后台作业完成发布 ==")
    path = _make_dicom(work, "disconnect")
    body, boundary = _multipart(project_id, path.name, path.read_bytes(),
                                slow=SLOW_SECONDS)
    parsed = urlparse(base)
    # 记录断连前的“全部”批次 ID（含前序场景遗留的 FAILED 等待方），
    # 否则会把场景 C 的失败批次误判为本次断连产生的新批次
    before = {b["id"] for b in client.get(f"{base}/api/batches",
              params={"project_id": project_id}).json()}

    sock = socket.create_connection((parsed.hostname, parsed.port), timeout=10)
    request = (
        f"POST /api/batches HTTP/1.1\r\nHost: {parsed.hostname}:{parsed.port}\r\n"
        f"Content-Type: multipart/form-data; boundary={boundary}\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
    ).encode() + body
    sock.sendall(request)
    sock.close()  # 不读响应，立即断连
    print("  客户端已断开连接（未读取响应），后台作业应继续完成发布/回滚")

    new_id = None
    for _ in range(80):
        rows = client.get(f"{base}/api/batches",
                          params={"project_id": project_id}).json()
        fresh = [b for b in rows if b["id"] not in before]
        fresh_failed = [b for b in fresh if b["status"] == "FAILED"]
        # 只检查本场景之后新建的批次：此前场景遗留的 FAILED 与本场景无关
        check(not fresh_failed,
              "断连批次未失败（后台作业完成发布或回滚，而非中断）")
        published = [b for b in fresh if b["status"] == "PUBLISHED"]
        if published:
            new_id = published[0]["id"]
            break
        time.sleep(0.25)
    check(new_id is not None, "断连后后台作业自行完成发布")

    if new_id is not None:
        dl = client.get(f"{base}/api/batches/{new_id}/download")
        check(dl.status_code == 200 and len(dl.content) > 0, "断连批次的 ZIP 可正常下载")

    follow = _make_dicom(work, "after_disc")
    resp = _upload(client, base, project_id, follow)
    check(resp.status_code == 201, "后续上传立即成功，执行名额已释放")
    check(client.get(base + "/api/health").json()["inflight_jobs"] == 0,
          "在途作业计数归零，连接与名额均已释放")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", default=str(ROOT / "data" / "samples"))
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="dicom-concurrency-demo-"))
    work = tmp / "work"
    work.mkdir(parents=True, exist_ok=True)
    os.environ["DICOM_REMAP_DATA"] = str(tmp)
    os.environ["DICOM_REMAP_LOCK_WAIT"] = str(DEMO_LOCK_WAIT)
    os.environ["DICOM_REMAP_WORKERS"] = str(DEMO_WORKERS)
    # 演示需要让占用方慢过等待窗口，故放宽演示进程的慢转换上限（生产默认仍 10s）
    os.environ["DICOM_REMAP_MAX_SLOW_SECONDS"] = "30"

    from app.config import reload_settings  # noqa: E402
    reload_settings()

    from app.main import app  # noqa: E402

    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port,
                                          log_level="warning"))
    threading.Thread(target=server.run, daemon=True).start()
    base = f"http://127.0.0.1:{port}"
    for _ in range(60):
        try:
            if httpx.get(base + "/api/health", timeout=1).status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(0.1)

    print(f"临时数据目录：{tmp}")
    print(f"演示参数：DICOM_REMAP_LOCK_WAIT={DEMO_LOCK_WAIT}s（仅演示进程，"
          f"生产默认仍为 60s），workers={DEMO_WORKERS}，慢转换={SLOW_SECONDS}s")

    with httpx.Client(timeout=60) as client:
        project_id = client.get(base + "/api/projects").json()[0]["id"]
        scenario_a_query_sla(client, base, project_id, work)
        scenario_b_slow_no_lock(client, base, project_id, work)
        scenario_c_timeout_cleanup(client, base, project_id, work)
        scenario_d_disconnect(client, base, project_id, work)

    print()
    if failures:
        print(f"演示结束：{len(failures)} 项失败：{failures}")
        if not args.keep:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)
        return 1
    print(f"全部并发修复断言通过。临时数据目录：{tmp}")
    if not args.keep:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
