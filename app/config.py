"""运行期配置。所有路径与限额均可用环境变量覆盖，便于容器化与测试。"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    data_dir: Path = Path(os.environ.get("DICOM_REMAP_DATA", "/data"))
    max_files: int = _int_env("DICOM_REMAP_MAX_FILES", 20)
    # 16 MiB
    max_total_bytes: int = _int_env("DICOM_REMAP_MAX_TOTAL_BYTES", 16 * 1024 * 1024)
    # 并发批次等待对端提交/放弃的最长轮询时间（秒）
    lock_wait_seconds: int = _int_env("DICOM_REMAP_LOCK_WAIT", 60)
    # 批次流水线专用执行器的工作线程数。刻意与 anyio 共享线程池隔离：
    # 上传的长等待只占用本执行器名额，查询接口始终能在共享池内立即被调度。
    workers: int = _int_env("DICOM_REMAP_WORKERS", 8)
    # 演示用“可控慢转换”允许注入的最大延时（秒），请求传入值会被钳制到该上限
    max_slow_seconds: float = _float_env("DICOM_REMAP_MAX_SLOW_SECONDS", 10.0)
    # 进程优雅退出时等待在途批次完成发布/回滚的最长时间（秒）
    shutdown_wait_seconds: float = _float_env("DICOM_REMAP_SHUTDOWN_WAIT", 70.0)
    db_filename: str = "registry.db"

    @property
    def db_path(self) -> Path:
        return self.data_dir / self.db_filename

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def published_dir(self) -> Path:
        return self.data_dir / "published"


settings = Settings()


def reload_settings() -> "Settings":
    """根据当前环境变量刷新全局配置。

    关键：原地修改同一个 Settings 对象（frozen dataclass 借助 object.__setattr__），
    而不是重绑定模块变量——其他模块以 ``from .config import settings`` 持有的引用
    才能立即看到新值。data_dir 相关路径均为 property，会动态反映新目录。

    不能简单 ``Settings()`` 后拷贝字段：dataclass 字段默认值在模块导入时就已
    求值固化，导入之后再改环境变量（测试夹具切换数据目录、缩短锁等待等），
    新建实例仍会拿到旧默认值。因此这里按当前环境变量显式重新计算每个字段。
    """
    object.__setattr__(
        settings,
        "data_dir",
        Path(os.environ.get("DICOM_REMAP_DATA", "/data")),
    )
    object.__setattr__(settings, "max_files", _int_env("DICOM_REMAP_MAX_FILES", 20))
    object.__setattr__(
        settings,
        "max_total_bytes",
        _int_env("DICOM_REMAP_MAX_TOTAL_BYTES", 16 * 1024 * 1024),
    )
    object.__setattr__(settings, "lock_wait_seconds", _int_env("DICOM_REMAP_LOCK_WAIT", 60))
    object.__setattr__(settings, "workers", _int_env("DICOM_REMAP_WORKERS", 8))
    object.__setattr__(
        settings, "max_slow_seconds", _float_env("DICOM_REMAP_MAX_SLOW_SECONDS", 10.0)
    )
    object.__setattr__(
        settings,
        "shutdown_wait_seconds",
        _float_env("DICOM_REMAP_SHUTDOWN_WAIT", 70.0),
    )
    return settings


def ensure_dirs() -> None:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.staging_dir.mkdir(parents=True, exist_ok=True)
    settings.published_dir.mkdir(parents=True, exist_ok=True)
