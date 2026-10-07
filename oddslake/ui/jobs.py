"""Ağır analiz işlerinin tek slotlu çalıştırıcısı ve bellek koruyucusu.

Aynı anda en fazla bir ağır iş çalışır (QThreadPool, maxThreadCount=1). İş, iki
argüman alan bir fonksiyondur: cancel_fn() -> bool ve progress_fn(int). İptal
istendiğinde cancel_fn True döner; iş bunu görünce InterruptedError fırlatmalıdır.
InterruptedError, failed("iptal") olarak bildirilir.

Bellek koruyucusu her 500 ms'de arayüz süreci dahil toplam RSS değerini ölçer.
hard_limit_mb aşılırsa işi iptal eder, memory_warning ve failed("bellek siniri
asildi: X MB") sinyallerini gönderir. Bu durumda işin iç iş parçacığı ancak
kendi kontrol noktasında durabildiği için slot, iş gerçekten bitene kadar dolu kalır.

Sinyaller işçi iş parçacığından yayılır; Qt bağlantıları ana iş parçacığında kurulduğu
için alıcı tarafı kuyruklanmış (queued) olarak çalışır.
"""

from __future__ import annotations

import threading
from collections.abc import Callable

import psutil
from PySide6.QtCore import QObject, QRunnable, QThreadPool, QTimer, Signal

from ..config import MemoryBudget

JobFn = Callable[[Callable[[], bool], Callable[[int], None]], object]

CANCELLED_MESSAGE = "iptal"
MEMORY_MESSAGE_PREFIX = "bellek siniri asildi"
_MB = 1024 * 1024


class HeavyJobRunner(QObject):
    """Tek ağır iş slotu. submit() ile iş verilir, cancel() ile iptal edilir.

    Yaşam döngüsü: çalışan bir iş varken bu nesne yok edilmemelidir. İşçi görevi nesneye
    referans tutar; son referans bir işçi iş parçacığında bırakılırsa QObject ve havuz
    orada yok edilir. Sahip, nesneyi bırakmadan önce wait_idle() çağırmalıdır
    (MainWindow.closeEvent bunu yapar).

    Sinyaller:
      progress(int)     işin bildirdiği ilerleme
      finished(object)  iş normal biterse dönen değer
      failed(str)       iptal ("iptal"), istisna mesajı veya bellek sınırı mesajı
      memory_warning(int) bellek sınırı aşıldığında ölçülen RSS (MB)
    """

    progress = Signal(int)
    finished = Signal(object)
    failed = Signal(str)
    memory_warning = Signal(int)

    def __init__(self, budget: MemoryBudget, parent: QObject | None = None):
        super().__init__(parent)
        self._budget = budget
        self._pool = QThreadPool()
        self._pool.setMaxThreadCount(1)
        self._process = psutil.Process()
        self._lock = threading.Lock()
        self._job_name: str | None = None
        self._cancel_event = threading.Event()
        self._abort_reason: str | None = None
        # Çalışan görevin Python referansı; iş bitene kadar yaşamalıdır.
        self._active_task: _HeavyTask | None = None
        self._guard = QTimer(self)
        self._guard.setInterval(500)
        self._guard.timeout.connect(self._check_memory)
        self._guard.start()

    @property
    def is_busy(self) -> bool:
        with self._lock:
            return self._job_name is not None

    @property
    def current_job(self) -> str | None:
        with self._lock:
            return self._job_name

    def submit(self, name: str, fn: JobFn) -> bool:
        """İşi slota koyar. Slot doluysa False döner ve iş başlatılmaz."""
        with self._lock:
            if self._job_name is not None:
                return False
            self._job_name = name
            self._cancel_event.clear()
            self._abort_reason = None
            task = _HeavyTask(self, fn)
            self._active_task = task
        self._pool.start(task)
        return True

    def cancel(self) -> None:
        """İptal bayrağını kaldırır. Çalışan iş cancel_fn ile bunu görür; boşta ise etkisizdir."""
        with self._lock:
            if self._job_name is not None:
                self._cancel_event.set()

    def wait_idle(self, timeout_ms: int = -1) -> bool:
        """İşçi havuzu boşalana kadar bekler. Zaman aşımında False döner."""
        return self._pool.waitForDone(timeout_ms)

    # ---- işçi iş parçacığı --------------------------------------------------------

    def _is_cancelled(self) -> bool:
        return self._cancel_event.is_set()

    def _report_progress(self, value: int) -> None:
        self.progress.emit(int(value))

    def _run(self, fn: JobFn) -> None:
        """İşçi iş parçacığında çalışır. Her istisna sınıfı ve mesajıyla bildirilir."""
        try:
            value = fn(self._is_cancelled, self._report_progress)
        except InterruptedError:
            self._conclude(False, CANCELLED_MESSAGE)
        except BaseException as exc:
            self._conclude(False, f"{type(exc).__name__}: {exc}")
        else:
            self._conclude(True, value)

    def _conclude(self, ok: bool, payload: object) -> None:
        """İşi kapatır ve slotu serbest bırakır. Bellek koruyucusu zaten bildirdiyse sessiz kalır."""
        with self._lock:
            abort = self._abort_reason
            cancelled = self._cancel_event.is_set()
            self._job_name = None
            self._active_task = None
        if abort is not None:
            return
        if not ok:
            self.failed.emit(str(payload))
        elif cancelled:
            self.failed.emit(CANCELLED_MESSAGE)
        else:
            self.finished.emit(payload)

    # ---- ana iş parçacığı: bellek koruyucusu ---------------------------------------

    def _check_memory(self) -> None:
        with self._lock:
            if self._job_name is None or self._abort_reason is not None:
                return
        rss_mb = self._process.memory_info().rss // _MB
        if rss_mb <= self._budget.hard_limit_mb:
            return
        reason = f"{MEMORY_MESSAGE_PREFIX}: {rss_mb} MB"
        with self._lock:
            if self._job_name is None or self._abort_reason is not None:
                return
            self._abort_reason = reason
            self._cancel_event.set()
        self.memory_warning.emit(rss_mb)
        self.failed.emit(reason)


class _HeavyTask(QRunnable):
    """HeavyJobRunner'ın tek işçi görevi."""

    def __init__(self, runner: HeavyJobRunner, fn: JobFn):
        super().__init__()
        self.setAutoDelete(True)
        self._runner = runner
        self._fn = fn

    def run(self) -> None:
        self._runner._run(self._fn)
