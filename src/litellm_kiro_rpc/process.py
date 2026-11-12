"""Kill the owned process tree, including descendants after the parent exits."""

import asyncio
import os
import signal


class ProcessTree:
    def __init__(self, pid):
        self.pid = pid
        self.handle = None
        if os.name == "nt":
            self._attach_windows(pid)

    def _attach_windows(self, pid):
        import ctypes
        from ctypes import wintypes as w

        class BASIC(ctypes.Structure):
            _fields_ = [
                ("per_process", ctypes.c_int64),
                ("per_job", ctypes.c_int64),
                ("flags", w.DWORD),
                ("min_working", ctypes.c_size_t),
                ("max_working", ctypes.c_size_t),
                ("active", w.DWORD),
                ("affinity", ctypes.c_size_t),
                ("priority", w.DWORD),
                ("scheduling", w.DWORD),
            ]

        class IO(ctypes.Structure):
            _fields_ = [
                (name, ctypes.c_uint64) for name in ("ro", "wo", "oo", "rt", "wt", "ot")
            ]

        class EXTENDED(ctypes.Structure):
            _fields_ = [
                ("basic", BASIC),
                ("io", IO),
                ("process_memory", ctypes.c_size_t),
                ("job_memory", ctypes.c_size_t),
                ("peak_process", ctypes.c_size_t),
                ("peak_job", ctypes.c_size_t),
            ]

        api = ctypes.WinDLL("kernel32", use_last_error=True)
        api.CreateJobObjectW.argtypes = [ctypes.c_void_p, w.LPCWSTR]
        api.CreateJobObjectW.restype = w.HANDLE
        api.SetInformationJobObject.argtypes = [
            w.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            w.DWORD,
        ]
        api.SetInformationJobObject.restype = w.BOOL
        api.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
        api.OpenProcess.restype = w.HANDLE
        api.AssignProcessToJobObject.argtypes = [w.HANDLE, w.HANDLE]
        api.AssignProcessToJobObject.restype = w.BOOL
        api.CloseHandle.argtypes = [w.HANDLE]
        api.CloseHandle.restype = w.BOOL
        api.TerminateJobObject.argtypes = [w.HANDLE, w.UINT]
        api.TerminateJobObject.restype = w.BOOL
        api.QueryInformationJobObject.argtypes = [
            w.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            w.DWORD,
            ctypes.c_void_p,
        ]
        api.QueryInformationJobObject.restype = w.BOOL
        job = api.CreateJobObjectW(None, None)
        if not job:
            raise OSError("Cannot create process job")
        limits = EXTENDED()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        process = None
        try:
            if not api.SetInformationJobObject(
                job, 9, ctypes.byref(limits), ctypes.sizeof(limits)
            ):
                raise OSError("Cannot configure process job")
            process = api.OpenProcess(0x0100 | 0x0001, False, pid)
            if not process or not api.AssignProcessToJobObject(job, process):
                raise OSError("Cannot contain process")
            self.handle, self.api = job, api
        except BaseException:
            api.CloseHandle(job)
            raise
        finally:
            if process:
                api.CloseHandle(process)

    def kill(self):
        if os.name == "nt":
            if self.handle:
                self.api.TerminateJobObject(self.handle, 1)
        else:
            try:
                os.killpg(self.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def resume(self):
        """Resume only owned threads after assigning the suspended process to a job."""
        if os.name != "nt":
            return
        import ctypes
        from ctypes import wintypes as w

        class THREADENTRY(ctypes.Structure):
            _fields_ = [
                ("size", w.DWORD),
                ("usage", w.DWORD),
                ("thread_id", w.DWORD),
                ("owner_pid", w.DWORD),
                ("base_priority", w.LONG),
                ("delta_priority", w.LONG),
                ("flags", w.DWORD),
            ]

        api = self.api
        api.CreateToolhelp32Snapshot.argtypes = [w.DWORD, w.DWORD]
        api.CreateToolhelp32Snapshot.restype = w.HANDLE
        for name in ("Thread32First", "Thread32Next"):
            fn = getattr(api, name)
            fn.argtypes = [w.HANDLE, ctypes.POINTER(THREADENTRY)]
            fn.restype = w.BOOL
        api.OpenThread.argtypes = [w.DWORD, w.BOOL, w.DWORD]
        api.OpenThread.restype = w.HANDLE
        api.ResumeThread.argtypes = [w.HANDLE]
        api.ResumeThread.restype = w.DWORD
        snapshot = api.CreateToolhelp32Snapshot(4, 0)
        if snapshot == ctypes.c_void_p(-1).value:
            raise OSError("Cannot enumerate owned process threads")
        resumed = False
        try:
            entry = THREADENTRY()
            entry.size = ctypes.sizeof(entry)
            more = api.Thread32First(snapshot, ctypes.byref(entry))
            while more:
                if entry.owner_pid == self.pid:
                    thread = api.OpenThread(2, False, entry.thread_id)
                    if not thread:
                        raise OSError("Cannot open owned process thread")
                    try:
                        if api.ResumeThread(thread) == 0xFFFFFFFF:
                            raise OSError("Cannot resume owned process thread")
                        resumed = True
                    finally:
                        api.CloseHandle(thread)
                more = api.Thread32Next(snapshot, ctypes.byref(entry))
            if not resumed:
                raise OSError("No suspended thread found")
        finally:
            api.CloseHandle(snapshot)

    async def wait(self, timeout):
        if os.name != "nt" or not self.handle:
            return
        import ctypes
        from ctypes import wintypes as w

        class ACCOUNTING(ctypes.Structure):
            _fields_ = [
                (name, ctypes.c_int64)
                for name in ("user", "kernel", "period_user", "period_kernel")
            ] + [
                (name, w.DWORD) for name in ("faults", "total", "active", "terminated")
            ]

        try:
            async with asyncio.timeout(timeout):
                while True:
                    info = ACCOUNTING()
                    if not self.api.QueryInformationJobObject(
                        self.handle, 1, ctypes.byref(info), ctypes.sizeof(info), None
                    ):
                        raise OSError("Cannot verify process cleanup")
                    if info.active == 0:
                        return
                    await asyncio.sleep(0.01)
        finally:
            self.api.CloseHandle(self.handle)
            self.handle = None


async def stop_process(proc, tree, timeout):
    if proc.stdin:
        proc.stdin.close()
    try:
        await asyncio.wait_for(proc.wait(), timeout)
    except TimeoutError:
        pass
    finally:
        if tree:
            tree.kill()
        elif proc.returncode is None:
            proc.kill()
    await asyncio.wait_for(proc.wait(), timeout)
    if tree:
        await tree.wait(timeout)
