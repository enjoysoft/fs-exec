"""Windows source handles for fs-rally (imported only on Windows).

Hold every ancestor without FILE_SHARE_DELETE while the leaf is open, and deny
writers on the leaf. OPEN_REPARSE_POINT means inspection never follows a link.
Destinations and the private journal still require protected structural ACLs.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path


@contextmanager
def open_locked(path: Path):
    import ctypes
    import msvcrt
    import os
    from ctypes import wintypes

    class FileInformation(ctypes.Structure):
        _fields_ = [
            ("attributes", wintypes.DWORD),
            ("created", wintypes.FILETIME),
            ("accessed", wintypes.FILETIME),
            ("written", wintypes.FILETIME),
            ("volume", wintypes.DWORD),
            ("size_high", wintypes.DWORD),
            ("size_low", wintypes.DWORD),
            ("links", wintypes.DWORD),
            ("index_high", wintypes.DWORD),
            ("index_low", wintypes.DWORD),
        ]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.GetFileInformationByHandle.argtypes = [wintypes.HANDLE, ctypes.POINTER(FileInformation)]
    kernel.GetFileInformationByHandle.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    parents = []
    leaf = None
    try:
        # Opening top-down while holding ancestors prevents renames or junction
        # replacement between the attribute check and the final leaf open.
        for current in (*reversed(path.absolute().parents), path.absolute()):
            is_leaf = current == path.absolute()
            handle = kernel.CreateFileW(str(current), 0x80000000 if is_leaf else 0x80,
                                        1 if is_leaf else 3, None, 3, 0x02200000, None)
            if handle == ctypes.c_void_p(-1).value:
                raise ctypes.WinError(ctypes.get_last_error())
            if is_leaf:
                leaf = handle
            else:
                parents.append(handle)
            info = FileInformation()
            if not kernel.GetFileInformationByHandle(handle, ctypes.byref(info)):
                raise ctypes.WinError(ctypes.get_last_error())
            if info.attributes & 0x400 or (is_leaf and info.attributes & (0x200 | 0x10)):
                raise ValueError("reparse/sparse/non-regular Windows source")
            if not is_leaf and not info.attributes & 0x10:
                raise ValueError("non-directory Windows ancestor")
        fd = msvcrt.open_osfhandle(leaf, os.O_RDONLY | os.O_BINARY)
        leaf = None  # fd now owns the handle.
        with os.fdopen(fd, "rb") as stream:
            yield stream
    finally:
        if leaf is not None:
            kernel.CloseHandle(leaf)
        for handle in reversed(parents):
            kernel.CloseHandle(handle)
