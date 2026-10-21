"""Kill an isolated ext4 guest at each journal phase, then boot the same disk.

This models guest kernel/page-cache loss, not physical host/device power loss.
QEMU writeback honors guest flushes; unsafe caching and -snapshot are forbidden.
"""
import pathlib
import selectors
import subprocess
import tempfile
import time

PHASES = (
    "journal_synced", "journal_renamed", "journal_directory_synced",
    "journal_manifest_written", "journal_manifest_synced", "journal_manifest_directory_synced",
)


def boot(disk, phase, stage, output):
    command = ["qemu-system-x86_64", "-accel", "tcg", "-m", "768", "-smp", "2",
               "-nographic", "-no-reboot", "-kernel", "/kernel", "-initrd", "/initramfs.gz",
               "-append", f"console=ttyS0 panic=-1 cursus_stage={stage} cursus_phase={phase}",
               "-drive", f"file={disk},format=raw,if=virtio,cache=writeback"]
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as guest:
        selector = selectors.DefaultSelector()
        selector.register(guest.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + 240
        transcript = bytearray()
        try:
            while time.monotonic() < deadline:
                for key, _ in selector.select(timeout=1):
                    chunk = key.fileobj.read1(65536)
                    if chunk:
                        transcript.extend(chunk)
                    elif guest.poll() is not None:
                        raise RuntimeError(f"guest exited early: {phase} {stage}")
                marker = f"POWER_CUT_READY {phase}".encode() if stage == "seed" else b"POWER_TEST_EXIT=0"
                if marker in transcript:
                    guest.kill()  # no guest shutdown, filesystem sync, or unmount
                    guest.wait(timeout=10)
                    return
                if b"--- FAIL:" in transcript or b"POWER_TEST_FAILED" in transcript:
                    raise RuntimeError(f"guest assertion failed: {phase} {stage}")
            raise TimeoutError(f"guest timeout: {phase} {stage}")
        finally:
            selector.close()
            if guest.poll() is None:
                guest.kill()
                guest.wait(timeout=10)
            output.write_bytes(transcript)


def main():
    output = pathlib.Path("/results")
    output.mkdir(exist_ok=True)
    subprocess.run(["qemu-system-x86_64", "--version"], check=True)
    subprocess.run(["dpkg-query", "-W", "linux-image-amd64", "e2fsprogs"], check=True)
    for phase in PHASES:
        with tempfile.TemporaryDirectory(prefix="cursus-power-") as directory:
            disk = pathlib.Path(directory) / "data.img"
            with disk.open("wb") as image:
                # Keep the broker's normal 256 MiB free-space safety floor.
                image.truncate(1024 * 1024 * 1024)
            subprocess.run(["mkfs.ext4", "-F", "-q", str(disk)], check=True)
            boot(disk, phase, "seed", output / f"{phase}-seed.log")
            boot(disk, phase, "verify", output / f"{phase}-verify.log")
            print(f"PASS guest power-cut phase={phase} filesystem=ext4,data=ordered cache=writeback", flush=True)


if __name__ == "__main__":
    main()
