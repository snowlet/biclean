import time
import sys

def progress_bar(iteration, total, length=40, fill="█", start_time=None):
    if start_time is None:
        start_time = time.time()

    percent = (iteration / total) if total else 0
    filled = int(length * percent)
    bar = fill * filled + "-" * (length - filled)

    elapsed = time.time() - start_time
    eta = (elapsed / iteration * (total - iteration)) if iteration else 0
    sys.stdout.write(
        f"\r[{bar}] {percent*100:6.4f}% | "
        f"[{iteration:,}/{total:,}] | "
        f"[elapsed: {format_seconds(elapsed)} / "
        f"ETA: {format_seconds(eta)}]      "
    )
    sys.stdout.flush()
    if iteration >= total:
        sys.stdout.write("\n")

def format_seconds(s):
    s = int(s)
    d, s = divmod(s, 86400) # 1 day = 86400 seconds
    h, s = divmod(s, 3600)  # 1 hour = 3600 seconds
    m, s = divmod(s, 60)    # 1 minute = 60 seconds

    if d:
        return f"{d:d}d {h:02d}h {m:02d}m {s:02d}s"
    if h:
        return f"{h:d}h {m:02d}m {s:02d}s"
    if m:
        return f"{m:d}m {s:02d}s"
    return f"{s:d}s"


def main():
    total = 100
    start_time = time.time()
    for i in range(total + 1):
        progress_bar(i, total, start_time=start_time)
        time.sleep(0.1)  # Simulate work being done

if __name__ == "__main__":
    main()
