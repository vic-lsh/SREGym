import subprocess
import sys
import threading
from time import sleep

server_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"


def automatic_submit():
    # Ensure exp_env directory exists
    import os
    os.makedirs("exp_env", exist_ok=True)

    ctr = 0
    while ctr < 10000:
        subprocess.run(
            [
                "curl",
                "-X",
                "POST",
                "http://localhost:8000/submit",
                "-H",
                "Content-Type: application/json",
                "-d",
                '{"solution":"yes"}',
            ],
            capture_output=True,
            text=True,
            cwd="exp_env",
        )
        sleep(60)
        ctr += 1


if __name__ == "__main__":
    automatic_submit()
