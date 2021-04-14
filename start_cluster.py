import atexit
import subprocess

jobs = [
    ["cabal", "run", "haskv-exe", "--", "./config.txt", "100"],
    ["cabal", "run", "haskv-exe", "--", "./config.txt", "101"],
    ["cabal", "run", "haskv-exe", "--", "./config.txt", "102"],
    ["cabal", "run", "haskv-exe", "--", "./config.txt", "103"],
]

pids = []

def exit_handler():
    for pid in pids:
        pid.terminate()

atexit.register(exit_handler)

for job in jobs:
    pids.append(subprocess.Popen(job))

while True:
    pass
