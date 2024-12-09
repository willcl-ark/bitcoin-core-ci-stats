
import json
import requests
from datetime import datetime, timedelta
import re
import unittest

MIN_COMMAND_DURAITON_SEC = 1
CIRRUS_API_URL = "https://api.cirrus-ci.com/graphql"
TASK_QUERY = """
    query OwnerRepositoryQuery(
      $platform: String!
      $owner: String!
      $name: String!
    ) {
      ownerRepository(platform: $platform, owner: $owner, name: $name) {
        id
        platform
        owner
        name
        builds(last: 1) {
          edges {
            node {
              id
              status
              branch
              tag
              changeMessageTitle
              buildCreatedTimestamp
              clockDurationInSeconds
              changeIdInRepo

              latestGroupTasks {
                ...TaskInfo
                allOtherRuns {
                  ...TaskInfo
                }
              }
            }
          }
        }
      }
    }

    fragment TaskInfo on Task {
      id
      name
      status
      creationTimestamp
      scheduledTimestamp
      executingTimestamp
      durationInSeconds
      finalStatusTimestamp
      executionInfo {
        labels
      }
      build {
        id
        status
        branch
        changeIdInRepo
        changeMessageTitle
        buildCreatedTimestamp
      }
    }
    """


class Build:
    def __init__(self, api_response):
        self.id: int = api_response["id"]
        self.status: str = api_response["status"]
        self.branch: str = api_response["branch"]
        self.changeIdInRepo: str = api_response["changeIdInRepo"]
        self.changeMessageTitle: str = api_response["changeMessageTitle"]
        self.buildCreatedTimestamp: int = api_response["buildCreatedTimestamp"]

    def to_dict(self):
        return {
            "id": self.id,
            "status": self.status,
            "branch": self.branch,
            "changeIdInRepo": self.changeIdInRepo,
            "changeMessageTitle": self.changeMessageTitle,
            "buildCreatedTimestamp": self.buildCreatedTimestamp,
        }


class Task:
    def __init__(self, api_response):
        self.id: int = api_response["id"]
        self.status: str = api_response["status"]
        self.name: str = api_response["name"]
        self.creationTimestamp: int = api_response["creationTimestamp"]
        self.scheduledTimestamp: int = api_response["scheduledTimestamp"]
        self.executingTimestamp: int = api_response["executingTimestamp"]
        self.duration: int = api_response["durationInSeconds"]
        self.finalStatusTimestamp: int = api_response["finalStatusTimestamp"]
        self.executionInfoLabels: list[str] = api_response["executionInfo"][
            "labels"] if "labels" in api_response["executionInfo"] else []
        self.build: Build = Build(api_response=api_response["build"])
        self.log: str = ""
        self.log_status_code: int = 0
        self.commands: list[Command] = []
        self.runtime_stats = TaskRuntimeStats()

    def to_dict(self):
        return {
            "id": self.id,
            "status": self.status,
            "name": self.name,
            "creationTimestamp": self.creationTimestamp,
            "scheduledTimestamp": self.scheduledTimestamp,
            "executingTimestamp": self.executingTimestamp,
            "duration": self.duration,
            "finalStatusTimestamp": self.finalStatusTimestamp,
            "executionInfoLabels": self.executionInfoLabels,
            "build": self.build.to_dict(),
            "log": self.log,
            "log_status_code": self.log_status_code,
            "commands": [c.to_dict() for c in self.commands],
            "runtime_stats": self.runtime_stats.to_dict()
        }


class Command:
    def __init__(self, cmd, start, line):
        self.cmd = cmd
        self.start = start
        self.line = line
        self.duration = -1
        self.output = list()

    def to_dict(self):
        return {
            "cmd": self.cmd,
            "line": self.line,
            # "start": self.start.strftime("%H:%M:%S"),
            "duration": str(self.duration),
            # "output": self.output,
        }


class TaskRuntimeStats:
    def __init__(self):
        self.ccache_hitrate = ""
        self.docker_build_cached = False
        self.docker_build_duration = -1
        self.ccache_zerostats_duration = -1
        self.configure_duration = -1
        self.build_duration = -1
        self.unit_test_duration = -1
        self.functional_test_duration = -1
        self.depends_duration = -1

    def to_dict(self):
        return {
            "docker_build_cached": self.docker_build_cached,
            "docker_build_duration": self.docker_build_duration,
            "ccache_zerostats_duration": self.ccache_zerostats_duration,
            "configure_duration": self.configure_duration,
            "depends_build_duration": self.depends_duration,
            "build_duration": self.build_duration,
            "ccache_hitrate": self.ccache_hitrate,
            "unit_test_duration": self.unit_test_duration,
            "functional_test_duration": self.functional_test_duration,
        }

    def process_command(self, command):
        if "docker build" in command.cmd:
            self.docker_build_duration = int(command.duration.total_seconds())
            for line in command.output:
                if " CACHED" in line:
                    self.docker_build_cached = True
                    break
        if "ccache --zero-stats" == command.cmd:
            self.ccache_zerostats_duration = int(
                command.duration.total_seconds())
        if "cmake -S " in command.cmd:
            self.configure_duration = int(command.duration.total_seconds())
        if " make " in command.cmd and " -C depends " in command.cmd:
            self.depends_build_duration = int(command.duration.total_seconds())
        if "cmake --build " in command.cmd:
            self.build_duration = int(command.duration.total_seconds())
        if "ccache --show-stats" in command.cmd:
            for line in command.output:
                if "Hits:" in line:
                    match = re.search(r"\((\d+\.\d+%)\)", line)
                    if match:
                        self.ccache_hitrate = match.group(1)
                    break
        if "ctest " in command.cmd:
            self.unit_test_duration = int(command.duration.total_seconds())
        if "test/functional/test_runner.py " in command.cmd:
            self.functional_test_duration = int(
                command.duration.total_seconds())

def fetch_cirrus_ci_task_log(id) -> tuple[int, str]:
    URL = f"https://api.cirrus-ci.com/v1/task/{id}/logs/ci.log"
    response = requests.get(URL)
    return response.status_code, response.text


def fetch_cirrus_ci_tasks(owner="bitcoin", repository="bitcoin") -> list:
    payload = {
        "query": TASK_QUERY,
        "variables": {
            "owner": owner,
            "name": repository,
            "platform": "github",
        }
    }

    response = requests.post(CIRRUS_API_URL, json=payload)
    if response.status_code == 200:
        data = response.json()
        tasks = list()
        builds = data["data"]["ownerRepository"]["builds"]["edges"]
        for build_node in builds:
            build = build_node["node"]
            latestTasks = build["latestGroupTasks"]
            for latestTask in latestTasks:
                tasks.append(Task(latestTask))
                for otherTasks in latestTask["allOtherRuns"]:
                    tasks.append(Task(otherTasks))
        return tasks
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []


def update_task_with_parsed_log(task):
    if task.log_status_code != 200:
        return task

    # Regex to match timestamp and command lines starting with " + "
    command_pattern = re.compile(r'\[(\d{2}:\d{2}:\d{2}\.\d{3})\] \+ ')

    commands = list()
    stats = TaskRuntimeStats()

    current_command = None
    for i, line in enumerate(task.log.split("\n")):
        command_match = command_pattern.search(line)

        if command_match:
            timestamp = datetime.strptime(
                command_match.group(1), "%H:%M:%S.%f")
            command_match
            command = line.split(" + ", 1)[1].strip()
            if current_command is not None:
                duration = timestamp - current_command.start
                if duration < timedelta(0):
                    # Handle potential cases where the time might roll over midnight
                    duration += timedelta(days=1)
                current_command.duration = duration
                stats.process_command(command=current_command)
                # filter out commands than ran shorter than MIN_COMMAND_DURAITON_SEC
                if current_command.duration > timedelta(seconds=MIN_COMMAND_DURAITON_SEC):
                    commands.append(current_command)
                current_command = None
            current_command = Command(command, timestamp, i)
        else:
            if current_command is None:
                # skip the early commands, this is usually just
                # ./ci/test_run_all.sh
                continue
            else:
                current_command.output.append(line)
    task.log = "<cleared>"
    task.commands = commands
    task.runtime_stats = stats
    return task


def update_task_with_log(task: Task) -> Task:
    if task.log_status_code == 0:
        code, text = fetch_cirrus_ci_task_log(task.id)
        task.log_status_code = code
        task.log = text
        print(f"fetched log for task {task.id} ({task.name}): {
              code} - {len(task.log.split("\n"))} log lines")
    return task


def main():

    unittest.main(exit=False)

    tasks = fetch_cirrus_ci_tasks(owner="0xB10C")
    tasks = [update_task_with_log(task) for task in tasks]

    print("parsing logs..")
    tasks = [update_task_with_parsed_log(task) for task in tasks]

    print("writing tasks..")
    with open("tasks.json", "w") as f:
        json.dump([t.to_dict() for t in tasks], f, indent=2)


if __name__ == "__main__":
    main()

# unit tests

class TestTaskRuntimeStats(unittest.TestCase):
    def test_docker_build_duration(self):
        DURATION = 10
        c = Command(cmd="docker build --file /tmp/cirrus-build/ci/test_imagefile --build-arg CI_IMAGE_NAME_TAG=docker.io/ubuntu:24.04 --build-arg FILE_ENV=./ci/test/00_setup_env_mac_cross.sh --label=bitcoin-ci-test --tag=ci_macos_cross", start=0, line=0)
        c.duration = timedelta(seconds=DURATION)
        stats = TaskRuntimeStats()
        stats.process_command(c)
        self.assertEqual(stats.docker_build_duration, DURATION)

    def test_docker_build_cached(self):
        c = Command(cmd="docker build --file /tmp/cirrus-build/ci/test_imagefile --build-arg CI_IMAGE_NAME_TAG=docker.io/ubuntu:24.04 --build-arg FILE_ENV=./ci/test/00_setup_env_mac_cross.sh --label=bitcoin-ci-test --tag=ci_macos_cross", start=0, line=0)
        c.output = [
            "[10:06:36.093] #7 [2/4] COPY ./ci/retry/retry /usr/bin/retry",
            "[10:06:36.093] #7 CACHED",
            "[10:06:36.093]",
            "[10:06:36.093] #8 [3/4] COPY ./ci/test/00_setup_env.sh ././ci/test/00_setup_env_native_nowallet_libbitcoinkernel.sh ./ci/test/01_base_install.sh /ci_container_base/ci/test/",
            "[10:06:36.093] #8 CACHED",
        ]
        c.duration = timedelta(seconds=0)
        stats = TaskRuntimeStats()
        stats.process_command(c)
        self.assertEqual(stats.docker_build_cached, True)

    def test_ccache_zerostats_duration(self):
        DURATION = 4
        c = Command(cmd="ccache --zero-stats", start=0, line=0)
        c.duration = timedelta(seconds=DURATION)
        stats = TaskRuntimeStats()
        stats.process_command(c)
        self.assertEqual(stats.ccache_zerostats_duration, DURATION)

    def test_configure_duration(self):
        DURATION = 5
        c = Command(
            cmd="bash -c 'cmake -S /ci_container_base -DBUILD_BENCH=ON -DBUILD_FUZZ_BINARY=ON", start=0, line=0)
        c.duration = timedelta(seconds=DURATION)
        stats = TaskRuntimeStats()
        stats.process_command(c)
        self.assertEqual(stats.configure_duration, DURATION)

    def test_depends_build_duration(self):
        DURATION = 6
        c = Command(
            cmd="bash -c 'CONFIG_SHELL= make -j10 -C depends HOST=x86_64-apple-darwin  LOG=1'", start=0, line=0)
        c.duration = timedelta(seconds=DURATION)
        stats = TaskRuntimeStats()
        stats.process_command(c)
        self.assertEqual(stats.depends_build_duration, DURATION)

    def test_build_duration(self):
        DURATION = 7
        c = Command(
            cmd="bash -c 'cmake --build . -j10 --target all deploy'", start=0, line=0)
        c.duration = timedelta(seconds=DURATION)
        stats = TaskRuntimeStats()
        stats.process_command(c)
        self.assertEqual(stats.build_duration, DURATION)

    def test_ccache_show_stats(self):
        HITRATE = "77.2%"
        c = Command(
            cmd="bash -c 'ccache --version | head -n 1 && ccache --show-stats'", start=0, line=0)
        c.duration = timedelta(seconds=0)
        c.output = [
            "[10:30:08.073] ccache version 4.7.5",
            "[10:30:13.421] Cacheable calls:    707 / 707 (100.0%)",
            f"[10:30:13.421]   Hits:             fake / 707 ({HITRATE})",
            "[10:30:13.421]     Direct:         305 / 707 (43.14%)",
            "[10:30:13.421]     Preprocessed:   402 / 707 (56.86%)",
            "[10:30:13.421]   Misses:             0 / 707 ( 0.00%)",
            "[10:30:13.421] Local storage:",
            "[10:30:13.421]   Cache size (GB): 0.00",
        ]
        stats = TaskRuntimeStats()
        stats.process_command(c)
        self.assertEqual(stats.ccache_hitrate, HITRATE)
