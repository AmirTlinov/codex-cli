#!/usr/bin/env python3
"""Measure a real stdio code-mode host's residency and sequential tool latency.

Pass binaries built from the base and changed revisions with the same profile.
Uses isolated sessions, local echo tools and long timers; no model or MCP calls.
"""

import argparse
import asyncio
import json
import pathlib
import statistics
import struct
import subprocess
import sys
import time


def sample(pid, stage):
    rss = int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)]))
    if sys.platform == "darwin":
        threads = (
            len(subprocess.check_output(["ps", "-M", "-p", str(pid)]).splitlines()) - 1
        )
    else:
        threads = len(list(pathlib.Path(f"/proc/{pid}/task").iterdir()))
    return {"stage": stage, "os_threads": threads, "rss_kib": rss}


async def measure(args):
    process = await asyncio.create_subprocess_exec(
        str(args.binary.resolve()),
        "--listen",
        "stdio",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stderr = asyncio.create_task(process.stderr.read())
    result = {"binary": str(args.binary.resolve()), "samples": []}
    request_id = 0
    delegates = 0

    async def send(message):
        body = json.dumps(message).encode()
        process.stdin.write(struct.pack("<I", len(body)) + body)
        await process.stdin.drain()

    async def receive():
        nonlocal delegates
        while True:
            prefix = await asyncio.wait_for(process.stdout.readexactly(4), 30)
            (size,) = struct.unpack("<I", prefix)
            assert size < 16 * 1024 * 1024, size
            message = json.loads(await process.stdout.readexactly(size))
            if message["type"] == "delegate/request":
                assert message["request"]["type"] == "tool/invoke", message
                delegates += 1
                await send(
                    {
                        "type": "delegate/response",
                        "id": message["id"],
                        "result": {
                            "status": "ok",
                            "value": {"type": "tool/result", "result": 42},
                        },
                    }
                )
                continue
            if message.get("result", {}).get("status") == "error":
                raise RuntimeError(message)
            return message

    async def request(method, session, **extra):
        nonlocal request_id
        request_id += 1
        await send(
            {
                "type": "operation/request",
                "id": request_id,
                "request": {"method": method, "sessionId": session, **extra},
            }
        )
        expected = {"operation/response"}
        if method == "session/execute":
            expected.add("execute/initialResponse")
        replies = {}
        while expected:
            message = await receive()
            if message.get("id") == request_id and message["type"] in expected:
                expected.remove(message["type"])
                replies[message["type"]] = message["result"]["value"]
        return replies

    async def execute(session, source, tools=(), yield_ms=60_000):
        replies = await request(
            "session/execute",
            session,
            request={
                "tool_call_id": f"benchmark-{request_id}",
                "enabled_tools": list(tools),
                "source": source,
                "yield_time_ms": yield_ms,
                "max_output_tokens": 100,
            },
        )
        response = replies["execute/initialResponse"]
        if "Result" in response:
            assert response["Result"]["error_text"] is None, response
        return response

    try:
        await send(
            {
                "type": "connection/hello",
                "supportedVersions": [1],
                "requiredCapabilities": [],
                "optionalCapabilities": [],
            }
        )
        assert (await receive())["type"] == "connection/ready"
        result["samples"].append(sample(process.pid, "empty_host"))
        await request("session/open", "latency")
        tool = {
            "name": "echo",
            "tool_name": {"name": "echo", "namespace": None},
            "description": "local echo",
            "kind": "function",
            "input_schema": None,
            "output_schema": None,
        }
        elapsed = []
        source = f"for (let i = 0; i < {args.roundtrips}; ++i) {{ if (await tools.echo({{}}) !== 42) throw Error('echo'); }}"
        for i in range(args.repeats + 2):
            start = time.perf_counter()
            response = await execute("latency", source, [tool])
            assert "Result" in response, response
            if i >= 2:
                elapsed.append((time.perf_counter() - start) * 1000)
        result["latency"] = {
            "roundtrips_per_cell": args.roundtrips,
            "repeats": args.repeats,
            "median_ms": statistics.median(elapsed),
            "samples_ms": elapsed,
            "delegate_calls": delegates,
        }
        await request("session/shutdown", "latency")
        sessions = [f"residency-{i}" for i in range(args.sessions or max(args.stages))]
        for session in sessions:
            await request("session/open", session)
        result["samples"].append(sample(process.pid, f"{len(sessions)}_empty_sessions"))
        start = time.perf_counter()
        for i, session in enumerate(sessions[: max(args.stages)], 1):
            response = await execute(
                session,
                "await new Promise(resolve => setTimeout(resolve, 3_600_000));",
                yield_ms=1,
            )
            assert "Yielded" in response, response
            if i in args.stages:
                measurement = sample(process.pid, f"{i}_parked_cells")
                measurement["elapsed_s"] = time.perf_counter() - start
                result["samples"].append(measurement)
                print(json.dumps(measurement), flush=True)
        # Also observe whether an idle host has a periodic work source.
        result["idle_cpu_before"] = subprocess.check_output(
            ["ps", "-o", "time=", "-p", str(process.pid)], text=True
        ).strip()
        await asyncio.sleep(2)
        result["idle_cpu_after"] = subprocess.check_output(
            ["ps", "-o", "time=", "-p", str(process.pid)], text=True
        ).strip()
        start = time.perf_counter()
        for session in sessions:
            await request("session/shutdown", session)
        result["shutdown_s"] = time.perf_counter() - start
        result["samples"].append(sample(process.pid, "all_sessions_closed"))
        result["status"] = "passed"
    except Exception as error:
        result["status"] = "failed"
        result["error"] = repr(error)
    finally:
        process.stdin.close()
        try:
            await asyncio.wait_for(process.wait(), 10)
        except asyncio.TimeoutError:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 3)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        result["exit_code"] = process.returncode
        result["stderr_tail"] = (await stderr).decode(errors="replace")[-2000:]
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return result["status"] == "passed" and result["exit_code"] == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--stages", type=int, nargs="+", default=[1, 8, 32])
    parser.add_argument(
        "--sessions", type=int, help="Open additional empty logical sessions"
    )
    parser.add_argument("--roundtrips", type=int, default=100)
    parser.add_argument("--repeats", type=int, default=20)
    args = parser.parse_args()
    if min(args.stages) < 1 or args.roundtrips < 1 or args.repeats < 1:
        parser.error("stages, roundtrips and repeats must be positive")
    if args.sessions is not None and args.sessions < max(args.stages):
        parser.error("sessions must cover the largest stage")
    sys.exit(0 if asyncio.run(measure(args)) else 1)
