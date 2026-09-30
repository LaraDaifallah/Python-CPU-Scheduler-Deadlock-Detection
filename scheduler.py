"""CPU scheduling simulation with priority queues, aging and deadlock recovery."""
import re
import sys
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class Process:
    pid: int
    arrival: int
    base_prio: int
    cmds: List[Tuple]
    cur_prio: int = field(init=False)
    cmd_i: int = 0
    exec_prog: int = 0
    state: str = "NEW"
    io_end: Optional[int] = None
    held: Dict[int, int] = field(default_factory=lambda: defaultdict(int))
    wait_ready: int = 0
    finish: Optional[int] = None
    q_left: int = 0
    ready_timer: int = 0

    def __post_init__(self):
        self.cur_prio = self.base_prio

    def cur_cmd(self) -> Optional[Tuple]:
        if self.cmd_i >= len(self.cmds):
            return None
        return self.cmds[self.cmd_i]


def _split_top_level_commas(s: str) -> List[str]:
    out, cur, depth = [], "", 0
    for ch in s:
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
        if ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def parse_input_text(text: str) -> Tuple[Dict[int, int], List[Process]]:
    lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
    if not lines:
        raise ValueError("Empty input")
    res_matches = re.findall(r"\[(\d+)\s*,\s*(\d+)\]", lines[0])
    if not res_matches:
        raise ValueError("Bad resources line")
    total_res = {int(r): int(c) for r, c in res_matches}
    proc_re = re.compile(r"^\s*(\d+)\s+(\d+)\s+(\d+)\s+(.+)$")
    procs: List[Process] = []
    for ln in lines[1:]:
        m = proc_re.match(ln)
        if not m:
            raise ValueError(f"Bad process line: {ln}")
        pid, arr, pr = int(m.group(1)), int(m.group(2)), int(m.group(3))
        bursts = re.findall(r"(CPU|IO)\s*\{([^}]*)\}", m.group(4), flags=re.I)
        if not bursts:
            raise ValueError(f"No bursts found: {ln}")
        cmds: List[Tuple] = []
        for btype_raw, content in bursts:
            content = content.strip()
            if btype_raw.upper().strip() == "IO":
                cmds.append(("IO", int(content)))
                continue
            for part in _split_top_level_commas(content):
                part = part.strip()
                rm = re.match(r"R\[(\d+)\s*,\s*(\d+)\]", part, flags=re.I)
                fm = re.match(r"F\[(\d+)\s*,\s*(\d+)\]", part, flags=re.I)
                if rm:
                    cmds.append(("REQ", int(rm.group(1)), int(rm.group(2))))
                elif fm:
                    cmds.append(("REL", int(fm.group(1)), int(fm.group(2))))
                else:
                    cmds.append(("EXEC", int(part)))
        procs.append(Process(pid=pid, arrival=arr, base_prio=pr, cmds=cmds))
    procs.sort(key=lambda p: (p.arrival, p.pid))
    return total_res, procs


class Scheduler:
    """Simulate one CPU; smaller numeric priorities run first."""

    def __init__(self, total_res: Dict[int, int], procs: List[Process], quantum: int = 8):
        self.total = dict(total_res)
        self.avail = dict(total_res)
        self.procs = procs
        self.quantum = quantum
        self.ready: List[deque] = [deque() for _ in range(21)]
        self.io_list: List[Process] = []
        self.waiting: deque = deque()
        self.pending_req: Dict[int, Tuple[int, int]] = {}
        self.t = 0
        self.running: Optional[Process] = None
        self.gantt_ticks: List[str] = []
        self.deadlock_events: List[str] = []
        self.deadlock_recovery: List[str] = []

    def log(self, msg: str):
        print(f"[T={self.t:03d}] {msg}")

    def _best_ready_prio(self) -> Optional[int]:
        for pr in range(21):
            if self.ready[pr]:
                return pr
        return None

    def _rr_active_for_priority(self, pr: int) -> bool:
        return len(self.ready[pr]) > 0

    def _rr_active_for_running(self) -> bool:
        return self.running is not None and self._rr_active_for_priority(self.running.cur_prio)

    def _enqueue_ready(self, p: Process, front: bool = False, reason: str = ""):
        rr_was_active = False
        if self.running and self.running.cur_prio == p.cur_prio:
            rr_was_active = self._rr_active_for_running()
        p.state = "READY"
        dq = self.ready[p.cur_prio]
        if front:
            dq.appendleft(p)
        else:
            dq.append(p)
        suffix = f" [{reason}]" if reason else ""
        self.log(f"P{p.pid} entered READY (prio={p.cur_prio}){suffix}")
        if self.running and self.running.cur_prio == p.cur_prio and self.running is not p:
            if self._rr_active_for_running() and not rr_was_active:
                self.running.q_left = self.quantum
                self.log(f"RR START: Quantum set for RUNNING P{self.running.pid} = {self.quantum}")

    def _dispatch(self):
        if self.running is not None:
            return
        best = self._best_ready_prio()
        if best is None:
            return
        p = self.ready[best].popleft()
        p.state = "RUNNING"
        p.ready_timer = 0
        p.q_left = self.quantum
        self.running = p
        self.log(f"P{p.pid} scheduled (prio={p.cur_prio})")

    def _yield_running_to_ready(self, reason: str = ""):
        if not self.running:
            return
        cur = self.running
        self.running = None
        self._enqueue_ready(cur, front=False, reason=reason)

    def _remove_from_all_queues(self, p: Process):
        for queue in [*self.ready, self.io_list, self.waiting]:
            try:
                queue.remove(p)
            except ValueError:
                pass
        self.pending_req.pop(p.pid, None)
        if self.running is p:
            self.running = None

    def _terminate_process(self, p: Process, reason: str):
        self.log(f"P{p.pid} terminating ({reason})")
        released_pairs = []
        for rid, amt in list(p.held.items()):
            if amt > 0:
                self.avail[rid] = self.avail.get(rid, 0) + amt
                released_pairs.append(f"R{rid}x{amt}")
            p.held[rid] = 0
        rel_str = ", ".join(released_pairs) if released_pairs else "none"
        if released_pairs:
            self.log(f"P{p.pid} released resources: {rel_str}")
        else:
            self.log(f"P{p.pid} released no resources")
        self._remove_from_all_queues(p)
        p.state = "TERM"
        p.finish = self.t
        if "deadlock" in reason:
            self.deadlock_recovery.append(
                f"t={self.t}: Terminated P{p.pid} ({reason}), released: {rel_str}"
            )

    def _active_processes(self) -> List[Process]:
        return [p for p in self.procs if p.state not in ("NEW", "TERM")]

    def _detect_deadlock(self) -> List[Process]:
        active = self._active_processes()
        if not active:
            return []
        waiting_pids = {p.pid for p in self.waiting}
        if not waiting_pids:
            return []
        rids = sorted(self.total.keys())
        work = {rid: self.avail.get(rid, 0) for rid in rids}
        finish = {p.pid: (p.pid not in waiting_pids) for p in active}
        for p in active:
            if finish[p.pid]:
                for rid in rids:
                    work[rid] += p.held.get(rid, 0)
        changed = True
        while changed:
            changed = False
            for p in active:
                if finish[p.pid]:
                    continue
                cmd = p.cur_cmd()
                if not cmd or cmd[0] != "REQ":
                    finish[p.pid] = True
                    for rid in rids:
                        work[rid] += p.held.get(rid, 0)
                    changed = True
                    continue
                rid, amt = cmd[1], cmd[2]
                if work.get(rid, 0) >= amt:
                    finish[p.pid] = True
                    for rr in rids:
                        work[rr] += p.held.get(rr, 0)
                    changed = True
        return [p for p in active if not finish[p.pid]]

    def _deadlock_check_and_recover(self, trigger: str):
        dead = self._detect_deadlock()
        if not dead:
            return
        dead_pids = sorted(p.pid for p in dead)
        snapshot = (
            f"t={self.t}: DEADLOCK detected (trigger={trigger}) among: "
            + ", ".join("P" + str(x) for x in dead_pids)
        )
        self.deadlock_events.append(snapshot)
        self.log("DEADLOCK DETECTED: " + snapshot)
        while True:
            dead = self._detect_deadlock()
            if not dead:
                return
            victim = sorted(dead, key=lambda p: (p.cur_prio, p.pid), reverse=True)[0]
            self._terminate_process(victim, reason="deadlock_victim_lowest priority_highest PID")
            self._resource_satisfaction(tag="after_recovery")

    def _handle_arrivals(self):
        for p in self.procs:
            if p.state == "NEW" and p.arrival == self.t:
                self.log(f"P{p.pid} arrived.")
                p.ready_timer = 0
                self._enqueue_ready(p, reason="arrival")

    def _handle_io_completions(self):
        for p in list(self.io_list):
            if p.io_end is not None and self.t >= p.io_end:
                self.io_list.remove(p)
                p.io_end = None
                p.cmd_i += 1
                p.exec_prog = 0
                p.cur_prio = p.base_prio
                p.ready_timer = 0
                self.log(f"P{p.pid} returned from IO (prio reset to {p.cur_prio}).")
                self._enqueue_ready(p, reason="io_done")

    def _resource_satisfaction(self, tag: str):
        for p in list(self.waiting):
            rid, amt = self.pending_req[p.pid]
            if self.avail.get(rid, 0) >= amt:
                self.avail[rid] -= amt
                p.held[rid] += amt
                p.cmd_i += 1
                self.pending_req.pop(p.pid, None)
                try:
                    self.waiting.remove(p)
                except ValueError:
                    pass
                p.ready_timer = 0
                self.log(f"P{p.pid} unblocked, acquired R{rid} [{tag}]")
                self._enqueue_ready(p, reason=f"unblock:{tag}")

    def _apply_aging_at_boundary(self):
        for pr in range(1, 21):
            for p in list(self.ready[pr]):
                if p.ready_timer >= 10 and p.cur_prio > 0:
                    old_prio = p.cur_prio
                    rr_was_active_after_promotion = False
                    if self.running and self.running.cur_prio == old_prio - 1:
                        rr_was_active_after_promotion = self._rr_active_for_running()
                    self.ready[pr].remove(p)
                    p.cur_prio -= 1
                    p.ready_timer = 0
                    self.ready[p.cur_prio].append(p)
                    self.log(f"AGING: P{p.pid} priority improved {old_prio} -> {p.cur_prio}")
                    if self.running and self.running.cur_prio == p.cur_prio:
                        if self._rr_active_for_running() and not rr_was_active_after_promotion:
                            self.running.q_left = self.quantum
                            self.log(f"RR START: Quantum set for RUNNING P{self.running.pid} = {self.quantum}")

    def _preempt_if_higher_ready(self):
        if not self.running:
            return
        best = self._best_ready_prio()
        if best is not None and best < self.running.cur_prio:
            self.log(
                f"PREEMPTION: Ready P{self.ready[best][0].pid} (prio={best}) "
                f"preempts RUNNING P{self.running.pid}"
            )
            self._yield_running_to_ready(reason="preempted")

    def _rr_rotate_if_needed(self):
        if not self.running:
            return
        if self._rr_active_for_running() and self.running.q_left <= 0:
            self.log(f"Quantum Expired for P{self.running.pid}")
            self._yield_running_to_ready(reason="quantum_expired")

    def _collapse_zero_time_until_exec_or_block(self) -> bool:
        while self.running:
            p = self.running
            cmd = p.cur_cmd()
            if cmd is None:
                self._terminate_process(p, reason="normal_completion")
                self.running = None
                self._resource_satisfaction(tag="after_normal_termination")
                return False
            if cmd[0] == "IO":
                dur = cmd[1]
                p.state = "IO"
                p.io_end = self.t + dur
                self.io_list.append(p)
                self.log(f"P{p.pid} -> IO for {dur} (ends at {p.io_end})")
                self.running = None
                return False
            if cmd[0] == "REQ":
                rid, amt = cmd[1], cmd[2]
                if self.avail.get(rid, 0) >= amt:
                    self.avail[rid] -= amt
                    p.held[rid] += amt
                    p.cmd_i += 1
                    self.log(f"P{p.pid} acquired R{rid}x{amt}")
                    continue
                self.log(f"P{p.pid} BLOCKED on R{rid}x{amt}")
                p.state = "WAITING"
                self.pending_req[p.pid] = (rid, amt)
                self.waiting.append(p)
                self.running = None
                self._deadlock_check_and_recover(trigger=f"block:P{p.pid}")
                return False
            if cmd[0] == "REL":
                rid, amt = cmd[1], cmd[2]
                p.held[rid] -= amt
                self.avail[rid] += amt
                p.cmd_i += 1
                self.log(f"P{p.pid} released R{rid}x{amt}")
                self._resource_satisfaction(tag="after_release")
                continue
            if cmd[0] == "EXEC":
                return True
            raise RuntimeError(f"Unknown cmd: {cmd}")
        return False

    def step(self):
        self._handle_arrivals()
        self._handle_io_completions()
        self._resource_satisfaction(tag="early")
        self._apply_aging_at_boundary()
        self._preempt_if_higher_ready()
        self._rr_rotate_if_needed()
        self._dispatch()
        while True:
            if not self.running:
                self._dispatch()
                if not self.running:
                    break
            if self._collapse_zero_time_until_exec_or_block():
                break
        self._resource_satisfaction(tag="same_tick")
        if not self.running:
            self._dispatch()
        while True:
            if not self.running:
                self._dispatch()
                if not self.running:
                    break
            if self._collapse_zero_time_until_exec_or_block():
                break
        if self.running is None and self._best_ready_prio() is None and len(self.waiting) > 0:
            self._deadlock_check_and_recover(trigger="cpu_idle_with_waiting")
        if not self.running:
            if not all(p.state == "TERM" for p in self.procs):
                self.gantt_ticks.append("IDLE")
        else:
            p = self.running
            self.gantt_ticks.append(f"P{p.pid}")
            cmd = p.cur_cmd()
            if not cmd or cmd[0] != "EXEC":
                raise RuntimeError(f"Expected EXEC, got {cmd}")
            p.exec_prog += 1
            if self._rr_active_for_running():
                p.q_left -= 1
            if p.exec_prog >= cmd[1]:
                p.exec_prog = 0
                p.cmd_i += 1
        for pr in range(21):
            for p in self.ready[pr]:
                p.wait_ready += 1
                p.ready_timer += 1
        self.t += 1

    def run(self, max_ticks: int = 100000):
        while not all(p.state == "TERM" for p in self.procs):
            if self.t >= max_ticks:
                break
            self.step()
        while self.gantt_ticks and self.gantt_ticks[-1] == "IDLE":
            self.gantt_ticks.pop()

    def gantt_events(self) -> List[Tuple[int, str]]:
        events = []
        prev = None
        for t, lab in enumerate(self.gantt_ticks):
            if lab != prev:
                events.append((t, lab))
                prev = lab
        return events


def main():
    INPUT_FILE = "innn.txt"
    try:
        print(f"Reading input from '{INPUT_FILE}'...")
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            input_text = f.read()
        total_res, procs = parse_input_text(input_text)
        print(f"Parsed {len(procs)} processes and {len(total_res)} resource types.")
        print("=" * 60)
        sim = Scheduler(total_res, procs, quantum=8)
        sim.run()
        print("\n" + "=" * 60)
        print("FINAL SYSTEM REPORT")
        print("=" * 60)
        out = []
        for t, lab in sim.gantt_events():
            out.append(f"{t} p{lab[1:]}" if lab.startswith("P") else f"{t} idle")
        out.append(str(len(sim.gantt_ticks)))
        print("Gantt Chart Timeline:", " ".join(out))
        total_wait = 0
        total_tat = 0
        for p in sorted(procs, key=lambda x: x.pid):
            tat = (p.finish - p.arrival) if p.finish is not None else 0
            total_wait += p.wait_ready
            total_tat += tat
        avg_wait = total_wait / len(procs) if procs else 0.0
        avg_tat = total_tat / len(procs) if procs else 0.0
        print("\nSummary:")
        print(f"  Average Waiting Time: {avg_wait:.2f}")
        print(f"  Average Turnaround Time: {avg_tat:.2f}")
        print("\n" + "=" * 60)
        print("DEADLOCK REPORT")
        print("=" * 60)
        if not sim.deadlock_events:
            print("No deadlocks detected during simulation.")
        else:
            print("Deadlock Detections:")
            for s in sim.deadlock_events:
                print(f"  - {s}")
            print("\nRecovery Actions:")
            for s in sim.deadlock_recovery:
                print(f"  - {s}")
    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_FILE}' not found!")
        sys.exit(1)
    except Exception as e:
        print(f"Error during simulation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
