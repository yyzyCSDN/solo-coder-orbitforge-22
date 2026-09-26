"""On-board command execution timeline.

Every command_id reaches exactly one terminal status (EXECUTED, FAILED,
REJECTED_EXPIRED, REJECTED_PRECONDITION) and retransmitted or out-of-order
batches are de-duplicated by command_id, so an already-finished command can
never execute twice.  Each submission yields an ExecutionReceipt carrying the
command_id, status and reason, so ground can trace the outcome — including
failures — back to the specific command.
"""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from orbitforge.telemetry import sequence as arrival_sequence


class CommandStatus(Enum):
    PENDING = 'pending'
    EXECUTED = 'executed'
    FAILED = 'failed'
    REJECTED_DUPLICATE = 'rejected_duplicate'
    REJECTED_EXPIRED = 'rejected_expired'
    REJECTED_PRECONDITION = 'rejected_precondition'

    @property
    def is_terminal(self):
        return self is not CommandStatus.PENDING


@dataclass(frozen=True)
class Precondition:
    name: str
    check: object  # callable(now_tai_s, context) -> bool


@dataclass(frozen=True)
class CommandPacket:
    command_id: int
    apid: int
    sequence: int
    execute_at: float
    opcode: str
    args: tuple = ()
    expires_at: float | None = None
    preconditions: tuple = ()

    def __post_init__(self):
        if not 0 <= self.sequence < arrival_sequence.MODULUS:
            raise ValueError('sequence range')


@dataclass(frozen=True)
class ExecutionReceipt:
    command_id: int
    apid: int
    sequence: int
    opcode: str
    status: CommandStatus
    reason: str
    execute_at: float
    receipt_at: float

    @property
    def is_terminal(self):
        return self.status.is_terminal


class CommandTimeline:

    def __init__(self, handler=None, context=None):
        self._handler = handler if handler is not None else lambda packet, now: None
        self._context = context if context is not None else {}
        self._terminal = {}
        self._pending = {}
        self._receipts = []
        self._arrivals = []
        self._last_sequence = {}

    def submit(self, packet, now):
        self._log_arrival(packet)
        known = self._terminal.get(packet.command_id)
        if known is not None:
            return self._record(packet, CommandStatus.REJECTED_DUPLICATE,
                                f'already {known.status.value} at {known.receipt_at}', now)
        if packet.command_id in self._pending:
            return self._record(packet, CommandStatus.REJECTED_DUPLICATE, 'already scheduled', now)
        if self._expired(packet, now):
            return self._terminate(packet, CommandStatus.REJECTED_EXPIRED,
                                   f'expired at {packet.expires_at}, arrived {now}', now)
        if packet.execute_at > now:
            self._pending[packet.command_id] = packet
            return self._record(packet, CommandStatus.PENDING, f'scheduled for {packet.execute_at}', now)
        return self._execute(packet, now)

    def submit_batch(self, packets, now):
        ordered = sorted(packets, key=lambda p: (p.execute_at, p.command_id))
        return [self.submit(p, now) for p in ordered]

    def run_until(self, now):
        due = sorted((p for p in self._pending.values() if p.execute_at <= now),
                     key=lambda p: (p.execute_at, p.command_id))
        receipts = []
        for packet in due:
            del self._pending[packet.command_id]
            receipts.append(self._execute(packet, now))
        return receipts

    @property
    def receipts(self):
        return tuple(self._receipts)

    @property
    def arrivals(self):
        return tuple(self._arrivals)

    @property
    def context(self):
        return self._context

    def receipt_for(self, command_id):
        return self._terminal.get(command_id)

    def status_of(self, command_id):
        receipt = self._terminal.get(command_id)
        if receipt is not None:
            return receipt.status
        if command_id in self._pending:
            return CommandStatus.PENDING
        return None

    def pending(self):
        return sorted(self._pending.values(), key=lambda p: (p.execute_at, p.command_id))

    def failures(self):
        return [r for r in self._terminal.values() if r.status is CommandStatus.FAILED]

    def _execute(self, packet, now):
        if self._expired(packet, now):
            return self._terminate(packet, CommandStatus.REJECTED_EXPIRED,
                                   f'expired at {packet.expires_at}, due {now}', now)
        for pre in packet.preconditions:
            if not pre.check(now, self._context):
                return self._terminate(packet, CommandStatus.REJECTED_PRECONDITION,
                                       f'precondition not satisfied: {pre.name}', now)
        try:
            self._handler(packet, now)
        except Exception as exc:
            return self._terminate(packet, CommandStatus.FAILED,
                                   f'{type(exc).__name__}: {exc}', now)
        return self._terminate(packet, CommandStatus.EXECUTED, 'executed', now)

    def _expired(self, packet, now):
        return packet.expires_at is not None and now > packet.expires_at

    def _record(self, packet, status, reason, now):
        receipt = ExecutionReceipt(packet.command_id, packet.apid, packet.sequence,
                                   packet.opcode, status, reason, packet.execute_at, now)
        self._receipts.append(receipt)
        return receipt

    def _terminate(self, packet, status, reason, now):
        receipt = self._record(packet, status, reason, now)
        self._terminal[packet.command_id] = receipt
        return receipt

    def _log_arrival(self, packet):
        last = self._last_sequence.get(packet.apid)
        kind = 'first' if last is None else arrival_sequence.classify(last, packet.sequence)
        self._arrivals.append((packet.apid, packet.sequence, kind))
        if kind in ('first', 'next', 'gap'):
            self._last_sequence[packet.apid] = packet.sequence
