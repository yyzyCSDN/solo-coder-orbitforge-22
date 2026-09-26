from __future__ import annotations
from dataclasses import dataclass
from orbitforge.telemetry import sequence as wireseq
from .packet import CommandPacket

PENDING = 'pending'
SCHEDULED = 'scheduled'
EXECUTED = 'executed'
FAILED = 'failed'
REJECTED = 'rejected'
EXPIRED = 'expired'
DUPLICATE = 'duplicate'
TERMINAL = frozenset((EXECUTED, FAILED, REJECTED, EXPIRED, DUPLICATE))
ACCEPTED = 'accepted'

@dataclass(frozen=True)
class Receipt:
    apid: int
    sequence: int
    opcode: int
    state: str
    reason: str
    received_tai_s: float
    finished_tai_s: float
    exec_time_tai_s: int | None
    result: object = None

    @property
    def key(self):
        return (self.apid, self.sequence)

@dataclass(frozen=True)
class IngestResult:
    apid: int
    sequence: int
    outcome: str
    detail: str = ''
    missing: tuple = ()

class _Record:

    __slots__ = ('packet', 'received_tai_s', 'state', 'receipt')

    def __init__(self, packet: CommandPacket, received_tai_s: float):
        self.packet = packet
        self.received_tai_s = received_tai_s
        self.state = PENDING
        self.receipt = None

class Dispatcher:

    def __init__(self, context=None):
        self.context = {} if context is None else context
        self._handlers = {}
        self._preconditions = {}
        self._records = {}
        self._receipts = []
        self._last_sequence = {}

    def register_precondition(self, name, predicate):
        self._preconditions[name] = predicate

    def register_handler(self, opcode, handler, preconditions=()):
        for name in preconditions:
            if name not in self._preconditions:
                raise KeyError(f'unknown precondition: {name}')
        self._handlers[opcode] = (handler, tuple(preconditions))

    def ingest(self, packets, now):
        if isinstance(packets, CommandPacket):
            packets = (packets,)
        return [self._ingest_one(p, now) for p in packets]

    def run_until(self, now):
        candidates = [r for r in self._records.values() if r.state in (PENDING, SCHEDULED)]
        candidates.sort(key=lambda r: (r.packet.exec_time_tai_s if r.packet.exec_time_tai_s is not None else r.received_tai_s, r.packet.apid, r.packet.sequence))
        made = []
        for record in candidates:
            packet = record.packet
            if self._expired(packet, now):
                made.append(self._finish(record, now, EXPIRED, 'execution window passed'))
                continue
            if packet.exec_time_tai_s is not None and packet.exec_time_tai_s > now:
                continue
            _, conditions = self._handlers[packet.opcode]
            failed = next((n for n in conditions if not self._preconditions[n](self.context)), None)
            if failed is not None:
                made.append(self._finish(record, now, REJECTED, f'precondition failed: {failed}'))
                continue
            handler = self._handlers[packet.opcode][0]
            try:
                result = handler(packet.payload, self.context)
            except Exception as exc:
                made.append(self._finish(record, now, FAILED, f'{type(exc).__name__}: {exc}'))
            else:
                made.append(self._finish(record, now, EXECUTED, '', result))
        return made

    def status_of(self, apid, sequence):
        record = self._records.get((apid, sequence))
        return None if record is None else record.state

    def receipt_for(self, apid, sequence):
        record = self._records.get((apid, sequence))
        return None if record is None else record.receipt

    @property
    def receipts(self):
        return tuple(self._receipts)

    def failures(self):
        return tuple(r for r in self._receipts if r.state in (FAILED, REJECTED, EXPIRED))

    def outstanding(self):
        return tuple(sorted(k for k, r in self._records.items() if r.state not in TERMINAL))

    def _ingest_one(self, packet, now):
        key = (packet.apid, packet.sequence)
        missing = self._gap_diagnostic(packet)
        existing = self._records.get(key)
        if existing is not None:
            return IngestResult(packet.apid, packet.sequence, DUPLICATE, f'already {existing.state}', missing)
        record = _Record(packet, now)
        self._records[key] = record
        if packet.opcode not in self._handlers:
            self._finish(record, now, REJECTED, f'unknown opcode: {packet.opcode}')
            return IngestResult(packet.apid, packet.sequence, REJECTED, record.receipt.reason, missing)
        if self._expired(packet, now):
            self._finish(record, now, EXPIRED, 'arrived after expiry')
            return IngestResult(packet.apid, packet.sequence, EXPIRED, record.receipt.reason, missing)
        if packet.exec_time_tai_s is not None and packet.exec_time_tai_s > now:
            record.state = SCHEDULED
        return IngestResult(packet.apid, packet.sequence, ACCEPTED, '', missing)

    def _finish(self, record, now, state, reason, result=None):
        packet = record.packet
        receipt = Receipt(packet.apid, packet.sequence, packet.opcode, state, reason, record.received_tai_s, now, packet.exec_time_tai_s, result)
        record.state = state
        record.receipt = receipt
        self._receipts.append(receipt)
        return receipt

    def _gap_diagnostic(self, packet):
        last = self._last_sequence.get(packet.apid)
        if last is None:
            self._last_sequence[packet.apid] = packet.sequence
            return ()
        kind = wireseq.classify(last, packet.sequence)
        if kind in ('next', 'gap'):
            self._last_sequence[packet.apid] = packet.sequence
        if kind == 'gap':
            return tuple(wireseq.missing_sequences(last, packet.sequence))
        return ()

    @staticmethod
    def _expired(packet, now):
        return packet.expire_time_tai_s is not None and now > packet.expire_time_tai_s
