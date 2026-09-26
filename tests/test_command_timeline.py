import pytest
from orbitforge.operations.command_timeline import (
    CommandPacket, CommandStatus, CommandTimeline, Precondition,
)


def make_packet(command_id=1, execute_at=100.0, **kw):
    defaults = dict(apid=10, sequence=command_id % 16384, opcode='SET_MODE')
    defaults.update(kw)
    return CommandPacket(command_id=command_id, execute_at=execute_at, **defaults)


def recording_handler(calls):
    def handle(packet, now):
        calls.append((packet.command_id, now))
    return handle


def test_due_command_executes_immediately():
    calls = []
    tl = CommandTimeline(handler=recording_handler(calls))
    receipt = tl.submit(make_packet(1, execute_at=100.0), now=150.0)
    assert receipt.status is CommandStatus.EXECUTED and receipt.is_terminal
    assert calls == [(1, 150.0)]
    assert tl.status_of(1) is CommandStatus.EXECUTED


def test_future_command_pends_then_runs_at_execution_time():
    calls = []
    tl = CommandTimeline(handler=recording_handler(calls))
    receipt = tl.submit(make_packet(1, execute_at=100.0), now=50.0)
    assert receipt.status is CommandStatus.PENDING and not receipt.is_terminal
    assert calls == [] and tl.run_until(99.0) == []
    receipts = tl.run_until(100.0)
    assert [r.status for r in receipts] == [CommandStatus.EXECUTED]
    assert calls == [(1, 100.0)]


def test_retransmitted_command_never_reexecutes():
    calls = []
    tl = CommandTimeline(handler=recording_handler(calls))
    packet = make_packet(7, execute_at=10.0)
    first = tl.submit(packet, now=10.0)
    second = tl.submit(packet, now=11.0)
    third = tl.submit_batch([packet, packet], now=12.0)
    assert first.status is CommandStatus.EXECUTED
    assert second.status is CommandStatus.REJECTED_DUPLICATE
    assert all(r.status is CommandStatus.REJECTED_DUPLICATE for r in third)
    assert 'already executed' in second.reason
    assert calls == [(7, 10.0)]
    assert tl.receipt_for(7).status is CommandStatus.EXECUTED


def test_out_of_order_batch_executes_in_timeline_order():
    calls = []
    tl = CommandTimeline(handler=recording_handler(calls))
    batch = [make_packet(3, execute_at=30.0), make_packet(1, execute_at=10.0),
             make_packet(2, execute_at=20.0)]
    tl.submit_batch(batch, now=100.0)
    assert [c[0] for c in calls] == [1, 2, 3]


def test_expired_command_is_terminal_and_not_executed():
    calls = []
    tl = CommandTimeline(handler=recording_handler(calls))
    receipt = tl.submit(make_packet(1, execute_at=10.0, expires_at=50.0), now=60.0)
    assert receipt.status is CommandStatus.REJECTED_EXPIRED and receipt.is_terminal
    assert calls == []
    again = tl.submit(make_packet(1, execute_at=10.0, expires_at=50.0), now=61.0)
    assert again.status is CommandStatus.REJECTED_DUPLICATE
    assert tl.status_of(1) is CommandStatus.REJECTED_EXPIRED


def test_pending_command_that_expires_is_rejected_when_due():
    calls = []
    tl = CommandTimeline(handler=recording_handler(calls))
    tl.submit(make_packet(1, execute_at=100.0, expires_at=150.0), now=0.0)
    receipts = tl.run_until(200.0)
    assert [r.status for r in receipts] == [CommandStatus.REJECTED_EXPIRED]
    assert calls == []


def test_failed_precondition_names_the_condition_and_is_terminal():
    calls = []
    context = {'sunlit': False}
    tl = CommandTimeline(handler=recording_handler(calls), context=context)
    pre = Precondition('sunlit', lambda now, ctx: ctx['sunlit'])
    receipt = tl.submit(make_packet(1, execute_at=10.0, preconditions=(pre,)), now=10.0)
    assert receipt.status is CommandStatus.REJECTED_PRECONDITION
    assert 'sunlit' in receipt.reason and calls == []
    context['sunlit'] = True
    retry = tl.submit(make_packet(1, execute_at=10.0, preconditions=(pre,)), now=11.0)
    assert retry.status is CommandStatus.REJECTED_DUPLICATE and calls == []
    fresh = tl.submit(make_packet(2, execute_at=10.0, preconditions=(pre,)), now=12.0)
    assert fresh.status is CommandStatus.EXECUTED and calls == [(2, 12.0)]


def test_handler_failure_is_located_to_the_command():
    def handler(packet, now):
        if packet.opcode == 'ARM':
            raise RuntimeError('latch valve stuck')
    tl = CommandTimeline(handler=handler)
    ok = tl.submit(make_packet(1, execute_at=10.0, opcode='HEATER_ON'), now=10.0)
    bad = tl.submit(make_packet(2, execute_at=11.0, opcode='ARM'), now=11.0)
    assert ok.status is CommandStatus.EXECUTED
    assert bad.status is CommandStatus.FAILED and bad.command_id == 2
    assert 'latch valve stuck' in bad.reason
    failures = tl.failures()
    assert len(failures) == 1 and failures[0].command_id == 2
    resend = tl.submit(make_packet(2, execute_at=11.0, opcode='ARM'), now=12.0)
    assert resend.status is CommandStatus.REJECTED_DUPLICATE
    assert 'already failed' in resend.reason


def test_every_command_reaches_exactly_one_terminal_state():
    tl = CommandTimeline(handler=recording_handler([]))
    pre = Precondition('never', lambda now, ctx: False)
    tl.submit_batch([
        make_packet(1, execute_at=10.0),
        make_packet(2, execute_at=10.0, expires_at=5.0),
        make_packet(3, execute_at=10.0, preconditions=(pre,)),
        make_packet(1, execute_at=10.0),
    ], now=10.0)
    terminals = {cid: tl.status_of(cid) for cid in (1, 2, 3)}
    assert terminals == {1: CommandStatus.EXECUTED,
                         2: CommandStatus.REJECTED_EXPIRED,
                         3: CommandStatus.REJECTED_PRECONDITION}
    assert all(tl.receipt_for(cid).is_terminal for cid in (1, 2, 3))
    assert sum(1 for r in tl.receipts if r.command_id == 1
               and r.status is CommandStatus.REJECTED_DUPLICATE) == 1


def test_arrival_log_flags_retransmission_and_gap():
    tl = CommandTimeline(handler=recording_handler([]))
    tl.submit(make_packet(1, execute_at=10.0, sequence=5), now=10.0)
    tl.submit(make_packet(2, execute_at=10.0, sequence=9), now=10.0)
    tl.submit(make_packet(1, execute_at=10.0, sequence=5), now=10.0)
    kinds = [a[2] for a in tl.arrivals]
    assert kinds == ['first', 'gap', 'old']


def test_invalid_sequence_rejected_at_construction():
    with pytest.raises(ValueError):
        make_packet(1, sequence=16384)
