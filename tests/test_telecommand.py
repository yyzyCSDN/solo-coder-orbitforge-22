import pytest
from orbitforge.telecommand.packet import CommandPacket, encode, decode
from orbitforge.telecommand.dispatcher import (
    Dispatcher, ACCEPTED, DUPLICATE, EXECUTED, EXPIRED, FAILED, REJECTED, SCHEDULED,
)

APID = 42


def make_dispatcher(context=None):
    d = Dispatcher(context)
    d.register_precondition('sunlit', lambda ctx: ctx.get('sunlit', False))
    d.register_precondition('ground_contact', lambda ctx: ctx.get('ground_contact', False))
    return d


def test_packet_roundtrip_immediate():
    p = CommandPacket(APID, 7, 100, payload=b'\x01\x02')
    assert decode(encode(p)) == p


def test_packet_roundtrip_time_tagged():
    p = CommandPacket(APID, 7, 100, exec_time_tai_s=1000, expire_time_tai_s=2000, payload=b'abc')
    assert decode(encode(p)) == p


def test_packet_decode_rejects_truncated_and_bad_length():
    p = CommandPacket(APID, 1, 100, payload=b'xy')
    data = encode(p)
    with pytest.raises(ValueError):
        decode(data[:8])
    with pytest.raises(ValueError):
        decode(data[:-1])


def test_packet_encode_range_checks():
    with pytest.raises(ValueError):
        encode(CommandPacket(2048, 0, 1))
    with pytest.raises(ValueError):
        encode(CommandPacket(0, 16384, 1))
    with pytest.raises(ValueError):
        encode(CommandPacket(0, 0, 65536))


def test_immediate_command_executes_with_receipt():
    d = make_dispatcher()
    d.register_handler(100, lambda payload, ctx: payload[0] + 1)
    (result,) = d.ingest(CommandPacket(APID, 1, 100, payload=b'\x05'), now=10)
    assert result.outcome == ACCEPTED
    (receipt,) = d.run_until(10)
    assert receipt.state == EXECUTED and receipt.result == 6
    assert receipt.key == (APID, 1) and receipt.received_tai_s == 10
    assert d.status_of(APID, 1) == EXECUTED


def test_scheduled_command_waits_for_exec_time():
    d = make_dispatcher()
    calls = []
    d.register_handler(100, lambda payload, ctx: calls.append(1))
    d.ingest(CommandPacket(APID, 1, 100, exec_time_tai_s=500), now=10)
    assert d.status_of(APID, 1) == SCHEDULED
    assert d.run_until(499) == [] and calls == []
    (receipt,) = d.run_until(500)
    assert receipt.state == EXECUTED and len(calls) == 1


def test_scheduled_command_expires_before_exec_time():
    d = make_dispatcher()
    d.register_handler(100, lambda payload, ctx: None)
    d.ingest(CommandPacket(APID, 1, 100, exec_time_tai_s=500, expire_time_tai_s=400), now=10)
    (receipt,) = d.run_until(401)
    assert receipt.state == EXPIRED and receipt.reason == 'execution window passed'
    assert d.status_of(APID, 1) == EXPIRED


def test_late_arrival_expires_at_ingest():
    d = make_dispatcher()
    d.register_handler(100, lambda payload, ctx: None)
    (result,) = d.ingest(CommandPacket(APID, 1, 100, expire_time_tai_s=100), now=150)
    assert result.outcome == EXPIRED and result.detail == 'arrived after expiry'
    assert d.receipt_for(APID, 1).state == EXPIRED


def test_precondition_failure_rejects_with_reason():
    d = make_dispatcher(context={'sunlit': False})
    d.register_handler(100, lambda payload, ctx: None, preconditions=('sunlit',))
    d.ingest(CommandPacket(APID, 1, 100), now=10)
    (receipt,) = d.run_until(10)
    assert receipt.state == REJECTED and receipt.reason == 'precondition failed: sunlit'
    assert d.failures() == (receipt,)


def test_precondition_passes_when_context_changes():
    d = make_dispatcher(context={'sunlit': False})
    d.register_handler(100, lambda payload, ctx: 'fired', preconditions=('sunlit',))
    d.ingest(CommandPacket(APID, 1, 100, exec_time_tai_s=500), now=10)
    d.context['sunlit'] = True
    (receipt,) = d.run_until(500)
    assert receipt.state == EXECUTED and receipt.result == 'fired'


def test_handler_exception_fails_and_locates_command():
    d = make_dispatcher()

    def boom(payload, ctx):
        raise RuntimeError('valve stuck')

    d.register_handler(100, lambda payload, ctx: None)
    d.register_handler(200, boom)
    d.ingest([CommandPacket(APID, 1, 100), CommandPacket(APID, 2, 200)], now=10)
    d.run_until(10)
    assert d.status_of(APID, 1) == EXECUTED
    assert d.status_of(APID, 2) == FAILED
    receipt = d.receipt_for(APID, 2)
    assert receipt.opcode == 200 and 'valve stuck' in receipt.reason
    assert [r.key for r in d.failures()] == [(APID, 2)]


def test_retransmission_after_execution_does_not_reexecute():
    d = make_dispatcher()
    calls = []
    d.register_handler(100, lambda payload, ctx: calls.append(1))
    packet = CommandPacket(APID, 5, 100)
    d.ingest(packet, now=10)
    d.run_until(10)
    (dup,) = d.ingest(packet, now=20)
    assert dup.outcome == DUPLICATE and dup.detail == f'already {EXECUTED}'
    assert d.run_until(20) == []
    assert len(calls) == 1 and d.status_of(APID, 5) == EXECUTED


def test_retransmitted_batch_mixed_with_new_command():
    d = make_dispatcher()
    calls = []
    d.register_handler(100, lambda payload, ctx: calls.append(payload))
    batch = [CommandPacket(APID, i, 100, payload=bytes([i])) for i in (1, 2, 3)]
    d.ingest(batch, now=10)
    d.run_until(10)
    results = d.ingest([batch[1], batch[2], CommandPacket(APID, 4, 100, payload=b'\x04')], now=20)
    assert [r.outcome for r in results] == [DUPLICATE, DUPLICATE, ACCEPTED]
    d.run_until(20)
    assert calls == [b'\x01', b'\x02', b'\x03', b'\x04']


def test_out_of_order_batch_executes_once_in_exec_time_order():
    d = make_dispatcher()
    order = []
    d.register_handler(100, lambda payload, ctx: order.append(payload.decode()))
    late = CommandPacket(APID, 3, 100, exec_time_tai_s=300, payload=b'c')
    early = CommandPacket(APID, 1, 100, exec_time_tai_s=100, payload=b'a')
    mid = CommandPacket(APID, 2, 100, exec_time_tai_s=200, payload=b'b')
    d.ingest([late, early, mid], now=50)
    d.run_until(300)
    assert order == ['a', 'b', 'c']
    assert d.outstanding() == ()


def test_unknown_opcode_rejected_at_ingest():
    d = make_dispatcher()
    (result,) = d.ingest(CommandPacket(APID, 1, 999), now=10)
    assert result.outcome == REJECTED and result.detail == 'unknown opcode: 999'
    assert d.status_of(APID, 1) == REJECTED


def test_sequence_gap_reported_on_ingest():
    d = make_dispatcher()
    d.register_handler(100, lambda payload, ctx: None)
    d.ingest(CommandPacket(APID, 5, 100), now=10)
    (result,) = d.ingest(CommandPacket(APID, 8, 100), now=11)
    assert result.outcome == ACCEPTED and result.missing == (6, 7)


def test_status_of_unknown_command_is_none():
    d = make_dispatcher()
    assert d.status_of(APID, 77) is None
    assert d.receipt_for(APID, 77) is None
