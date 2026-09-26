from __future__ import annotations
import struct
from dataclasses import dataclass

UNSET_TIME = 0xFFFFFFFF
_TAIL_SIZE = 10

@dataclass(frozen=True)
class CommandPacket:
    apid: int
    sequence: int
    opcode: int
    exec_time_tai_s: int | None = None
    expire_time_tai_s: int | None = None
    payload: bytes = b''


def encode(packet: CommandPacket):
    if not 0 <= packet.apid < 2048:
        raise ValueError('APID range')
    if not 0 <= packet.sequence < 16384:
        raise ValueError('sequence range')
    if not 0 <= packet.opcode < 65536:
        raise ValueError('opcode range')
    for t in (packet.exec_time_tai_s, packet.expire_time_tai_s):
        if t is not None and not 0 <= t < UNSET_TIME:
            raise ValueError('time range')
    first = packet.apid
    second = 0xC000 | packet.sequence
    length = len(packet.payload) + _TAIL_SIZE - 1
    exec_field = UNSET_TIME if packet.exec_time_tai_s is None else packet.exec_time_tai_s
    expire_field = UNSET_TIME if packet.expire_time_tai_s is None else packet.expire_time_tai_s
    header = struct.pack('>HHHIIH', first, second, length, exec_field, expire_field, packet.opcode)
    return header + packet.payload


def decode(data: bytes):
    if len(data) < 16:
        raise ValueError('truncated command packet')
    first, second, length, exec_field, expire_field, opcode = struct.unpack('>HHHIIH', data[:16])
    expected = length + 1 - _TAIL_SIZE
    payload = data[16:]
    if len(payload) != expected:
        raise ValueError('packet length mismatch')
    exec_time = None if exec_field == UNSET_TIME else exec_field
    expire_time = None if expire_field == UNSET_TIME else expire_field
    return CommandPacket(first & 0x7FF, second & 0x3FFF, opcode, exec_time, expire_time, payload)
