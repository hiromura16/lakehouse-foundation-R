"""隔離ネットワーク用の読取専用Modbus TCP装置。"""

import asyncio
import json
import os

from pymodbus.pdu import ModbusPDU
from pymodbus.server import StartAsyncTcpServer
from pymodbus.simulator import DataType, SimData, SimDevice

# 0xFF85は符号付きで-123、0x8000はこの合成装置固有の欠測コード。
TEMPERATURE = {"normal": 0xFF85, "invalid": 0x8000, "recovered": 0x00F3}


def trace_request(sending: bool, pdu: ModbusPDU) -> ModbusPDU:
    """受信要求の機能コードと範囲だけを出力し、収集が読取専用と検証する。"""
    if not sending:
        print(
            json.dumps(
                {
                    "kind": "request",
                    "unit": pdu.dev_id,
                    "function": pdu.function_code,
                    "address": getattr(pdu, "address", None),
                    "count": getattr(pdu, "count", None),
                }
            )
        )
    return pdu


async def main() -> None:
    """6レジスタだけを定義し、合成データはコンテナの再作成で切り替える。"""
    mode = os.environ.get("FOUNDATION_MODBUS_FIXTURE_MODE", "normal")
    registers = (TEMPERATURE[mode], 0x3412, 0x5678, 0x1234, 0x0000, 0x41BC)
    device = SimDevice(
        id=7,
        simdata=[
            SimData(
                address,
                values=value,
                datatype=DataType.REGISTERS,
                readonly=True,
            )
            for address, value in enumerate(registers)
        ],
    )
    await StartAsyncTcpServer(
        context=device,
        address=("0.0.0.0", 1502),
        trace_pdu=trace_request,
    )


if __name__ == "__main__":
    asyncio.run(main())
