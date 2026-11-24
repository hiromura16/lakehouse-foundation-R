"""合成装置だけに接続し、設定された読取・拒否の境界を照合する。"""

import json

from pymodbus.client import ModbusTcpClient
from pymodbus.constants import ExcCodes


def main() -> None:
    """番地外、単点書込、複数点書込の例外と書込前後の不変を確認する。"""
    with ModbusTcpClient("simulator", port=1502, timeout=2, retries=0) as plc:
        initial = plc.read_holding_registers(0, count=6, device_id=7)
        if initial.isError():
            raise RuntimeError("合成レジスタが読み出せません。")
        checks = {
            "invalid_address": plc.read_holding_registers(
                6, count=1, device_id=7
            ),
            "single_write": plc.write_register(0, 42, device_id=7),
            "multiple_write": plc.write_registers(2, [42, 42], device_id=7),
        }
        for label, response in checks.items():
            if (
                not response.isError()
                or response.exception_code != ExcCodes.ILLEGAL_ADDRESS
            ):
                raise RuntimeError(f"{label} で番地エラーが返りませんでした。")
        final = plc.read_holding_registers(0, count=6, device_id=7)
        if final.isError() or final.registers != initial.registers:
            raise RuntimeError("拒否後の合成レジスタが変化しました。")
        print(
            json.dumps(
                {
                    "phase": "negative_probe",
                    "exception_code": int(ExcCodes.ILLEGAL_ADDRESS),
                    "checks": list(checks),
                    "registers_unchanged": True,
                }
            )
        )


if __name__ == "__main__":
    main()
