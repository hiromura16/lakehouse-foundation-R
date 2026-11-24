"""外部機器を使わず、暗号化されたOPC UA購読向けのデータ品質を再現する。"""

import argparse
import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from asyncua import Client, Server, ua
from asyncua.common.utils import ServiceError
from asyncua.crypto import cert_gen
from asyncua.crypto.permission_rules import SimpleRoleRuleset
from asyncua.crypto.security_policies import SecurityPolicyBasic256Sha256
from asyncua.server.user_managers import CertificateUserManager
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.x509.oid import ExtendedKeyUsageOID

HOST = "opcua-simulator"
SERVER_URI = "urn:foundation:r:opcua-simulator"
CLIENT_URI = "urn:foundation:r:opcua-collector"
SOURCE_TIME = datetime(2026, 1, 5, 13, tzinfo=timezone.utc)
PKI = Path("/run/secrets")


class ReadOnly(SimpleRoleRuleset):
    """模擬機器の既定ロールから書込みとメソッド呼出しを外す。"""

    def check_validity(self, user, action_type, body):
        """Readと購読のほかに必要なセッション操作だけを許可する。"""
        forbidden = {
            ua.ObjectIds.WriteRequest_Encoding_DefaultBinary,
            ua.ObjectIds.CallRequest_Encoding_DefaultBinary,
        }
        if action_type.Identifier in forbidden:
            return False
        return super().check_validity(user, action_type, body)


async def certificates(directory):
    """試験プロジェクト専用の鍵と証明書を生成する。"""
    for name, uri, host, usage in (
        ("server", SERVER_URI, HOST, ExtendedKeyUsageOID.SERVER_AUTH),
        (
            "client",
            CLIENT_URI,
            "telegraf-opcua",
            ExtendedKeyUsageOID.CLIENT_AUTH,
        ),
        ("other", SERVER_URI, HOST, ExtendedKeyUsageOID.SERVER_AUTH),
    ):
        key = directory / f"{name}-key.pem"
        cert = directory / f"{name}-cert.pem"
        private_key = cert_gen.generate_private_key()
        leaf = cert_gen.generate_self_signed_app_certificate(
            private_key,
            uri,
            {"commonName": host},
            [x509.UniformResourceIdentifier(uri), x509.DNSName(host)],
            extended=[usage],
            days=1,
        )
        # ファイルを新規作成する時点から、所有者だけが鍵を読めるようにする。
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        with os.fdopen(os.open(key, flags, 0o400), "wb") as stream:
            stream.write(cert_gen.dump_private_key_as_pem(private_key))
        cert.write_bytes(leaf.public_bytes(serialization.Encoding.PEM))
        cert.chmod(0o444)


async def simulate():
    """固定ノードだけを公開し、付与した証明書以外のセッションを拒否する。"""
    users = CertificateUserManager()
    client_cert = PKI / "client-cert"
    await users.add_user(client_cert, "collector", format="pem")
    fingerprint = x509.load_pem_x509_certificate(
        client_cert.read_bytes()
    ).fingerprint(hashes.SHA256())

    async def validate(cert, application):
        """承認済みクライアントのDER指紋を、セッション作成時に照合する。"""
        if cert.fingerprint(hashes.SHA256()) != fingerprint:
            raise ServiceError(ua.StatusCodes.BadCertificateUntrusted)

    server = Server(user_manager=users)
    await server.init()
    await server.set_application_uri(SERVER_URI)
    server.set_endpoint(f"opc.tcp://{HOST}:4840/foundation")
    server.set_security_policy(
        [ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt],
        permission_ruleset=ReadOnly(),
    )
    server.set_identity_tokens([ua.X509IdentityToken])
    await server.load_certificate(PKI / "server-cert", format="pem")
    await server.load_private_key(PKI / "server-key", format="pem")
    server.set_certificate_validator(validate)
    idx = await server.register_namespace(SERVER_URI)
    point = await server.nodes.objects.add_variable(
        ua.NodeId("Temperature", idx), "Temperature", 22.5
    )

    async def measure(delta, value, status=ua.StatusCodes.Good):
        """ServerTimestampと異なるSourceTimestampを通知に入れる。"""
        stamp = SOURCE_TIME + timedelta(seconds=delta)
        await point.write_value(
            ua.DataValue(
                ua.Variant(value, ua.VariantType.Double),
                ua.StatusCode(status),
                SourceTimestamp=stamp,
                ServerTimestamp=stamp + timedelta(minutes=10),
            )
        )
        # 購読や保存が止まった際に、設備側の確定段階だけを追えるようにする。
        print(f"試験用温度の書込み完了: {delta}秒、状態0x{status:08x}")

    async def wait_for_trigger(name):
        """通知を照合してから試験側が次の点を進める。"""
        flag = Path("/tmp") / name
        while not flag.exists():
            await asyncio.sleep(0.2)

    await measure(0, 22.5)
    async with server:
        print("試験用OPC UAを開始しました。")
        # 試験指示はコンテナ内のファイルだけで受ける。
        # 収集周期が変わっても複数の値が同じ周期に重ならないようにする。
        await wait_for_trigger("next-bad")
        await measure(1, 99.5, ua.StatusCodes.BadNoCommunication)
        await wait_for_trigger("next-good")
        await measure(2, 23.5)
        await wait_for_trigger("last-good")
        await measure(3, 24.5)
        await asyncio.Future()


async def check_read_only(directory):
    """同じ認証済みユーザーで読めることと、書込みを拒否することを試す。"""
    cert = directory / "client-cert.pem"
    key = directory / "client-key.pem"
    client = Client(f"opc.tcp://{HOST}:4840/foundation", timeout=5)
    client.application_uri = CLIENT_URI
    await client.set_security(
        SecurityPolicyBasic256Sha256,
        cert,
        key,
        server_certificate=str(directory / "server-cert.pem"),
    )
    await client.load_client_certificate(str(cert), extension="pem")
    await client.load_private_key(key, extension="pem")
    async with client:
        index = await client.get_namespace_index(SERVER_URI)
        point = client.get_node(ua.NodeId("Temperature", index))
        assert await point.read_value() == 22.5
        try:
            await point.write_value(91.25)
        except ua.UaStatusCodeError as error:
            assert error.code == ua.StatusCodes.BadUserAccessDenied
        else:
            raise AssertionError("読取ユーザーが模擬設備に書き込みました。")
        assert await point.read_value() == 22.5


def main():
    """試験機器の起動と、試験用証明書の作成を切り替える。"""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--certificates", type=Path)
    mode.add_argument("--check-read-only", type=Path)
    args = parser.parse_args()
    if args.certificates:
        asyncio.run(certificates(args.certificates))
    elif args.check_read_only:
        asyncio.run(check_read_only(args.check_read_only))
    else:
        asyncio.run(simulate())


if __name__ == "__main__":
    main()
