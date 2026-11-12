#!/usr/bin/env python3
"""制造交换 XML 采集适配的端到端演练。

用独立临时 SQLite 数据库运行，合成文件位于 demo_support/mfg_drill 与
demo_support/mfg_maps，脚本以绝对路径引用输入，解释器以隔离模式运行，
不依赖当前工作目录。覆盖：
1. 跨文件前序（含同文件后向批次引用）的等待与自动重试；
2. 带单位精确十进制的换算（g -> kg，多投入多产出）；
3. 成员无法转换时整事务拒收，材料图不变；
4. 数量不守恒时整事务拒收，不补损耗；
5. 未知业务代码作为可修复问题保留，补齐对照表后重处理；
6. 重复文件幂等、同号异内容隔离；
7. 外部实体、未声明命名空间、未知结构明确硬拒收；
8. 已接受事务的解释不被新映射修订改变。
"""

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from chainledger.service import SupplyChain  # noqa: E402
from chainledger.types import ChainError  # noqa: E402

DRILL = ROOT / "demo_support" / "mfg_drill"
MAPS = ROOT / "demo_support" / "mfg_maps"


def expect_chain_error(label, fn, code=None):
    try:
        fn()
    except ChainError as exc:
        if code is not None and exc.code != code:
            raise AssertionError(f"{label}: 期望错误码 {code}，实际 {exc.code}") from exc
        print(f"  - {label}: 硬拒收 code={exc.code}")
        return exc
    raise AssertionError(f"{label}: 应当被拒收但没有")


def main():
    with tempfile.TemporaryDirectory() as directory:
        db = Path(directory) / "drill.sqlite3"
        with SupplyChain(db) as app:
            print("== 1. 加载对照表 r1 ==")
            loaded = app.manufacturing.load_mapping_file(MAPS / "codes-r1.json")
            assert loaded["revision"] == 1 and not loaded["existing"]
            again = app.manufacturing.load_mapping_file(MAPS / "codes-r1.json")
            assert again["existing"]

            print("== 2. 前序未到，先采集混料事务（跨文件前序 + 后向批次引用）==")
            first = app.manufacturing.ingest_xml(DRILL / "02_transform_forward.xml")
            assert first["received"] == 1, first
            tx = app.manufacturing.transaction("mill", "tx-mix")
            assert tx["adapter_state"] == "submitted" and tx["state"] == "pending", tx
            assert tx["xml_location"].endswith("@line3"), tx["xml_location"]
            assert tx["mapping"]["id"] == "plant-codes"
            assert tx["mapping"]["revision"] == 1
            unresolved = app.manufacturing.unresolved()
            assert [item["external_id"] for item in unresolved] == ["tx-mix"]
            print(f"  - tx-mix 已采集但无法应用（前序序号未到），XML 位置={tx['xml_location']}")
            assert app.inventory.list()["items"] == []

            print("== 3. 采集前序收货，原消费链自动重试 ==")
            received = app.manufacturing.ingest_xml(DRILL / "01_receive.xml")
            assert received["drain"]["applied"] == [
                "mfg:mill:rx-1", "mfg:mill:rx-2", "mfg:mill:tx-mix"
            ]
            assert app.manufacturing.transaction("mill", "tx-mix")["state"] == "applied"
            assert app.manufacturing.unresolved() == []
            # 单位换算核对：5000 g -> 5 kg；10 + 5 = 12 + 2.5 + 0.5
            assert app.graph.stock("mix-a") == 12
            assert app.graph.stock("mix-b") == 2.5
            assert app.graph.stock("raw") == 90, app.graph.stock("raw")
            assert app.graph.stock("raw-2") == 5
            assert app.graph.balance("raw", "dock-a") == 90
            assert app.trace.path("raw", "mix-b") == ["raw", "mix-b"]
            assert app.trace.path("raw-2", "mix-a") == ["raw-2", "mix-a"]
            print("  - g->kg 换算、多投入多产出、谱系 raw/raw-2 -> mix-a/mix-b 正确")

            print("== 4. 成员无法转换：整事务拒收，材料图不变 ==")
            bad = app.manufacturing.ingest_xml(DRILL / "03_reject_member.xml")
            assert bad["received"] == 0 and bad["blocked"] == 1, bad
            held = app.manufacturing.transaction("mill", "tx-bad-member")
            assert held["adapter_state"] == "blocked"
            assert held["reason"]["code"] == "unknown_location", held["reason"]
            assert held["reason"]["code"] != "conservation"
            # 没有任何成员落地：无 bad-child 批次、无转换操作、raw 库存不变
            try:
                app.graph.lot("bad-child")
                raise AssertionError("bad-child 不应被创建")
            except ChainError as exc:
                assert exc.code == "dependency"
            assert app.graph.stock("raw") == 90
            assert app.journal.one(
                "SELECT COUNT(*) AS n FROM operations WHERE envelope='mfg:mill:tx-bad-member'"
            )["n"] == 0
            print("  - 未知位置代码 NEVER-MAPPED 保留为可修复问题，无任何成员提交")

            print("== 5. 数量不守恒：整事务拒收，损耗不被补平 ==")
            unbalanced = app.manufacturing.ingest_xml(DRILL / "04_unbalanced.xml")
            assert unbalanced["received"] == 1
            ub = app.manufacturing.transaction("mill", "tx-unbalanced")
            # 前一事务停在适配器层形成序号空档，信封先 pending，尚未作用于材料图
            assert ub["adapter_state"] == "submitted" and ub["state"] == "pending"
            try:
                app.graph.lot("unbalanced-child")
                raise AssertionError("unbalanced-child 不应被创建")
            except ChainError as exc:
                assert exc.code == "dependency"
            assert app.graph.stock("raw") == 90
            print("  - 不守恒事务尚未应用；待队首释放后由原消费链整事务拒收")

            print("== 6. 安全与结构硬拒收（无库写入）==")
            expect_chain_error("外部实体/DOCTYPE",
                               lambda: app.manufacturing.ingest_xml(DRILL / "05_xxe.xml"),
                               "xml")
            expect_chain_error("未声明命名空间 v3",
                               lambda: app.manufacturing.ingest_xml(DRILL / "06_unknown_namespace.xml"),
                               "xml")
            expect_chain_error("未知元素",
                               lambda: app.manufacturing.ingest_xml(DRILL / "07_unknown_element.xml"),
                               "xml")
            files_before = app.journal.one("SELECT COUNT(*) AS n FROM mfg_files")["n"]
            assert files_before == 4  # 01/02/03/04 四个文件摘要；硬拒收的无记录

            print("== 7. 重复文件幂等；同号异内容隔离 ==")
            repeat = app.manufacturing.ingest_xml(DRILL / "01_receive.xml")
            assert repeat["duplicate_file"] and repeat["received"] == 0
            assert app.graph.stock("raw") == 90
            mutated = Path(directory) / "01_receive_mutated.xml"
            mutated.write_text(
                (DRILL / "01_receive.xml").read_text().replace(
                    "<Quantity unit=\"kg\">100</Quantity>",
                    "<Quantity unit=\"kg\">101</Quantity>"
                ),
                encoding="utf-8",
            )
            conflict = app.manufacturing.ingest_xml(mutated)
            assert conflict["quarantined"] == 1 and conflict["received"] == 0
            quarantine = app.manufacturing.quarantine()
            assert len(quarantine) == 1
            assert quarantine[0]["external_id"] == "rx-1"
            assert quarantine[0]["reason"]["code"] == "content_conflict"
            assert app.graph.stock("raw") == 90
            print("  - 重复文件不改变材料图；rx-1 的异内容版本已隔离")

            print("== 8. 补齐对照表后重处理选定事务，释放队首并触发守恒拒收 ==")
            upgraded = app.manufacturing.load_mapping_file(MAPS / "codes-r2.json")
            assert upgraded["revision"] == 2 and not upgraded["existing"]
            # 已接受事务仍钉在 r1，解释未被新映射偷偷改变
            pinned = app.manufacturing.transaction("mill", "tx-mix")
            assert pinned["mapping"]["revision"] == 1
            fixed = app.manufacturing.reprocess("mill", "tx-bad-member")
            assert fixed["transaction"]["state"] == "applied"
            assert fixed["transaction"]["mapping"]["revision"] == 2
            assert app.graph.stock("bad-child") == 3.5
            assert app.graph.stock("raw") == 86
            # 队首释放后，不守恒事务进入原消费链并被整事务拒收
            ub = app.manufacturing.transaction("mill", "tx-unbalanced")
            assert ub["state"] == "rejected"
            assert ub["reason"]["code"] == "conservation", ub["reason"]
            assert ub["reason"]["details"]["waste"] == "1"
            try:
                app.graph.lot("unbalanced-child")
                raise AssertionError("unbalanced-child 不应被创建")
            except ChainError as exc:
                assert exc.code == "dependency"
            assert app.graph.stock("raw") == 86
            print("  - tx-bad-member 用 r2 重处理成功；已接受的 tx-mix 仍为 r1")
            print("  - 8 != 8 + 1 被整事务拒收，损耗保持 1 kg 未被修平")

            print("== 9. 全量核对：审计链、守恒与谱系 ==")
            check = app.check()
            assert check["sqlite"] == "ok"
            assert check["conservation"]["conserved"]
            materials = check["conservation"]["materials"]
            row = next(item for item in materials if item["material"] == "alloy")
            assert row["difference"] == "0"
            topology = check["topology"]
            assert topology["maximum_depth"] == 1
            assert set(app.trace.topology()["order"]) >= {
                "raw", "raw-2", "mix-a", "mix-b", "bad-child"
            }
            # 守恒数字核对：110 收货 = 库存 109 + 损耗 1（拒收事务不入账、不补平）
            assert row["received"] == "110"
            assert row["waste"] == "1"
            assert row["stock"] == "109"
            assert app.journal.verify_audit()["entries"] > 0
            print(f"  - 守恒：received={row['received']} waste={row['waste']} "
                  f"stock={row['stock']} difference={row['difference']}")
            print("  - 审计链完整，谱系无环，SQLite integrity ok")

    print("制造交换采集演练全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
