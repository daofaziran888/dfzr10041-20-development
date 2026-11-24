"""测试：字段停用影响表、解析试跑、撤回复算、批次决策与请求链复查。

覆盖业务场景：
1. 给 A、B、C 逐个送原始日志，停用前预览列出真实字段影响（可展开含原行）
2. 预览后补 A 的 trace_id 依赖记录；B 的规则试跑确认字段输出
3. B 用不带 trace_id 的新结果完成实际读取，再撤回并重新解析，同一原行恢复旧 trace_id
4. B 的历史样本仍带 trace_id，但新结果删掉该字段后仍可解析
5. C 无可验证解析规则，单独待核，不能发起试跑
6. A 没修好前不能领到新版；并发确认同份试跑只出一份结果
7. detect_schema_change 比较结果串联 apply_parse_rules 决定本批来源去留
8. 先试 B（独立模式，本轮无 trace_id）vs 整组等待（B 维持旧结果含 trace_id）
9. 已领用新版须先交回试跑资格才能回旧规则；历史试跑记录可复算
10. 请求链复查：A、B 同字段约定才可交付，禁止 B 新结果 + A 旧依赖混搭
"""
from __future__ import annotations

import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.field_deactivation import (
    ENTITLEMENT_ACTIVE,
    ENTITLEMENT_CONFIRMED,
    ENTITLEMENT_RETURNED,
    ELIGIBILITY_BLOCKED,
    ELIGIBILITY_PENDING,
    ELIGIBILITY_READY,
    FIELD_TRIAL_SOURCES,
    PRESET_RULE_VERSIONS,
    SOURCE_RAW_LINES,
    TRIAL_CONFIRMED,
    TRIAL_READY,
    TRIAL_WITHDRAWN,
    FieldDeactivationCoordinator,
)
from app.core.models import ParseResultStatus
from app.core.pipeline import IngestPipeline


TRACE_ID = "trace_id"


class TestDeactivationPreview:
    """停用前预览：字段停用影响表。"""

    def test_preview_lists_per_source_impact_and_raw_lines(self):
        """逐来源送原行：A/B 历史样本命中 trace_id，C 文本命中但无规则待核。"""
        coordinator = FieldDeactivationCoordinator()
        preview = coordinator.preview_deactivation(TRACE_ID)

        assert preview["field_name"] == TRACE_ID
        assert preview["total_event_count"] == 7  # A 三行 + B 两行 + C 两行
        by_source = {item["source_id"]: item for item in preview["sources"]}
        assert set(by_source) == {"SRC-A", "SRC-B", "SRC-C"}

        # 命中来源、缺失字段与真实原行都要给出（可展开预览）
        for source_id in ("SRC-A", "SRC-B", "SRC-C"):
            item = by_source[source_id]
            assert item["hit"] is True
            assert TRACE_ID in item["missing_fields"]
            assert item["raw_lines"] == SOURCE_RAW_LINES[source_id]
            assert item["event_count"] >= 2
        assert by_source["SRC-A"]["event_count"] == 3
        assert by_source["SRC-B"]["event_count"] == 2
        assert by_source["SRC-C"]["event_count"] == 2

        # A/B 有新旧规则版本；C 没有规则，资格待核
        assert by_source["SRC-A"]["rule_versions"] == ["PARS-A-V1", "PARS-A-V2"]
        assert by_source["SRC-B"]["rule_versions"] == ["PARS-B-V1", "PARS-B-V2"]
        assert by_source["SRC-C"]["rule_versions"] == []
        assert by_source["SRC-C"]["eligibility"] == ELIGIBILITY_PENDING

    def test_preview_field_not_hit_keeps_missing_empty(self):
        preview = FieldDeactivationCoordinator().preview_deactivation("not_exist_field")
        assert preview["total_event_count"] == 0
        assert preview["hit_sources"] == []
        assert all(item["missing_fields"] == [] for item in preview["sources"])


class TestDependencyAndTrialEligibility:
    """依赖补录与试跑资格。"""

    def test_record_a_dependency_blocks_new_version(self):
        """预览后补录 A 对 trace_id 的真实依赖，A 资格变为依赖阻断。"""
        coordinator = FieldDeactivationCoordinator()
        result = coordinator.record_dependency(
            "SRC-A", TRACE_ID, depends=True, note="下游链路拼接依赖")
        assert result["ok"] is True
        state = {item["source_id"]: item for item in coordinator.list_source_states()}
        assert state["SRC-A"]["eligibility"] == ELIGIBILITY_BLOCKED
        assert TRACE_ID in state["SRC-A"]["dependency_fields"]

        blocked = coordinator.start_trial("SRC-A", TRACE_ID)
        assert blocked["ok"] is False
        assert "不能领取新版试跑资格" in blocked["message"]

    def test_c_without_rule_stays_pending_and_cannot_trial(self):
        """来源 C 无可验证解析规则，单独待核，发起试跑直接拒绝。"""
        coordinator = FieldDeactivationCoordinator()
        result = coordinator.start_trial("SRC-C", TRACE_ID)
        assert result["ok"] is False
        assert "单独待核" in result["message"]
        state = {item["source_id"]: item for item in coordinator.list_source_states()}
        assert state["SRC-C"]["eligibility"] == ELIGIBILITY_PENDING

        dep = coordinator.record_dependency("SRC-C", TRACE_ID, depends=False)
        assert dep["ok"] is False  # 无规则来源不进入资格流转


class TestBTrialRun:
    """B 的新版规则试跑。"""

    def test_b_new_rule_drops_trace_id_but_still_parses(self):
        """B 历史样本带 trace_id，新结果删掉该字段后仍可解析。"""
        coordinator = FieldDeactivationCoordinator()
        result = coordinator.start_trial("SRC-B", TRACE_ID)
        assert result["ok"] is True
        trial = result["trial"]
        assert trial["rule_version"] == "PARS-B-V2"
        assert trial["status"] == TRIAL_READY

        for evidence in trial["evidence"]:
            assert evidence["parse_status"] == ParseResultStatus.OK.value
            assert TRACE_ID not in evidence["parsed_fields"]

        # 新版面形（无 trace_id 段）也能解析
        new_format_line = [e for e in trial["evidence"] if "对账拉取" in e["raw_line"]][0]
        assert new_format_line["parsed_fields"]["message"] == "对账拉取 batch=B-20261120"

        state = {item["source_id"]: item for item in coordinator.list_source_states()}
        assert state["SRC-B"]["entitlement"] == ENTITLEMENT_ACTIVE
        assert state["SRC-B"]["active_trial_id"] == trial["trial_id"]

    def test_b_trial_actual_read_returns_parsed_fields(self):
        """用 B 的不带 trace_id 新结果完成一次实际读取。"""
        coordinator = FieldDeactivationCoordinator()
        trial = coordinator.start_trial("SRC-B", TRACE_ID)["trial"]
        read = coordinator.read_trial_line(trial["trial_id"], line_number=3)
        assert read["ok"] is True
        assert TRACE_ID not in read["evidence"]["parsed_fields"]
        assert read["evidence"]["rule_version"] == "PARS-B-V2"


class TestWithdrawAndRestore:
    """撤回试跑并恢复旧字段解释。"""

    def test_withdraw_reparses_same_lines_and_restores_trace_id(self):
        """实际读取 → 撤回 → 重新解析，同一原行恢复旧 trace_id，而非改版本名。"""
        coordinator = FieldDeactivationCoordinator()
        trial = coordinator.start_trial("SRC-B", TRACE_ID)["trial"]
        coordinator.read_trial_line(trial["trial_id"], line_number=1)

        withdrawn = coordinator.withdraw_trial(trial["trial_id"])
        assert withdrawn["ok"] is True
        updated = withdrawn["trial"]
        assert updated["status"] == TRIAL_WITHDRAWN
        assert updated["restored_evidence"] is not None

        # 同一原行：旧规则重新解析后 trace_id 真实回到 parsed_fields
        restored_first = updated["restored_evidence"][0]
        assert restored_first["rule_version"] == "PARS-B-V1"
        assert restored_first["parsed_fields"][TRACE_ID] == "REQ-7788-0001"

        # 原行指纹一致、证据来自重新解析而非版本名回退
        assert updated["raw_fingerprint"]
        history_actions = [item["action"] for item in updated["history"]]
        assert "撤回试跑" in history_actions

        state = {item["source_id"]: item for item in coordinator.list_source_states()}
        assert state["SRC-B"]["entitlement"] == ENTITLEMENT_RETURNED
        assert state["SRC-B"]["active_trial_id"] is None

    def test_confirmed_new_must_return_entitlement_before_old_rule(self):
        """已按新版领用的来源，撤回时先交回资格再回旧规则。"""
        coordinator = FieldDeactivationCoordinator()
        trial = coordinator.start_trial("SRC-B", TRACE_ID)["trial"]
        confirm = coordinator.confirm_trial(trial["trial_id"])
        assert confirm["ok"] is True
        state = {item["source_id"]: item for item in coordinator.list_source_states()}
        assert state["SRC-B"]["entitlement"] == ENTITLEMENT_CONFIRMED
        assert state["SRC-B"]["confirmed_new"] is True

        withdrawn = coordinator.withdraw_trial(trial["trial_id"])
        assert withdrawn["ok"] is True
        assert "先交回新版领用资格" in withdrawn["message"]
        updated = withdrawn["trial"]
        assert updated["status"] == TRIAL_WITHDRAWN
        assert updated["restored_evidence"][0]["parsed_fields"][TRACE_ID] == "REQ-7788-0001"

        state = {item["source_id"]: item for item in coordinator.list_source_states()}
        assert state["SRC-B"]["entitlement"] == ENTITLEMENT_RETURNED
        assert state["SRC-B"]["confirmed_new"] is False

    def test_history_record_remains_recomputable_after_withdraw(self):
        """撤回后历史试跑记录仍可按旧/新版本复算。"""
        coordinator = FieldDeactivationCoordinator()
        trial = coordinator.start_trial("SRC-B", TRACE_ID)["trial"]
        coordinator.withdraw_trial(trial["trial_id"])

        old_re = coordinator.recompute_trial(trial["trial_id"], "PARS-B-V1")
        new_re = coordinator.recompute_trial(trial["trial_id"], "PARS-B-V2")
        assert old_re["ok"] is True and new_re["ok"] is True
        assert old_re["evidence"][0]["parsed_fields"][TRACE_ID] == "REQ-7788-0001"
        assert TRACE_ID not in new_re["evidence"][0]["parsed_fields"]
        # 复算不改变试跑状态
        assert coordinator.get_trial(trial["trial_id"])["status"] == TRIAL_WITHDRAWN


class TestConcurrentTrial:
    """并发与幂等。"""

    def test_concurrent_start_returns_single_trial_result(self):
        """并发确认同份试跑只出一份结果。"""
        coordinator = FieldDeactivationCoordinator()
        results: list[dict] = []

        def worker():
            results.append(coordinator.start_trial("SRC-B", TRACE_ID))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        trial_ids = {item["trial"]["trial_id"] for item in results if item.get("trial")}
        assert len(trial_ids) == 1
        trials = coordinator.list_trials("SRC-B")
        assert len(trials) == 1


class TestBatchDecision:
    """detect_schema_change 串联 apply_parse_rules 的批次决策。"""

    def test_independent_mode_b_switches_a_blocked_c_pending(self):
        """先试 B：B 切新版产出无 trace_id；A 依赖阻断维持旧规则；C 待核。"""
        coordinator = FieldDeactivationCoordinator()
        coordinator.record_dependency("SRC-A", TRACE_ID, depends=True, note="链路依赖")
        decision = coordinator.decide_batch(TRACE_ID, mode="independent")

        by_source = {item["source_id"]: item for item in decision["decisions"]}
        assert decision["continuable_sources"] == ["SRC-B"]

        b = by_source["SRC-B"]
        assert b["decision"] == "切换新版"
        assert b["effective_rule"] == "PARS-B-V2"
        assert b["output_contains_field"] is False
        assert b["schema_change"]["removed_fields"] == [TRACE_ID]
        # 新规则对 B 全部原行实际解析通过
        assert b["trial_parse_ok"] is True
        assert all(e["parse_status"] == ParseResultStatus.OK.value for e in b["evidence"])

        a = by_source["SRC-A"]
        assert a["decision"] == "维持旧规则"
        assert a["effective_rule"] == "PARS-A-V1"
        assert a["output_contains_field"] is True
        assert a["dependency_blocked"] is True

        c = by_source["SRC-C"]
        assert c["decision"] == "待核"

    def test_group_mode_keeps_b_on_old_result(self):
        """整组等待：A 未修好时 B 本轮也继续旧结果（含 trace_id）。"""
        coordinator = FieldDeactivationCoordinator()
        coordinator.record_dependency("SRC-A", TRACE_ID, depends=True)
        decision = coordinator.decide_batch(TRACE_ID, mode="group")

        assert decision["continuable_sources"] == []
        by_source = {item["source_id"]: item for item in decision["decisions"]}
        b = by_source["SRC-B"]
        assert b["decision"] == "整组等待·维持旧规则"
        assert b["effective_rule"] == "PARS-B-V1"
        assert b["output_contains_field"] is True

    def test_pipeline_evaluate_chains_detect_into_apply(self):
        """管道层：Schema 比较结果直接串联 apply_parse_rules 复验。"""
        pipeline = IngestPipeline()
        source = FIELD_TRIAL_SOURCES["SRC-B"]
        entries = pipeline.parse_raw_lines(source, SOURCE_RAW_LINES["SRC-B"])
        old_rule = PRESET_RULE_VERSIONS["SRC-B"]["PARS-B-V1"]
        new_rule = PRESET_RULE_VERSIONS["SRC-B"]["PARS-B-V2"]

        report = pipeline.evaluate_schema_change_impact(entries, old_rule, new_rule)
        assert report["schema_change"]["removed_fields"] == [TRACE_ID]
        assert report["parse_failed_count"] == 0
        assert report["can_continue"] is True
        assert all(TRACE_ID not in line["parsed_fields"] for line in report["line_results"])


class TestRequestChainReview:
    """值班复查：A、B 原日志合为一份请求链证据。"""

    def test_mixed_convention_rejected(self):
        """A 旧依赖 + B 新结果不能冒充一份完整新版证据。"""
        coordinator = FieldDeactivationCoordinator()
        coordinator.record_dependency("SRC-A", TRACE_ID, depends=True)
        result = coordinator.build_request_chain_evidence(
            TRACE_ID, ["SRC-A", "SRC-B"], convention="new")
        assert result["ok"] is False
        assert result["deliverable"] is False
        assert "冒充" in result["message"]

    def test_old_convention_delivers_when_both_recompute_old(self):
        """A 修好（解除依赖）后，两侧按旧版同一约定复算可交付，证据均含 trace_id。"""
        coordinator = FieldDeactivationCoordinator()
        result = coordinator.build_request_chain_evidence(
            TRACE_ID, ["SRC-A", "SRC-B"], convention="old")
        assert result["ok"] is True
        assert result["deliverable"] is True
        for item in result["per_source"]:
            assert item["field_present"] is True
            assert item["parse_ok"] is True
            assert item["evidence"]  # 只复算携带 trace_id 的请求链历史行
            assert all(
                TRACE_ID in evidence["parsed_fields"] for evidence in item["evidence"]
            )

    def test_new_convention_delivers_only_after_a_fixed(self):
        """A 解除依赖且其新规则实际能跑通后，新版请求链证据才交付。

        当前 A 的新规则未修好（对无 trace_id 原行解析失败），因此即使没补依赖记录，
        新版证据仍不可交付——资格由真实解析结果决定，而不是纸面状态。
        """
        coordinator = FieldDeactivationCoordinator()
        # 不补依赖，但 A 的新规则对新版面形仍解析失败
        result = coordinator.build_request_chain_evidence(
            TRACE_ID, ["SRC-A", "SRC-B"], convention="new")
        assert result["ok"] is False
        a_item = next(item for item in result["per_source"] if item["source_id"] == "SRC-A")
        assert a_item["parse_ok"] is False

    def test_chain_including_c_rejected(self):
        """C 无规则，纳入请求链复查时无法按同一约定复算。"""
        coordinator = FieldDeactivationCoordinator()
        result = coordinator.build_request_chain_evidence(
            TRACE_ID, ["SRC-B", "SRC-C"], convention="new")
        assert result["ok"] is False
        assert "待核" in result["message"] or "无可验证" in result["message"]


# ---------- 运行入口（确定性测试，不依赖 pytest） ----------

if __name__ == "__main__":
    import traceback

    test_classes = [
        TestDeactivationPreview,
        TestDependencyAndTrialEligibility,
        TestBTrialRun,
        TestWithdrawAndRestore,
        TestConcurrentTrial,
        TestBatchDecision,
        TestRequestChainReview,
    ]
    passed = 0
    failed = 0
    for cls in test_classes:
        for name in sorted(dir(cls)):
            if not name.startswith("test_"):
                continue
            try:
                getattr(cls(), name)()
                passed += 1
                print(f"  PASS  test_field_deactivation: {cls.__name__}.{name}")
            except Exception:
                failed += 1
                print(f"  FAIL  test_field_deactivation: {cls.__name__}.{name}")
                traceback.print_exc()

    print(f"\ntest_field_deactivation: {passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
