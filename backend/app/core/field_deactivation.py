"""字段停用影响域：停用前预览、解析试跑、撤回复算与批次切换决策。

业务链路：
1. 停用字段前，按来源逐个送历史原始日志，生成「字段停用影响表」（可展开预览），
   列出来源命中、停用后缺失字段、适用的新旧解析规则版本与试跑资格。
2. 解析验收从影响表领取真实原行与规则版本，走管道实际解析入口
   ``IngestPipeline.apply_parse_rules`` 重新运行，得到实际解析字段作为试跑证据。
3. ``detect_schema_change`` 的比较结果串联实际解析入口，决定本批哪些来源可继续。
4. 试跑通过后可用同一批原行撤回，按旧规则重新解析恢复旧字段解释；
   已领用新版的来源须先交回试跑资格才能回到旧规则，历史试跑记录保留且可复算。
5. 跨来源请求链复查要求两侧按同一字段约定复算，禁止新旧结果冒充一份完整证据。
"""
from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.core.models import (
    LogEntry, LogSourceConfig, ParseResultStatus, ParseRule,
)
from app.core.pipeline import IngestPipeline

# ---------- 预置来源：A/B/C 三个真实来源 ----------

FIELD_TRIAL_SOURCES: dict[str, LogSourceConfig] = {
    "SRC-A": LogSourceConfig(
        source_id="SRC-A",
        source_name="来源A·交易服务",
        app_label="trade-service",
        log_path="/var/log/trade/app.log",
        log_format="app",
        encoding="utf-8",
        collect_method="file_tail",
        access_state="已接入",
    ),
    "SRC-B": LogSourceConfig(
        source_id="SRC-B",
        source_name="来源B·支付服务",
        app_label="pay-service",
        log_path="/var/log/pay/app.log",
        log_format="app",
        encoding="utf-8",
        collect_method="file_tail",
        access_state="已接入",
    ),
    "SRC-C": LogSourceConfig(
        source_id="SRC-C",
        source_name="来源C·遗留系统",
        app_label="legacy-sys",
        log_path="/var/log/legacy/pipe.log",
        log_format="custom",
        encoding="utf-8",
        collect_method="file_tail",
        access_state="待验证",
    ),
}

# ---------- 预置历史原始日志（同一请求链 REQ-7788-0001 串起 A、B） ----------

SOURCE_RAW_LINES: dict[str, list[str]] = {
    "SRC-A": [
        # 历史样本：带 trace_id，A 的下游真实依赖该字段
        '2026-11-20 10:00:01.100 [trade-io-1] INFO  com.example.TradeService trace_id=REQ-7788-0001 - 订单创建 order=O-5001 amount=120.00',
        '2026-11-20 10:00:02.200 [trade-io-2] WARN  com.example.TradeService trace_id=REQ-7788-0001 - 库存重试 order=O-5001 retry=1',
        '2026-11-20 10:00:03.300 [trade-io-1] INFO  com.example.TradeService trace_id=REQ-7788-0002 - 订单关闭 order=O-5002',
        # 停用 trace_id 后的新版面形：A 的新规则尚未修好，应解析失败
        '2026-11-20 10:05:00.020 [trade-io-3] INFO  com.example.TradeService - 对账摘要 summary=S-99',
    ],
    "SRC-B": [
        # 历史样本：仍带 trace_id，但 B 的下游从不使用
        '2026-11-20 10:00:01.150 [pay-io-1] INFO  com.example.PayService trace_id=REQ-7788-0001 - 支付受理 order=O-5001 channel=wxpay',
        '2026-11-20 10:00:02.250 [pay-io-2] INFO  com.example.PayService trace_id=REQ-7788-0001 - 支付完成 order=O-5001 fee=0.60',
        # 新版面形：不再输出 trace_id，B 的新规则必须仍可解析
        '2026-11-20 10:05:00.010 [pay-io-3] INFO  com.example.PayService - 对账拉取 batch=B-20261120',
    ],
    "SRC-C": [
        # 遗留系统样本：历史上也出现 trace_id 文本，但没有可验证的解析规则
        'LEGACY|2026-11-20T10:00:01|node-c1|trace_id=REQ-7788-0001|job=snapshot',
        'LEGACY|2026-11-20T10:00:02|node-c2|trace_id=REQ-7788-0002|job=rotate',
        'LEGACY|2026-11-20T10:05:00|node-c1|job=cleanup',
    ],
}

# ---------- 预置解析规则版本 ----------

_APP_TIMESTAMP = r"(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3})"
_APP_HEAD = rf"^{_APP_TIMESTAMP}\s+\[([^\]]+)\]\s+(INFO|WARN|ERROR|DEBUG)\s+(\S+)\s+"

APP_OLD_FIELDS = ["timestamp", "thread", "level", "logger", "trace_id", "message"]
APP_NEW_FIELDS = ["timestamp", "thread", "level", "logger", "message"]

# 旧规则：显式捕获 trace_id
_APP_OLD_REGEX = _APP_HEAD + r"trace_id=(\S+)\s+-\s+(.*)$"
# B 的新规则：trace_id 段改为非捕获可选组，新版面形（无该段）与旧版面形都能解析，
# 解析结果里不再出现 trace_id 字段。
_APP_NEW_REGEX = _APP_HEAD + r"(?:trace_id=\S+\s+)?-\s+(.*)$"

PRESET_RULE_VERSIONS: dict[str, dict[str, ParseRule]] = {
    "SRC-A": {
        # A 的新规则尚未修好：仍强制要求 trace_id 段，遇到无 trace_id 的新原行即失败
        "PARS-A-V1": ParseRule(
            rule_id="PARS-A-V1", source_id="SRC-A",
            regex_pattern=_APP_OLD_REGEX, field_names=list(APP_OLD_FIELDS),
            time_format="%Y-%m-%d %H:%M:%S.%f", priority=10, state="生效中",
        ),
        "PARS-A-V2": ParseRule(
            rule_id="PARS-A-V2", source_id="SRC-A",
            regex_pattern=_APP_OLD_REGEX, field_names=list(APP_OLD_FIELDS),
            time_format="%Y-%m-%d %H:%M:%S.%f", priority=10, state="草稿",
        ),
    },
    "SRC-B": {
        "PARS-B-V1": ParseRule(
            rule_id="PARS-B-V1", source_id="SRC-B",
            regex_pattern=_APP_OLD_REGEX, field_names=list(APP_OLD_FIELDS),
            time_format="%Y-%m-%d %H:%M:%S.%f", priority=10, state="生效中",
        ),
        "PARS-B-V2": ParseRule(
            rule_id="PARS-B-V2", source_id="SRC-B",
            regex_pattern=_APP_NEW_REGEX, field_names=list(APP_NEW_FIELDS),
            time_format="%Y-%m-%d %H:%M:%S.%f", priority=10, state="草稿",
        ),
    },
    # SRC-C  intentionally 无规则版本：无可验证解析规则，单独待核
}

# 资格与试跑状态
ELIGIBILITY_BLOCKED = "依赖阻断"
ELIGIBILITY_PENDING = "待核"
ELIGIBILITY_READY = "可试跑"

TRIAL_READY = "试跑通过"
TRIAL_FAILED = "试跑失败"
TRIAL_WITHDRAWN = "已撤回"
TRIAL_CONFIRMED = "已领用新版"

ENTITLEMENT_NONE = "未发放"
ENTITLEMENT_ACTIVE = "试跑中"
ENTITLEMENT_CONFIRMED = "已领用新版"
ENTITLEMENT_RETURNED = "已交回"


@dataclass
class LineEvidence:
    """单行解析证据：原行、规则版本、实际解析字段与缺失字段。"""
    line_number: int
    raw_line: str
    rule_version: str
    parse_status: str
    parsed_fields: dict[str, str]
    missing_fields: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "line_number": self.line_number,
            "raw_line": self.raw_line,
            "rule_version": self.rule_version,
            "parse_status": self.parse_status,
            "parsed_fields": dict(self.parsed_fields),
            "missing_fields": list(self.missing_fields),
        }


@dataclass
class SourceDeactivationState:
    """单来源在一次字段停用中的状态：依赖记录、规则版本与试跑资格。"""
    source_id: str
    source_name: str
    eligibility: str
    dependency_fields: list[str] = field(default_factory=list)
    dependency_notes: list[str] = field(default_factory=list)
    rule_versions: dict[str, ParseRule] = field(default_factory=dict)
    active_trial_id: str | None = None
    entitlement: str = ENTITLEMENT_NONE
    confirmed_new: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "eligibility": self.eligibility,
            "dependency_fields": list(self.dependency_fields),
            "dependency_notes": list(self.dependency_notes),
            "rule_versions": sorted(self.rule_versions),
            "active_trial_id": self.active_trial_id,
            "entitlement": self.entitlement,
            "confirmed_new": self.confirmed_new,
        }


@dataclass
class FieldTrial:
    """一次字段停用试跑：绑定来源、字段、规则版本与同一批原行。"""
    trial_id: str
    source_id: str
    field_name: str
    rule_version: str
    raw_fingerprint: str
    raw_lines: list[str]
    started_at: str
    status: str
    evidence: list[LineEvidence]
    history: list[dict[str, Any]] = field(default_factory=list)
    restored_evidence: list[LineEvidence] | None = None
    restored_at: str | None = None
    confirmed_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "trial_id": self.trial_id,
            "source_id": self.source_id,
            "field_name": self.field_name,
            "rule_version": self.rule_version,
            "raw_fingerprint": self.raw_fingerprint,
            "raw_lines": list(self.raw_lines),
            "started_at": self.started_at,
            "status": self.status,
            "evidence": [item.to_dict() for item in self.evidence],
            "restored_evidence": None
            if self.restored_evidence is None
            else [item.to_dict() for item in self.restored_evidence],
            "restored_at": self.restored_at,
            "confirmed_at": self.confirmed_at,
            "history": list(self.history),
        }


def fingerprint_lines(raw_lines: list[str]) -> str:
    """对同一批原行计算指纹，撤回时校验确实是同一批原行。"""
    return hashlib.sha256("\n".join(raw_lines).encode("utf-8")).hexdigest()[:16]


class FieldDeactivationCoordinator:
    """字段停用编排器：预览、试跑、撤回、批次决策与请求链复查。

    所有写操作在同一把锁内完成，保证同一份试跑并发确认只产出一份结果。
    """

    def __init__(self, pipeline: IngestPipeline | None = None) -> None:
        self._lock = threading.RLock()
        self.pipeline = pipeline or IngestPipeline()
        self._states: dict[str, SourceDeactivationState] = {}
        self._trials: dict[str, FieldTrial] = {}
        self._seq = 0
        for source_id, source in FIELD_TRIAL_SOURCES.items():
            rules = PRESET_RULE_VERSIONS.get(source_id, {})
            if not rules:
                eligibility = ELIGIBILITY_PENDING
            else:
                eligibility = ELIGIBILITY_READY
            self._states[source_id] = SourceDeactivationState(
                source_id=source_id,
                source_name=source.source_name,
                eligibility=eligibility,
                rule_versions=dict(rules),
            )

    # ---------- 基础查询 ----------

    def list_source_states(self) -> list[dict[str, Any]]:
        with self._lock:
            return [state.to_dict() for state in self._states.values()]

    def list_trials(self, source_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            trials = self._trials.values()
            if source_id:
                trials = [t for t in trials if t.source_id == source_id]
            return [trial.to_dict() for trial in trials]

    def get_trial(self, trial_id: str) -> dict[str, Any] | None:
        with self._lock:
            trial = self._trials.get(trial_id)
            return trial.to_dict() if trial else None

    # ---------- 停用前预览：字段停用影响表 ----------

    def preview_deactivation(self, field_name: str) -> dict[str, Any]:
        """逐来源送历史原行，产出可展开的停用前预览（影响表）。

        每个来源给出：是否命中该字段、命中事件数、停用后缺失字段、
        可适用的新旧规则版本、当前试跑资格与依赖记录。
        """
        with self._lock:
            impact_sources: list[dict[str, Any]] = []
            total_events = 0
            for source_id, raw_lines in SOURCE_RAW_LINES.items():
                state = self._states[source_id]
                hit_lines = [line for line in raw_lines if field_name in line]
                hit = bool(hit_lines)
                total_events += len(hit_lines)
                rule_versions = sorted(state.rule_versions)
                if not rule_versions:
                    note = "无可验证的解析规则，单独待核"
                elif field_name in state.dependency_fields:
                    note = f"下游真实依赖字段 {field_name}，停用前须先消除依赖"
                else:
                    note = "可领取原行与规则版本进行新版试跑"
                impact_sources.append({
                    "source_id": source_id,
                    "source_name": state.source_name,
                    "hit": hit,
                    "event_count": len(hit_lines),
                    "sample_line_count": len(raw_lines),
                    "missing_fields": [field_name] if hit else [],
                    "old_rule_version": self._old_version(state),
                    "new_rule_version": self._new_version(state),
                    "rule_versions": rule_versions,
                    "eligibility": state.eligibility,
                    "entitlement": state.entitlement,
                    "dependency_fields": list(state.dependency_fields),
                    "dependency_notes": list(state.dependency_notes),
                    "active_trial_id": state.active_trial_id,
                    "note": note,
                    "raw_lines": list(raw_lines),
                })
            return {
                "field_name": field_name,
                "total_event_count": total_events,
                "hit_sources": [item["source_id"] for item in impact_sources if item["hit"]],
                "generated_at": datetime.now().isoformat(),
                "sources": impact_sources,
            }

    # ---------- 依赖记录（预览后复核补录） ----------

    def record_dependency(self, source_id: str, field_name: str,
                          depends: bool, note: str = "") -> dict[str, Any]:
        """补录某来源下游对停用字段的真实依赖，并刷新试跑资格。"""
        with self._lock:
            state = self._require_state(source_id)
            if not state.rule_versions:
                return {
                    "ok": False,
                    "message": f"{state.source_name} 无可验证解析规则，依赖记录保留为待核",
                }
            if depends and field_name not in state.dependency_fields:
                state.dependency_fields.append(field_name)
            if not depends and field_name in state.dependency_fields:
                state.dependency_fields.remove(field_name)
            state.dependency_notes.append(
                f"{datetime.now():%H:%M:%S} 复核记录：{field_name} 依赖={depends} {note}".strip()
            )
            state.eligibility = (
                ELIGIBILITY_BLOCKED if state.dependency_fields else ELIGIBILITY_READY
            )
            return {
                "ok": True,
                "message": f"{state.source_name} 依赖记录已更新，当前资格：{state.eligibility}",
                "state": state.to_dict(),
            }

    # ---------- 解析试跑：领取原行 + 规则版本，走真实解析入口 ----------

    def start_trial(self, source_id: str, field_name: str) -> dict[str, Any]:
        """发起新版试跑；同来源同字段的并发请求只允许一份试跑结果。"""
        with self._lock:
            state = self._require_state(source_id)
            if not state.rule_versions:
                return {"ok": False, "message": f"{state.source_name} 无可验证的解析规则，应单独待核，不能发起试跑"}
            if field_name in state.dependency_fields or state.eligibility == ELIGIBILITY_BLOCKED:
                return {
                    "ok": False,
                    "message": (
                        f"{state.source_name} 旧规则仍真实依赖 {field_name}，"
                        "依赖未消除前不能领取新版试跑资格"
                    ),
                }
            # 幂等：已有活动试跑时直接返回同一份结果，不重复落单
            if state.active_trial_id:
                existing = self._trials[state.active_trial_id]
                return {
                    "ok": True,
                    "message": "同份试跑已存在，返回唯一试跑结果",
                    "trial": existing.to_dict(),
                }

            new_version = self._new_version(state)
            rule = state.rule_versions[new_version]
            raw_lines = list(SOURCE_RAW_LINES[source_id])
            evidence = self._run_parse(source_id, raw_lines, rule, new_version)
            failed = [item for item in evidence if item.parse_status != ParseResultStatus.OK.value]
            self._seq += 1
            trial = FieldTrial(
                trial_id=f"TRIAL-{self._seq:04d}",
                source_id=source_id,
                field_name=field_name,
                rule_version=new_version,
                raw_fingerprint=fingerprint_lines(raw_lines),
                raw_lines=raw_lines,
                started_at=datetime.now().isoformat(),
                status=TRIAL_FAILED if failed else TRIAL_READY,
                evidence=evidence,
            )
            trial.history.append({
                "at": trial.started_at,
                "action": "发起试跑",
                "rule_version": new_version,
                "result": trial.status,
            })
            self._trials[trial.trial_id] = trial
            if failed:
                # 规则跑不通同样保留证据，但不发放试跑资格
                state.eligibility = ELIGIBILITY_BLOCKED
                return {
                    "ok": False,
                    "message": (
                        f"{state.source_name} 新规则 {new_version} 试跑失败："
                        f"{len(failed)} 行原行无法解析，修好前不能领到新版"
                    ),
                    "trial": trial.to_dict(),
                }
            state.active_trial_id = trial.trial_id
            state.entitlement = ENTITLEMENT_ACTIVE
            return {"ok": True, "message": "新版试跑通过，试跑资格已发放", "trial": trial.to_dict()}

    def confirm_trial(self, trial_id: str) -> dict[str, Any]:
        """确认领用新版：试跑资格转为已领用。"""
        with self._lock:
            trial = self._require_trial(trial_id)
            state = self._states[trial.source_id]
            if trial.status != TRIAL_READY:
                return {"ok": False, "message": f"试跑 {trial_id} 当前状态 {trial.status}，不能领用新版"}
            trial.status = TRIAL_CONFIRMED
            trial.confirmed_at = datetime.now().isoformat()
            state.confirmed_new = True
            state.entitlement = ENTITLEMENT_CONFIRMED
            trial.history.append({
                "at": trial.confirmed_at, "action": "领用新版", "rule_version": trial.rule_version,
            })
            return {"ok": True, "message": f"{state.source_name} 已按新版领用", "trial": trial.to_dict()}

    def withdraw_trial(self, trial_id: str) -> dict[str, Any]:
        """用同一批原行撤回试跑：按旧规则重新解析，恢复旧字段解释。

        已领用新版的来源在此一步先交回试跑资格，再回到旧规则；
        试跑记录保留，之后仍可按历史记录复算。
        """
        with self._lock:
            trial = self._require_trial(trial_id)
            state = self._states[trial.source_id]
            old_version = self._old_version(state)
            if old_version is None:
                return {"ok": False, "message": f"{state.source_name} 无旧规则版本，无法恢复旧字段解释"}
            current_fingerprint = fingerprint_lines(trial.raw_lines)
            if current_fingerprint != trial.raw_fingerprint:
                return {"ok": False, "message": "撤回失败：当前原行与试跑留存原行不是同一批"}

            released = state.confirmed_new
            # 真实重新解析，而不是只把页面版本名改回去
            restored = self._run_parse(
                trial.source_id, trial.raw_lines, state.rule_versions[old_version], old_version)
            trial.restored_evidence = restored
            trial.restored_at = datetime.now().isoformat()
            trial.status = TRIAL_WITHDRAWN
            state.active_trial_id = None
            state.confirmed_new = False
            state.entitlement = ENTITLEMENT_RETURNED
            if state.eligibility == ELIGIBILITY_BLOCKED and not state.dependency_fields:
                state.eligibility = ELIGIBILITY_READY
            trial.history.append({
                "at": trial.restored_at,
                "action": "交回资格并撤回试跑" if released else "撤回试跑",
                "rule_version": old_version,
                "released_new_entitlement": released,
            })
            return {
                "ok": True,
                "message": (
                    f"已按旧规则 {old_version} 重新解析同一批原行并恢复旧字段解释"
                    + ("（先交回新版领用资格）" if released else "")
                ),
                "trial": trial.to_dict(),
            }

    def recompute_trial(self, trial_id: str, rule_version: str) -> dict[str, Any]:
        """按历史试跑记录复算：不改变试跑状态，只重新跑一遍指定规则版本。"""
        with self._lock:
            trial = self._require_trial(trial_id)
            state = self._states[trial.source_id]
            rule = state.rule_versions.get(rule_version)
            if rule is None:
                return {"ok": False, "message": f"{state.source_name} 不存在规则版本 {rule_version}"}
            evidence = self._run_parse(trial.source_id, trial.raw_lines, rule, rule_version)
            trial.history.append({
                "at": datetime.now().isoformat(),
                "action": "历史记录复算",
                "rule_version": rule_version,
            })
            return {
                "ok": True,
                "message": f"已按 {rule_version} 复算同一批原行",
                "trial_id": trial.trial_id,
                "rule_version": rule_version,
                "evidence": [item.to_dict() for item in evidence],
            }

    def read_trial_line(self, trial_id: str, line_number: int) -> dict[str, Any]:
        """用试跑结果完成一次实际读取，返回该行的实际解析字段。"""
        with self._lock:
            trial = self._require_trial(trial_id)
            for item in trial.evidence:
                if item.line_number == line_number:
                    return {"ok": True, "trial_id": trial_id, "evidence": item.to_dict()}
            return {"ok": False, "message": f"试跑 {trial_id} 中不存在第 {line_number} 行"}

    # ---------- 批次决策：detect_schema_change 串联 apply_parse_rules ----------

    def decide_batch(self, field_name: str, mode: str = "independent") -> dict[str, Any]:
        """比较新旧 Schema 并串联真实解析，决定本批各来源能否继续。

        mode=independent：能切的来源先切（先试 B，本轮产出不带 trace_id）；
        mode=group：整组等待，任一来源未就绪则全部维持旧结果。
        """
        with self._lock:
            decisions: list[dict[str, Any]] = []
            any_blocked = False
            for source_id, state in self._states.items():
                old_version = self._old_version(state)
                new_version = self._new_version(state)
                if not state.rule_versions or old_version is None or new_version is None:
                    any_blocked = True
                    decisions.append({
                        "source_id": source_id,
                        "source_name": state.source_name,
                        "schema_change": None,
                        "decision": "待核",
                        "reason": "无可验证解析规则",
                        "effective_rule": None,
                        "output_contains_field": None,
                    })
                    continue
                old_rule = state.rule_versions[old_version]
                new_rule = state.rule_versions[new_version]
                # 第一步：Schema 比较
                change = self.pipeline.detect_schema_change(
                    old_rule.field_names, new_rule.field_names)
                # 第二步：比较结果串联实际解析入口 apply_parse_rules 复验
                evidence = self._run_parse(
                    source_id, list(SOURCE_RAW_LINES[source_id]), new_rule, new_version)
                parse_ok = all(
                    item.parse_status == ParseResultStatus.OK.value for item in evidence)
                dependency_blocked = field_name in state.dependency_fields
                can_continue = parse_ok and not dependency_blocked
                if not can_continue:
                    any_blocked = True
                decisions.append({
                    "source_id": source_id,
                    "source_name": state.source_name,
                    "schema_change": change,
                    "trial_parse_ok": parse_ok,
                    "dependency_blocked": dependency_blocked,
                    "decision_raw": "可切换新版" if can_continue else "维持旧规则",
                    "reason": self._decision_reason(parse_ok, dependency_blocked, field_name),
                    "new_rule_version": new_version,
                    "old_rule_version": old_version,
                    "evidence": [item.to_dict() for item in evidence],
                })

            if mode == "group" and any_blocked:
                # 整组等待：本轮所有来源继续旧结果，B 也不例外
                for item in decisions:
                    if item.get("decision_raw") == "可切换新版":
                        item["decision"] = "整组等待·维持旧规则"
                        item["effective_rule"] = item["old_rule_version"]
                        item["output_contains_field"] = True
                    elif self._states[item["source_id"]].rule_versions:
                        item["decision"] = "维持旧规则"
                        item["effective_rule"] = item["old_rule_version"]
                        item["output_contains_field"] = True
                    else:
                        item["decision"] = "待核"
            else:
                for item in decisions:
                    if item.get("decision_raw") == "可切换新版":
                        item["decision"] = "切换新版"
                        item["effective_rule"] = item["new_rule_version"]
                        item["output_contains_field"] = False
                    elif item["source_id"] in self._states and self._states[item["source_id"]].rule_versions:
                        item["decision"] = "维持旧规则"
                        item["effective_rule"] = item["old_rule_version"]
                        item["output_contains_field"] = True
            return {
                "field_name": field_name,
                "mode": mode,
                "decided_at": datetime.now().isoformat(),
                "continuable_sources": [
                    item["source_id"] for item in decisions if item["decision"] == "切换新版"
                ],
                "decisions": decisions,
            }

    # ---------- 请求链复查：跨来源证据必须同一字段约定 ----------

    def build_request_chain_evidence(self, field_name: str, source_ids: list[str],
                                     convention: str = "new") -> dict[str, Any]:
        """把多个来源的原日志合为一份请求链证据。

        只有两侧都能按同一字段约定（new=无停用字段 / old=保留停用字段）复算才交付；
        新旧混搭一律拒绝，不能把 B 的新结果与 A 的旧依赖冒充完整新版证据。
        """
        with self._lock:
            if convention not in ("new", "old"):
                return {"ok": False, "message": "字段约定只支持 new（新版无该字段）或 old（旧版保留该字段）"}
            missing = [sid for sid in source_ids if sid not in self._states]
            if missing:
                return {"ok": False, "message": f"未知来源：{'、'.join(missing)}"}
            per_source: list[dict[str, Any]] = []
            for source_id in source_ids:
                state = self._states[source_id]
                version = self._new_version(state) if convention == "new" else self._old_version(state)
                if not state.rule_versions or version is None:
                    return {
                        "ok": False,
                        "deliverable": False,
                        "message": f"{state.source_name} 无可验证的 {convention} 版解析规则，无法按同一约定复算",
                    }
                rule = state.rule_versions[version]
                all_lines = list(SOURCE_RAW_LINES[source_id])
                # 旧约定只复算携带该字段的请求链历史行；新约定须覆盖全部原行
                # （含去掉该字段后的新版面形），证明新规则两类面形都能解析。
                candidate_lines = (
                    [line for line in all_lines if field_name in line]
                    if convention == "old" else all_lines
                )
                evidence = self._run_parse(source_id, candidate_lines, rule, version)
                parse_ok = all(
                    item.parse_status == ParseResultStatus.OK.value for item in evidence)
                field_present = any(
                    field_name in item.parsed_fields for item in evidence)
                per_source.append({
                    "source_id": source_id,
                    "source_name": state.source_name,
                    "rule_version": version,
                    "parse_ok": parse_ok,
                    "field_present": field_present,
                    "evidence": [item.to_dict() for item in evidence],
                })
            if convention == "new":
                failed = [item["source_name"] for item in per_source if not item["parse_ok"]]
                if failed:
                    return {
                        "ok": False,
                        "deliverable": False,
                        "message": (
                            "新版请求链证据不可交付："
                            f"{'、'.join(failed)} 无法按新版复算；"
                            "禁止把 B 的新结果与 A 的旧依赖冒充一份完整新版证据"
                        ),
                        "per_source": per_source,
                    }
                leaked = [item["source_name"] for item in per_source if item["field_present"]]
                if leaked:
                    return {
                        "ok": False,
                        "deliverable": False,
                        "message": f"新版证据仍含停用字段 {field_name}：{'、'.join(leaked)}",
                        "per_source": per_source,
                    }
            else:
                missing_field = [
                    item["source_name"] for item in per_source
                    if not item["field_present"]
                ]
                if missing_field:
                    return {
                        "ok": False,
                        "deliverable": False,
                        "message": f"旧版请求链证据缺字段 {field_name}：{'、'.join(missing_field)}",
                        "per_source": per_source,
                    }
            return {
                "ok": True,
                "deliverable": True,
                "convention": convention,
                "message": f"两侧均按 {convention} 版同一字段约定复算通过，请求链证据可交付",
                "field_name": field_name,
                "per_source": per_source,
            }

    # ---------- 内部辅助 ----------

    def _run_parse(self, source_id: str, raw_lines: list[str],
                   rule: ParseRule, rule_version: str) -> list[LineEvidence]:
        """走管道真实入口：parse_raw_lines → apply_parse_rules。"""
        source = FIELD_TRIAL_SOURCES[source_id]
        entries: list[LogEntry] = self.pipeline.parse_raw_lines(source, list(raw_lines))
        self.pipeline.apply_parse_rules(entries, rule)
        evidence: list[LineEvidence] = []
        for entry, raw in zip(entries, [line for line in raw_lines if line.strip()]):
            parsed = dict(entry.parsed_fields or {})
            missing = [name for name in rule.field_names if name not in parsed]
            evidence.append(LineEvidence(
                line_number=entry.line_number,
                raw_line=entry.raw_line,
                rule_version=rule_version,
                parse_status=entry.parse_status.value,
                parsed_fields=parsed,
                missing_fields=missing,
            ))
        return evidence

    def _require_state(self, source_id: str) -> SourceDeactivationState:
        state = self._states.get(source_id)
        if state is None:
            raise KeyError(f"未知来源：{source_id}")
        return state

    def _require_trial(self, trial_id: str) -> FieldTrial:
        trial = self._trials.get(trial_id)
        if trial is None:
            raise KeyError(f"试跑 {trial_id} 不存在")
        return trial

    @staticmethod
    def _old_version(state: SourceDeactivationState) -> str | None:
        versions = sorted(state.rule_versions)
        return versions[0] if versions else None

    @staticmethod
    def _new_version(state: SourceDeactivationState) -> str | None:
        versions = sorted(state.rule_versions)
        return versions[-1] if versions else None

    @staticmethod
    def _decision_reason(parse_ok: bool, dependency_blocked: bool, field_name: str) -> str:
        reasons: list[str] = []
        if dependency_blocked:
            reasons.append(f"下游真实依赖 {field_name}")
        if not parse_ok:
            reasons.append("新规则对本来源原行实际解析失败")
        return "；".join(reasons) if reasons else "新规则实际解析通过且无字段依赖"


# 单例编排器，路由层共享同一份试跑状态
coordinator = FieldDeactivationCoordinator()
