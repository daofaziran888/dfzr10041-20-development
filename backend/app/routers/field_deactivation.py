"""字段停用工作流路由：停用前预览、解析试跑、撤回复算与批次决策。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.core.field_deactivation import coordinator

router = APIRouter(prefix="/api/field-deactivation", tags=["字段停用"])


class FieldPayload(BaseModel):
    field_name: str = Field(default="trace_id")


class DependencyPayload(BaseModel):
    field_name: str = Field(default="trace_id")
    depends: bool = True
    note: str = ""


class TrialStartPayload(BaseModel):
    field_name: str = Field(default="trace_id")


class TrialIdPayload(BaseModel):
    trial_id: str


class RecomputePayload(BaseModel):
    trial_id: str
    rule_version: str


class BatchDecisionPayload(BaseModel):
    field_name: str = Field(default="trace_id")
    mode: str = Field(default="independent", description="independent=能切先切；group=整组等待")


class ChainEvidencePayload(BaseModel):
    field_name: str = Field(default="trace_id")
    source_ids: list[str]
    convention: str = Field(default="new", description="new=新版无停用字段；old=旧版保留")


def _safe(call, *args, **kwargs) -> dict[str, Any]:
    try:
        return call(*args, **kwargs)
    except KeyError as exc:
        return {"ok": False, "message": str(exc).strip("'")}


@router.get("/sources")
def list_sources() -> dict[str, Any]:
    """来源清单及当前试跑资格。"""
    return {"ok": True, "sources": coordinator.list_source_states()}


@router.post("/preview")
def preview_deactivation(payload: FieldPayload) -> dict[str, Any]:
    """停用前预览：可展开的字段停用影响表（按来源命中、缺失字段、规则版本）。"""
    return coordinator.preview_deactivation(payload.field_name.strip())


@router.post("/sources/{source_id}/dependency")
def record_dependency(source_id: str, payload: DependencyPayload) -> dict[str, Any]:
    """预览后复核补录：某来源下游是否真实依赖停用字段。"""
    return _safe(
        coordinator.record_dependency,
        source_id, payload.field_name.strip(), payload.depends, payload.note.strip(),
    )


@router.post("/sources/{source_id}/trials")
def start_trial(source_id: str, payload: TrialStartPayload) -> dict[str, Any]:
    """解析验收：从影响表领取真实原行和新规则版本试跑。"""
    return coordinator.start_trial(source_id, payload.field_name.strip())


@router.get("/trials")
def list_trials(source_id: str | None = None) -> dict[str, Any]:
    """试跑记录列表（含已撤回，历史记录仍可复算）。"""
    return {"ok": True, "items": coordinator.list_trials(source_id)}


@router.get("/trials/{trial_id}")
def get_trial(trial_id: str) -> dict[str, Any]:
    trial = coordinator.get_trial(trial_id)
    if trial is None:
        return {"ok": False, "message": f"试跑 {trial_id} 不存在"}
    return {"ok": True, "trial": trial}


@router.post("/trials/confirm")
def confirm_trial(payload: TrialIdPayload) -> dict[str, Any]:
    """确认领用新版。"""
    return _safe(coordinator.confirm_trial, payload.trial_id.strip())


@router.post("/trials/withdraw")
def withdraw_trial(payload: TrialIdPayload) -> dict[str, Any]:
    """用同一批原行撤回试跑并按旧规则重新解析，恢复旧字段解释。"""
    return _safe(coordinator.withdraw_trial, payload.trial_id.strip())


@router.post("/trials/recompute")
def recompute_trial(payload: RecomputePayload) -> dict[str, Any]:
    """按历史试跑记录复算指定规则版本。"""
    return _safe(
        coordinator.recompute_trial,
        payload.trial_id.strip(), payload.rule_version.strip(),
    )


@router.get("/trials/{trial_id}/lines/{line_number}")
def read_trial_line(trial_id: str, line_number: int) -> dict[str, Any]:
    """用试跑结果完成一次实际读取。"""
    return _safe(coordinator.read_trial_line, trial_id, line_number)


@router.post("/batch-decisions")
def decide_batch(payload: BatchDecisionPayload) -> dict[str, Any]:
    """detect_schema_change 串联 apply_parse_rules，决定本批哪些来源可继续。"""
    mode = payload.mode.strip() if payload.mode.strip() in ("independent", "group") else "independent"
    return coordinator.decide_batch(payload.field_name.strip(), mode)


@router.post("/chain-evidence")
def build_chain_evidence(payload: ChainEvidencePayload) -> dict[str, Any]:
    """值班复查：多来源合为一份请求链证据，必须同一字段约定才可交付。"""
    return coordinator.build_request_chain_evidence(
        payload.field_name.strip(), payload.source_ids, payload.convention.strip())
