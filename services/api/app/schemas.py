from typing import Any, List, Dict
from pydantic import BaseModel


class StartEvalRequest(BaseModel):
    question: str
    top_k: int | None = 5
    window_size: int | None = 0
    topic: str | None = None
    scenario: str | None = None
    scenario_defined: bool | None = None


class OptionPayload(BaseModel):
    method: str | None = None
    answer: str
    sources: List[Dict[str, Any]]


class StartEvalResponse(BaseModel):
    evaluation_id: str
    optionA: OptionPayload
    optionB: OptionPayload


class SubmitEvalRequest(BaseModel):
    evaluation_id: str
    choice: str  # 'A' or 'B' or 'N' (neutral)


class SubmitEvalResponse(BaseModel):
    evaluation_id: str
    choice: str
    chosen_method: str
