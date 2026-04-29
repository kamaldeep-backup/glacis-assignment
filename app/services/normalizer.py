from typing import Any, Protocol

from app.config import Settings
from app.domain.schemas import NormalizedWebhook
from app.llm.agent import build_normalization_agent, build_user_prompt


class WebhookNormalizer(Protocol):
    async def normalize(self, payload: Any) -> NormalizedWebhook:
        pass


class PydanticAIWebhookNormalizer:
    def __init__(self, settings: Settings | None = None) -> None:
        self._agent = build_normalization_agent(settings)

    async def normalize(self, payload: Any) -> NormalizedWebhook:
        result = await self._agent.run(build_user_prompt(payload))
        return result.output
