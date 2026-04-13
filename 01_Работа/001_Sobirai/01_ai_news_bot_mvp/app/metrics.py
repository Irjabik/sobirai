from __future__ import annotations

from dataclasses import dataclass, field
from time import time


@dataclass
class RuntimeMetrics:
    started_at: float = field(default_factory=time)
    collected_posts: int = 0
    sent_messages: int = 0
    failed_messages: int = 0
    retry_attempts: int = 0

    def snapshot(self) -> dict[str, float | int]:
        uptime = int(time() - self.started_at)
        return {
            "uptime_sec": uptime,
            "collected_posts": self.collected_posts,
            "sent_messages": self.sent_messages,
            "failed_messages": self.failed_messages,
            "retry_attempts": self.retry_attempts,
        }

