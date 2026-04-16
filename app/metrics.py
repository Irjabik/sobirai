from __future__ import annotations

from dataclasses import dataclass, field
from time import time


@dataclass
class RuntimeMetrics:
    started_at: float = field(default_factory=time)
    collected_posts: int = 0
    x_collected_posts: int = 0
    x_api_requests: int = 0
    sent_messages: int = 0
    failed_messages: int = 0
    retry_attempts: int = 0

    def snapshot(self) -> dict[str, float | int]:
        uptime = int(time() - self.started_at)
        x_requests_per_post = (
            round(self.x_api_requests / self.x_collected_posts, 3)
            if self.x_collected_posts > 0
            else 0.0
        )
        return {
            "uptime_sec": uptime,
            "collected_posts": self.collected_posts,
            "x_collected_posts": self.x_collected_posts,
            "x_api_requests": self.x_api_requests,
            "x_requests_per_post": x_requests_per_post,
            "sent_messages": self.sent_messages,
            "failed_messages": self.failed_messages,
            "retry_attempts": self.retry_attempts,
        }

