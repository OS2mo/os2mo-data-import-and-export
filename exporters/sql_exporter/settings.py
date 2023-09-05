import sys
from typing import List, Any
from uuid import UUID

import structlog
from ra_utils.job_settings import JobSettings


class SqlExporterSettings(JobSettings):

    use_new_cache: bool = False
    std_page_size: int = 500
    primary_manager_responsibility: UUID | None = None
    exporters_actual_state_manager_responsibility_class: UUID | None = None
    prometheus_pushgateway: str | None = "pushgateway"
    mox_base: str = "http://mo:5000/lora"
    persist_caches: bool = True
    historic: bool = False
    skip_past: bool = False
    resolve_dar: bool = True

    def to_oldSettings(self):
        old_seetings = {
            "mox.base": self.mox_base,
            "mora.base": self.mora_base,
            "exporters.actual_state.manager_responsibility_class": self.primary_manager_responsibility
            or self.exporters_actual_state_manager_responsibility_class,
        }
        return old_seetings
