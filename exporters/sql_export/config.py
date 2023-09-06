import typing

from ra_utils.job_settings import JobSettings


class GqlLoraCacheSettings(JobSettings):
    class Config:
        frozen = True

    use_new_cache: bool = False
    primary_manager_responsibility: str | None = None
    exporters_actual_state_manager_responsibility_class: str | None = None
    prometheus_pushgateway: str | None = "pushgateway"
    mox_base: str = "http://mo:5000/lora"
    std_page_size: int = 400

    def to_old_settings(self) -> dict[str, typing.Any]:
        """Convert our DatabaseSettings to a settings.json format.

        This serves to implement the adapter pattern, adapting from pydantic and its
        corresponding 12-factor configuration paradigm with environment variables, to
        the current functionality of the program, based on the settings format from
        settings.json.

        Eventually the entire settings-processing within the program should be
        rewritten with a process similar to what has been done for the SD integration,
        but it was out of scope for the change when this code was introduced.
        """

        settings = {
            "mora.base": self.mora_base,
            "mox.base": self.mox_base,
            "exporters": {
                "actual_state": {
                    "manager_responsibility_class": self.primary_manager_responsibility
                }
            },
            "use_new_cache": self.use_new_cache,
        }

        return settings
