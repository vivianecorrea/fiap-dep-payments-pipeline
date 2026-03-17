import yaml
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class SparkConfig:
    app_name: str
    log_level: str = "WARN"


@dataclass
class SourceConfig:
    path: str
    format: str
    options: Dict[str, str] = field(default_factory=dict)


@dataclass
class SinkConfig:
    path: str
    format: str
    mode: str


@dataclass
class PipelineConfig:
    report_year: int


@dataclass
class AppConfig:
    spark: SparkConfig
    pipeline: PipelineConfig
    sources: Dict[str, SourceConfig]
    sink: SinkConfig

    @classmethod
    def from_yaml(cls, path: str) -> "AppConfig":
        with open(path, "r") as f:
            raw = yaml.safe_load(f)

        spark_cfg = SparkConfig(**raw["spark"])
        pipeline_cfg = PipelineConfig(**raw["pipeline"])

        sources = {
            name: SourceConfig(**cfg)
            for name, cfg in raw["sources"].items()
        }

        sink_raw = raw["sink"]["sales_report"]
        sink_cfg = SinkConfig(**sink_raw)

        return cls(
            spark=spark_cfg,
            pipeline=pipeline_cfg,
            sources=sources,
            sink=sink_cfg,
        )
