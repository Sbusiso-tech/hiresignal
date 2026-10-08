from jobmarket.sources.adzuna import AdzunaSource
from jobmarket.sources.arbeitnow import ArbeitnowSource
from jobmarket.sources.base import JobSource
from jobmarket.sources.himalayas import HimalayasSource
from jobmarket.sources.jobicy import JobicySource
from jobmarket.sources.jsearch import JSearchSource
from jobmarket.sources.remotive import RemotiveSource

SOURCES: dict[str, type[JobSource]] = {
    cls.name: cls
    for cls in (HimalayasSource, JobicySource, RemotiveSource, ArbeitnowSource, AdzunaSource, JSearchSource)
}

__all__ = ["SOURCES", "JobSource"]
