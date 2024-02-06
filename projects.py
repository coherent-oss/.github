#!/usr/bin/env -S pip-run httpx tabulate pyarrow pandas pypistats
from __future__ import annotations

import re
import xmlrpc.client
from functools import cache

import pandas as pd
from httpx import HTTPStatusError, get
from pypistats import recent

# ruff: noqa: EXE003, T201

SKELETON_PATTERN = re.compile(
    r"https://img.shields.io/badge/skeleton-\d{4}-informational",
)


def get_jaraco_projects() -> list[tuple[str, str]]:
    client = xmlrpc.client.ServerProxy("https://pypi.python.org/pypi")
    return client.user_packages("jaraco")  # type: ignore[return-value]


def get_pypi_stats_last_month(project: str) -> int:
    try:
        return recent(project, format="pandas").last_month[0]  # type: ignore[no-any-return]
    except HTTPStatusError:
        return 0


@cache
def get_pypi_project_data(project_url: str) -> dict[str, str]:
    return get(  # type: ignore[no-any-return]
        f"{project_url.replace('/project/', '/pypi/', 1)}/json",
    ).json()["info"]


def get_homepage(project: str) -> str:
    return get_pypi_project_data(project)["home_page"]


def get_skeleton_status(project_url: str, no_skeleton: str = "❌") -> str:
    # fmt: off
    badge_url = next(iter(
        SKELETON_PATTERN.findall(get_pypi_project_data(project_url)["description"]),
    ), None)
    # fmt: on
    if badge_url:
        return f"![skeleton]({badge_url})"
    return no_skeleton


if __name__ == "__main__":
    jaraco_projects = get_jaraco_projects()
    stats = pd.DataFrame.from_dict(
        {
            f"[{project}]({homepage_url})": (get_pypi_stats_last_month(project),)
            for role, project in jaraco_projects
            if (
                homepage_url := get_homepage(
                    project_url := f"https://pypi.org/project/{project}",
                )
            ).startswith("https://github.com/jaraco")
        },
        orient="index",
        columns=[
            key_column := "downloads last month <sub>(as of February 6th, 2024)</sub>",
        ],
    ).sort_values(key_column, ascending=False)

    print(stats.to_markdown())
