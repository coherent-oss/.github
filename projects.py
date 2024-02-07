#!/usr/bin/env -S pip-run httpx tabulate pyarrow pandas pypistats
from __future__ import annotations

import re
import xmlrpc.client
from collections import defaultdict
from datetime import datetime, timezone
from functools import cache
from posixpath import basename
from typing import NamedTuple, cast

import pandas as pd
from httpcore import UnsupportedProtocol
from httpx import HTTPError, HTTPStatusError, get
from pypistats import recent

# ruff: noqa: EXE003, T201

TODAY = datetime.now(timezone.utc).strftime("%B %-dth, %Y")
SKELETON_PATTERN = re.compile(
    r"https://img.shields.io/badge/skeleton-\d{4}-informational",
)


class Project(NamedTuple):
    name: str
    role: str
    cumulative_downloads: int
    downloads: int
    url: str


def get_jaraco_projects() -> dict[str, Project]:
    client = xmlrpc.client.ServerProxy("https://pypi.python.org/pypi")
    projects_by_homepage: dict[str, Project] = {}
    cumulative_downloads: dict[str, int] = defaultdict(int)

    for role, name in cast("list[tuple[str, str]]", client.user_packages("jaraco")):
        url = f"https://pypi.org/project/{name}"
        downloads = get_pypi_stats_last_month(name)
        homepage = get_homepage(url)
        cumulative_downloads[homepage] += downloads
        projects_by_homepage[homepage] = Project(
            downloads=downloads,
            cumulative_downloads=cumulative_downloads[homepage],
            role=role,
            name=basename(homepage.removesuffix("/")),
            url=url,
        )

    return projects_by_homepage


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


def get_homepage(project_url: str) -> str:
    try:
        return str(
            get(
                get_pypi_project_data(project_url)["home_page"],
                follow_redirects=True,
            ).url,
        )
    except (HTTPError, UnsupportedProtocol):
        return project_url


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
            f"[{project.name}]({homepage_url})": project.cumulative_downloads
            for homepage_url, project in jaraco_projects.items()
            if homepage_url.startswith("https://github.com/jaraco")
        },
        orient="index",
        columns=[
            downloads_column_name := f"downloads last month <sub>(as of {TODAY})</sub>",
        ],
    ).sort_values(downloads_column_name, ascending=False)
    downloads_column = stats[downloads_column_name]
    total_downloads = downloads_column.sum()
    stats[downloads_column_name] = downloads_column.map("{:,}".format)
    print(stats.to_markdown())
    print(
        "\nLast month, projects from the above table "
        f"had a total of {total_downloads:,} downloads.",
    )
