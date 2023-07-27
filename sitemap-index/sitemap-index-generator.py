import pymongo
import pandas as pd
import os
from flutter import check_type, checked
from dataclasses import dataclass
from typing import Optional
from posixpath import join

# TODO: replace this with a flag in the DB like excludeFromSitemapIndex or the like
excluded_repos = [
    "docs-404",
    "docs-meta",
    "devhub-content",
    "docs-mongodb-internal",
    "docs-mongodb-internal-base",
    "docs-csfle-merge",
    "docs-k8s-operator",
    "docs-php-library",
    "docs-ruby",
    "docs-mongoid",
    "mms-docs",
]


@checked
@dataclass
class SitemapUrlSuffix:
    gitBranchName: str
    urlSuffix: str
    extension: str


@checked
@dataclass
class Branch:
    gitBranchName: str
    active: bool
    publishOriginalBranchName: bool
    urlSlug: Optional[str]
    buildsWithSnooty: bool


@checked
@dataclass
class Repo:
    repoName: str
    branches: list[Branch] | None
    prefix: str
    baseUrl: str


class ConstructRepo:
    def __init__(self, data) -> None:
        self.data = data

        self.repoName: str = data["repoName"]
        self.branches = self.get_branches()
        self.prefix = self.get_prefix()
        self.baseUrl = self.derive_url()

    def get_prefix(self) -> str:
        if not check_type(str, self.data["prefix"]["dotcomprd"]):
            raise TypeError
        return self.data["prefix"]["dotcomprd"]

    def derive_url(self) -> str:
        url = join("https://www.mongodb.com", self.prefix)
        return url

    def get_branches(self) -> list[Branch] | None:
        if not self.data["branches"]:
            self.wonky = True
            return None
        branch_list: list[Branch] = []
        for branch in self.data["branches"]:
            new_branch = Branch(
                branch["gitBranchName"],
                branch.get("active", False),
                branch.get("publishOriginalBranchName", False),
                branch.get("urlSlug", None),
                branch.get("buildsWithSnooty", True),
            )
            branch_list.append(new_branch)
        return branch_list

    def export(self) -> Repo:
        repo = Repo(
            repoName=self.repoName,
            branches=self.branches,
            prefix=self.prefix,
            baseUrl=self.baseUrl,
        )
        return repo


class ConstructSitemapEntry:
    def __init__(self, data: Branch) -> None:
        self.data = data

        self.gitBranchName: str = data.gitBranchName
        self.urlSuffix = self.derive_url_suffix()
        self.extension = self.derive_extension()

    def derive_extension(self) -> str:
        if self.data.buildsWithSnooty:
            return "sitemap-0.xml"
        return "sitemap.xml.gz"

    def derive_url_suffix(self) -> str:
        urlSuffix: str = ""
        if self.data.urlSlug:
            urlSuffix = self.data.urlSlug
            return urlSuffix
        if self.data.publishOriginalBranchName:
            urlSuffix = self.gitBranchName
            return urlSuffix
        return urlSuffix

    def export(self) -> SitemapUrlSuffix:
        suffix = SitemapUrlSuffix(
            gitBranchName=self.gitBranchName,
            urlSuffix=self.urlSuffix,
            extension=self.extension,
        )
        return suffix


def run_validation(data) -> tuple[bool, str]:
    if not check_type(str, data["repoName"]):
        raise ValueError("No repo name?!")
    if not data.get("branches"):
        raise ValueError("No branch entry.")
    if not (data.get("prefix") and data["prefix"].get("dotcomprd")):
        raise ValueError("No dotcomprd prefix entry")
    return


def main() -> None:
    repos_branches = pymongo.MongoClient(os.environ.get("SNOOTY_CONN_STRING"))[
        "pool"
    ].repos_branches

    repos_branches_data = repos_branches.find()
    sitemap_urls: list[str] = []

    for r in repos_branches_data:
        try:
            run_validation(r)
        except Exception as e:
            print(e.args)

        # Skip repos that do not need sitemaps or whose sitemaps are horribly broken because built by legacy tooling
        if r["repoName"] in excluded_repos:
            print("Skipping")
            continue
        repo = ConstructRepo(r).export()

        if repo.branches:
            for b in repo.branches:
                if b.active:
                    print(b.gitBranchName)
                    sitemap_suffix = ConstructSitemapEntry(b).export()
                    sitemap_url = join(
                        repo.baseUrl, sitemap_suffix.urlSuffix, sitemap_suffix.extension
                    )
                    print(sitemap_url)
                    sitemap_urls.append(sitemap_url)
        else:
            print("Repo has no branches.")

    print(sitemap_urls)

    # Set up DataFrame from the list of URLs

    df = pd.DataFrame(sitemap_urls, columns=["loc"])

    xml_data = df.to_xml(
        root_name="sitemapindex",
        index=False,
        namespaces={"": "http://www.sitemaps.org/schemas/sitemap/0.9"},
        row_name="sitemap",
        xml_declaration=True,
    )
    print(xml_data)

    # Save the XML data to a file
    with open("sitemap-index.xml", "w") as file:
        file.write(xml_data)


if __name__ == "__main__":
    main()
