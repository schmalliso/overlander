import pymongo
import pandas as pd
import os
from flutter import check_type, checked
from dataclasses import dataclass
from typing import Optional
from posixpath import join

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
    eolType: Optional[str]


@checked
@dataclass
class Repo:
    repoName: str
    branches: list[Branch] | None
    prefix: str
    baseUrl: str

@checked
@dataclass
class DBBranchObj:
    """Define the branches object in repos_branches"""
    id: any #ObjectId
    gitBranchName: str
    active: bool
    urlAliases: Optional[list[str]]
    publishOriginalBranchName: bool
    urlSlug: str
    versionSelectorLabel: str
    isStableBranch: bool
    buildsWithSnooty: bool
    aliases: Optional[any]
    name: Optional[str]


@checked
@dataclass
class DBPrefixObj:
    """Define the prefixes object in repos_branches"""
    stg: str
    prd: str
    dotcomstg: str
    dotcomprd: str

@checked
@dataclass
class DBRepoObj:
    repoName: str
    branches: list[DBBranchObj]
    prefix: list[DBPrefixObj]
    bucket: list[any] #don't care
    url: list[any] #don't care
    project: str #don't care
    search: Optional[list[any]] #don't care
    groups: Optional[list[any]] #don't care
    displayName: Optional[str] #don't care
    _id: any #don't care

class ConstructRepo:
    def __init__(self, data) -> None:
        self.data = data

        self.repoName: str = data["repoName"]
        self.branches = self.get_branches()
        self.prefix = self.get_prefix()
        self.baseUrl = self.derive_url()

    def get_prefix(self) -> str:
        if not check_type(str, self.data["docset"][0]["prefix"]["dotcomprd"]):
            raise TypeError
        return self.data["docset"][0]["prefix"]["dotcomprd"]

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
                branch.get("eol_type", None)
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
        raise ValueError(f"No branch entry for {data['repoName']}.")
    if not (data["docset"].get("prefix") and data["docset"]["prefix"].get("dotcomprd")):
        raise ValueError(f"No dotcomprd prefix entry for {data['repoName']}")
    if not (data.get("prodDeployable")):
        raise ValueError(f"Cannot determine prod deployablility for {data['repoName']}")
    return


def main() -> None:
    repos_branches = pymongo.MongoClient(os.environ.get("SNOOTY_CONN_STRING"))[
        "pool"
    ].repos_branches

    lookup_pipeline = [
        {"$lookup": {
            "from": "docsets",
            "localField": "_id",
            "foreignField": "repos",
            "as": "docset"
        }}
    ]

    repos_branches_data = repos_branches.aggregate(lookup_pipeline)
    sitemap_urls: list[str] = []

    for r in repos_branches_data:
        try:
            run_validation(r)
        except Exception as e:
            print(e.args)

        print(r)

        # Skip repos that do not need sitemaps and br 
        if r["internalOnly"] or not r["prodDeployable"]:
            print(f"Skipping {r['repoName']}")
            continue
        repo = ConstructRepo(r).export()

        if repo.branches:
            for b in repo.branches:
                if b.active and not (b.eolType or b.urlSlug == "upcoming" or b.urlSlug == "beta" or (b.gitBranchName == "master" and b.urlSlug == "master")):
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
    with open("sitemap-index-full.xml", "w") as file:
        file.write(xml_data)


if __name__ == "__main__":
    main()
