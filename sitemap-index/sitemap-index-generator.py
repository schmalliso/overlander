import pymongo
import pandas as pd


repos_branches = pymongo.MongoClient()["pool"].repos_branches

repos_branches_data = repos_branches.find()
sitemap_urls = []

url = "https://www.mongodb.com/"

for repo in repos_branches_data:
    print(repo["repoName"])
    sitemap_extension = "/sitemap-0.xml"
    # Exclude repos we don't care about
    if repo["repoName"] in ["docs-404", "docs-meta", "devhub-content", "docs-mongodb-internal", "docs-mongodb-internal-base", "docs-csfle-merge", "docs-k8s-operator", "docs-php-library", "docs-ruby", "docs-mongoid", "mms-docs"]:
        continue
    if not repo["branches"]:
        continue
    for branch in repo["branches"]:
        if branch["buildsWithSnooty"] == False: #this will be useful once we fix the drivers, mms-docs, k8s maps
            sitemap_extension = "/sitemap.xml.gz"
        print(branch)
        print("branchName: " + branch["gitBranchName"])
        if not branch["active"]:
            continue
        branch_url_base = url + repo["prefix"]["dotcomprd"]
        print("URL BASE:" + branch_url_base)
        if "urlSlug" in branch and branch["urlSlug"] is not None:
            print("Using urlSlug for the slug")
            sitemap_urls.append(branch_url_base + "/" + branch["urlSlug"] + sitemap_extension)
            continue
        if branch["publishOriginalBranchName"] == True:
            print("Using gitBranchName for the slug")
            sitemap_urls.append(branch_url_base + "/" + branch["gitBranchName"] + sitemap_extension)
            continue
        print("I guess this isn't versioned?")
        sitemap_urls.append(branch_url_base + sitemap_extension)


print(sitemap_urls)

# Set up DataFrame from the list of URLs

df = pd.DataFrame(sitemap_urls, columns=["loc"])

xml_data = df.to_xml(root_name="sitemapindex", row_name="sitemap", xml_declaration=True)
print(xml_data)

# Save the XML data to a file
with open("sitemap-index.xml", "w") as file:
    file.write(xml_data)


## TODO: 
# - rewrite v6.0 branch of manual to be manual ratther than v6.0, 
# - confirm with ElizabethB how to handle 'aliases'
# - figure out where to put this and how to handle credentials properly
