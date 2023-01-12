from pymongo import MongoClient
import csv
import os

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

# export SNOOTY_CONN_STRING='thing'

client = MongoClient(os.environ.get('SNOOTY_CONN_STRING'))

results = client['pool']['repos_branches'].find()


newData = []


for result in results:
    repoName = result['repoName']
    if repoName == "devhub-content":
        continue
    branches = result['branches']
    print(repoName)
    if len(branches) == 1:
        newData.append({"repoName": repoName, "branchName": branches[0]['gitBranchName'], "url-version-component": ""})

    for branch in branches:
        if branch['active'] == False:
            continue
        branchName = branch.get('gitBranchName', '')
        publishOriginalBranchName = branch.get('publishOriginalBranchName', '')
        active = branch['active']
        snooty = branch.get('buildsWithSnooty', '')
        urlSlug = branch.get('urlSlug', branchName)
        if snooty == False:
            continue
        urlAliases = branch.get('urlAliases', '')
        stable = branch.get('isStableBranch', False)
        if urlAliases:
            for alias in urlAliases:
                newData.append({"repoName": repoName, "branchName": branchName, "url-version-component": alias, "slug": urlSlug, "isStableBranch": stable})
        if publishOriginalBranchName == True:
            newData.append({"repoName": repoName, "branchName": branchName, "url-version-component": branchName, "slug": urlSlug, "isStableBranch": stable})
        

csv_columns = ["repoName", "branchName", "url-version-component", "slug", "isStableBranch"]


try:
    with open('list.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=csv_columns)
        writer.writeheader()
        for data in newData:
            writer.writerow(data)
except IOError:
    print("NOOOPE")

