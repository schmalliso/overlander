#!/bin/bash

echo "Do you want to reclone the repos?"
options=("yes" "no")
select answer in "${options[@]}";do
    echo "You picked $answer"
    case $REPLY in
        1)
            echo "Cloning repos"
            sites=("mongodb/docs" "mongodb/docs-realm" "10gen/cloud-docs" "mongodb/docs-node")
            rm -rf ./sites-temp/*
            for site in "${sites[@]}"
            do
                echo "Processing $site"
                git -C ./sites-temp clone  --depth=1 git@github.com:$site.git
            done
            break
            ;;
        2)
            echo "Skipping cloning step."
            break
            ;;
    esac
done

echo "Do you want to regenerate the Snooty manifests?"
options=("yes" "no")
select answer in "${options[@]}";do
    echo "You picked $answer"
    case $REPLY in
    1)
        echo "regenerating manifests"
        rm -rf manifests/tmp/*
        for repo in `ls ./sites-temp/`;
        do
            if [[ -f $repo ]]; then
                echo "$repo is not a directory, skipping"
                break;
            else
                echo "Generating manifest for $repo"
                snooty build ./sites-temp/$repo --output=manifests/tmp/$repo
                unzip manifests/tmp/$repo -d manifests/$repo
            fi
        done
        break
        ;;
    2)
        echo "skipping manifest regeneration"
        break
        ;;
    esac
done

echo "Time to generate search indexes"
for manifest in `ls ./manifests/`; 
do
    echo "Processing $manifest"
    if [[ "$manifest" == "cloud-docs" ]]; then
        url="https://www.mongodb.com/docs/atlas/"
        output="atlas-master.json"
    elif [[ "$manifest" == "docs" ]]; then
        url="https://www.mongodb.com/docs/manual/"
        output="manual-master.json"
    elif [[ "$manifest" == "docs-realm" ]]; then
        url="https://www.mongodb.com/docs/realm/"
        output="realm-master.json"
    elif [[ "$manifest" == "docs-node" ]]; then
        url="https://www.mongodb.com/docs/drivers/node/current"
        output="node-master.json"
    else
        echo "Skipping this because it's probably the tmp folder."
        break
    fi
    mut-index ./manifests/$manifest/documents -o ./search-manifests/$output -u $url
done
#     mut-index upload -b allison-test-mut-index -p search-indexes/prd ./manifests/$manifest/documents -o $output -u $url -g
# done
