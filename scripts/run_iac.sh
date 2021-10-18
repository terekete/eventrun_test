
#!/bin/sh

set -e
BUILD_DIR=$(pwd)
echo "Build dir: " ${BUILD_DIR}
ls -la
cat DIFF_TEAM.txt

#COMMIT=$(cat /workspace/commit.txt)

cat DIFF_TEAM.txt | while read team
do
    echo "team:" team
    # if [[ $df_flex_folder =~ ^pipelines/([^/]*)/dataflows/flex-"$LANGUAGE"/([^/]*)/$ ]]
    # then
    #     # Generate directory for dataflow job in the zip artifact
    #     PIPELINE=${BASH_REMATCH[1]}
    #     DF_NAME=${BASH_REMATCH[2]}
    #     ARTIFACT_DIR=dataflows/flex-"$LANGUAGE"/"$PIPELINE"/"$DF_NAME"/

    #     # If the directory does not exist, it must have been deleted.
    #     # Just skip it, this Dataflow does not need to be deployed.
    #     if [[ ! -d $df_flex_folder ]]
    #     then
    #       echo "The following Dataflow Template has been Deleted:"
    #       echo "$df_flex_folder"
    #       echo "Nothing to be done. Skipping..."
    #       continue
    #     fi

    #     echo "Building: $df_flex_folder"

    #     cd $df_flex_folder
    #     if [ -e "Dockerfile" ]; then
    #         echo "Commit ID: " ${COMMIT}
    #         docker build --ssh default=/root/.ssh/id_rsa --no-cache -t "gcr.io/${CONTAINER_IMAGE_REGISTRY}/${PIPELINE}/${DF_NAME}:${COMMIT}" .
    #         docker push "gcr.io/${CONTAINER_IMAGE_REGISTRY}/${PIPELINE}/${DF_NAME}:${COMMIT}"
    #     else
    #         echo "Error: No docker file is located in the build artifact"
    #         exit 1
    #     fi
    #     cd $BUILD_DIR
        
    #     # Copy folder to staging
    #     mkdir -p staging/"$ARTIFACT_DIR"
    #     cp -r "$df_flex_folder"/metadata.json staging/"$ARTIFACT_DIR"
    # else
    #     echo "Error: $df_flex_folder does not match a dataflow flex directory structure"
    #     exit 1
    # fi
done

export PULUMI_CONFIG_PASSPHRASE=test
pulumi login gs://eventrun-state
python /workspace/scripts/iac.py
