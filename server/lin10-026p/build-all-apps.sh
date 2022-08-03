#!/bin/bash

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Parse versions
source $SCRIPT_DIR/../vidi_common/scripts/parse-version.sh $SCRIPT_DIR/../

# Checkout correct version of vidi_airports repo
bash $SCRIPT_DIR/../vidi_common/scripts/checkout-vidi-repo.sh $SCRIPT_DIR/../ vidi_airports $version_product

cd $SCRIPT_DIR/../

./vidi_common/scripts/app-operations/build/build-app.sh `pwd` vidi_airports SZX apps_forwarder BPI_ETL prd
./vidi_common/scripts/app-operations/build/package-app.sh `pwd` SZX apps_forwarder BPI_ETL prd

./vidi_common/scripts/app-operations/build/build-app.sh `pwd` vidi_airports SZX apps_indexer BPI_Storage prd
./vidi_common/scripts/app-operations/build/package-app.sh `pwd` SZX apps_indexer BPI_Storage prd

./vidi_common/scripts/app-operations/build/build-app.sh `pwd` vidi_airports SZX apps_searchhead vidi prd
cp -r config/splunk/SZX_vidi/default/ deployment_packages/$version_project/apps_searchhead/prd/SZX_vidi/
cp -r config/splunk/SZX_vidi/locale/ deployment_packages/$version_project/apps_searchhead/prd/SZX_vidi/
cp -r app_local_changes/SZX_vidi/local/ deployment_packages/$version_project/apps_searchhead/prd/SZX_vidi/
./vidi_common/scripts/app-operations/build/package-app.sh `pwd` SZX apps_searchhead vidi prd

./vidi_common/scripts/app-operations/build/build-app.sh `pwd` vidi_airports SZX apps_searchhead monitoring prd
./vidi_common/scripts/app-operations/build/package-app.sh `pwd` SZX apps_searchhead monitoring prd

# Build Splunk App DB Connect
source $SCRIPT_DIR/../vidi_airports/versions
./vidi_airports/scripts/build-and-package-splunk-app-db-connect.sh
