#!/bin/bash

# Ingest a tarball containing software, a compatibility layer,
# or (init) scripts to the NESSI CVMFS repository, and generate
# nested catalogs in a separate transaction.
# This script has to be run on a CVMFS publisher node.

# This script assumes that the given tarball is named like:
# eessi-<version>-{compat,init,scripts,software}-[additional information]-<timestamp>.tar.gz
# It also assumes, and verifies, that the  name of the top-level directory of the contents of the
# of the tarball matches <version>, and that name of the second level should is either compat, init, scripts, or software.

# Only if it passes these checks, the tarball gets ingested to the base dir in the repository specified below.

repo=pilot.nessi.no
basedir=versions
decompress="gunzip -c"
cvmfs_server="cvmfs_server"
# list of supported architectures for compat and software layers
declare -A archs=(["aarch64"]= ["ppc64le"]= ["riscv64"]= ["x86_64"]=)
# list of supported operating systems for compat and software layers
declare -A oss=(["linux"]= ["macos"]=)
# list of supported tarball content types
declare -A content_types=(["compat"]= ["init"]= ["scripts"]= ["software"]=)


function echo_green() {
    echo -e "\e[32m$1\e[0m"
}

function echo_red() {
    echo -e "\e[31m$1\e[0m"
}

function echo_yellow() {
    echo -e "\e[33m$1\e[0m"
}

function error() {
    echo_red "ERROR: $1" >&2
    exit 1
}

function is_repo_owner() {
    if [ -f "/etc/cvmfs/repositories.d/${repo}/server.conf" ]
    then
        . "/etc/cvmfs/repositories.d/${repo}/server.conf"
        [ x"$(whoami)" = x"$CVMFS_USER" ]
    fi
}

function check_repo_vars() {
    if [ -z "${repo}" ]
    then
        error "the 'repo' variable has to be set to the name of the CVMFS repository."
    fi

    if [ -z "${basedir}" ] || [ "${basedir}" == "/" ]
    then
        error "the 'basedir' variable has to be set to a subdirectory of the CVMFS repository."
    fi
}

function check_version() {
    if [ -z "${version}" ]
    then
        error "NESSI version cannot be derived from the filename."
    fi

    if [ -z "${tar_top_level_dir}" ]
    then
        error "no top level directory can be found in the tarball."
    fi

    # Check if the NESSI version number encoded in the filename
    # is valid, i.e. matches the format YYYY.DD
    if ! echo "${version}" | egrep -q '^20[0-9][0-9]\.(0[0-9]|1[0-2])$'
    then
        error "${version} is not a valid NESSI version."
    fi

    # Check if the version encoded in the filename matches the top-level dir inside the tarball
    if [ "${version}" != "${tar_top_level_dir}" ]
    then
        error "the version in the filename (${version}) does not match the top-level directory in the tarball (${tar_top_level_dir})."
    fi
}

function check_contents_type() {
    if [ -z "${contents_type_dir}" ]
    then
        error: "could not derive the content type of the tarball from the filename."
    fi

    if [ -z "${tar_contents_type_dir}" ]
    then
        error: "could not derive the content type of the tarball from the first file in the tarball."
    fi

    # Check if the name of the second-level dir in the tarball matches to what is specified in the filename
    if [ "${contents_type_dir}" != "${tar_contents_type_dir}" ]
    then
        error "the contents type in the filename (${contents_type_dir}) does not match the contents type in the tarball (${tar_contents_type_dir})."
    fi

    # Check if the second-level dir in the tarball is compat, software, scripts or init
    if [ ! -v content_types[${tar_contents_type_dir}] ]
    then
        error "the second directory level of the tarball contents should be either compat, software, scripts or init."
    fi
}

function cvmfs_regenerate_nested_catalogs() {
    # Use the .cvmfsdirtab to generate nested catalogs for the ingested tarball
    echo "Generating the nested catalogs..."
    ${cvmfs_server} transaction "${repo}"
    ${cvmfs_server} publish -m "Generate catalogs after ingesting ${tar_file_basename}" "${repo}"
    ec=$?
    if [ $ec -eq 0 ]
    then
        echo_green "Nested catalogs for ${repo} have been created!"
    else
        echo_red "failure when creating nested catalogs for ${repo}."
    fi
}

function cvmfs_add_more_metadata() {
     echo "Adding metadata to tag history"
     # example tag history via command 'cvmfs_server tag -x pilot.nessi.no'
     # generic-2022-11-16T07:52:56Z 00146ec1fc67287d8d0916ec4edd34f616a7e632 29696 36 1668585186 (default) Generate catalogs after ingesting eessi-2022.11-software-linux-x86_64-generic-1668253670.tar.gz
     # trunk 00146ec1fc67287d8d0916ec4edd34f616a7e632 29696 36 1668585186 (default) current HEAD
     # generic-2022-11-16T07:52:35Z 6977066e1491827e9be164a407e5230b6de17777 5087232 35 1668585171 (default)
     # trunk-previous 6977066e1491827e9be164a407e5230b6de17777 5087232 35 1668585171 (default) default undo target
     # generic-2022-11-15T21:55:49Z a5ef00196961b14d8b0d74a261bc909a800fbc29 28672 34 1668549349 (default) Generate catalogs after ingesting eessi-2022.11-software-linux-aarch64-generic-1668253729.tar.gz
     LAST_TAG=$(cvmfs_server tag -x ${repo} | head -n 1 | cut -f 1 -d ' ')
     cvmfs_server tag -a "${LAST_TAG}-meta" -m "TAR ${tar_file_basename} REPO ${GH_REPO} BRANCH ${BRANCH} PR/COMMIT ${PR_or_COMMIT} WHO ${WHO}" ${repo}
}

function cvmfs_ingest_tarball() {
    # Do a regular "cvmfs_server ingest" for a given tarball,
    # followed by regenerating the nested catalog
    echo "Ingesting tarball ${tar_file} to ${repo}..."
    ${decompress} "${tar_file}" | ${cvmfs_server} ingest -t - -b "${basedir}" -m "nessi" "${repo}"
    ec=$?
    if [ $ec -eq 0 ]
    then
        echo_green "${tar_file} has been ingested to ${repo}."
    else
        error "${tar_file} could not be ingested to ${repo}."
    fi

    cvmfs_add_more_metadata

    # "cvmfs_server ingest" doesn't automatically rebuild the nested catalogs,
    # so we do that forcefully by doing an empty transaction
    cvmfs_regenerate_nested_catalogs
}

function check_os() {
    # Check if the operating system directory is correctly set for the contents of the tarball
    os=$(echo "${tar_first_file}" | cut -d / -f 3)
    if [ -z "${os}" ]
    then
        error "no operating system directory found in the tarball!"
    fi
    if [ ! -v oss[${os}] ]
    then
        error "the operating system directory in the tarball is ${os}, which is not a valid operating system!"
    fi
    echo "OS component is '${os}'"
}

function check_arch() {
    # Check if the architecture directory is correctly set for the contents of the tarball
    arch_and_date=$(echo "${tar_first_file}" | cut -d / -f 4)
    arch=${arch_and_date//.*}
    if [ -z "${arch}" ]
    then
        error "no architecture directory found in the tarball!"
    fi
    if [ ! -v archs[${arch}] ]
    then
        error "the architecture directory in the tarball is ${arch}, which is not a valid architecture!"
    fi
    echo "full ARCH component is '${arch_and_date}'"
    echo "stnd ARCH component is '${arch}'"
}

function update_lmod_caches() {
    # Update the Lmod caches for the stacks of all supported CPUs
    script_dir=$(dirname $(realpath $BASH_SOURCE))
    update_caches_script=${script_dir}/update_lmod_caches.sh
    if [ ! -f ${update_caches_script} ]
    then
        error "cannot find the script for updating the Lmod caches; it should be placed in the same directory as the ingestion script!"
    fi
    if [ ! -x ${update_caches_script} ]
    then
        error "the script for updating the Lmod caches (${update_caches_script}) does not have execute permissions!"
    fi
    cvmfs_server transaction "${repo}"
    ${update_caches_script} /cvmfs/${repo}/${basedir}/${version}
    cvmfs_server publish -m "update Lmod caches after ingesting ${tar_file_basename}" "${repo}"
}

function ingest_init_tarball() {
    # Handle the ingestion of tarballs containing init scripts
    cvmfs_ingest_tarball
}

function ingest_scripts_tarball() {
    # Handle the ingestion of tarballs containing scripts directory with e.g. bash utils and GPU related scripts
    cvmfs_ingest_tarball
}

function ingest_software_tarball() {
    # Handle the ingestion of tarballs containing software installations
    check_arch
    check_os
    cvmfs_ingest_tarball
    update_lmod_caches
}

function ingest_compat_tarball() {
    # Handle the ingestion of tarballs containing a compatibility layer
    check_arch
    check_os
    # Assume that we already had a compat layer in place if there is a startprefix script in the corresponding CVMFS directory
    if [ -f "/cvmfs/${repo}/${basedir}/${version}/compat/${os}/${arch}/startprefix" ];
    then
        echo_yellow "Compatibility layer for version ${version}, OS ${os}, and architecture ${arch} already exists!"
        echo_yellow "Removing the existing layer, and adding the new one from the tarball..."
        ${cvmfs_server} transaction "${repo}"
        rm -rf "/cvmfs/${repo}/${basedir}/${version}/compat/${os}/${arch}/"
        tar --absolute-names -C "/cvmfs/${repo}/${basedir}/" -xzf "${tar_file}"
        ${cvmfs_server} publish -m "update compat layer for ${version}, ${os}, ${arch}" "${repo}"
        ec=$?
        if [ $ec -eq 0 ]
        then
            echo_green "Successfully ingested the new compatibility layer!"
	    cvmfs_add_more_metadata
        else
            ${cvmfs_server} abort "${repo}"
            error "error while updating the compatibility layer, transaction aborted."
        fi
    else
        cvmfs_ingest_tarball
    fi
}

# Check if a tarball has been specified
if [ "$#" -ne 5 ]; then
    error "usage: $0 <gzipped tarball> <GITHUB_REPO_SHORT user/reponame> <BRANCH main...> <PR/COMMIT> <WHO name of ingester>"
fi

tar_file="$1"
GH_REPO="$2"
BRANCH="$3"
PR_or_COMMIT="$4"
WHO="$5"

# Check if the given tarball exists
if [ ! -f "${tar_file}" ]; then
    error "tar file ${tar_file} does not exist!"
fi

# Get some information about the tarball
tar_file_basename=$(basename "${tar_file}")
version=$(echo "${tar_file_basename}" | cut -d- -f2)
# contents_type_dir=$(echo "${tar_file_basename}" | cut -d- -f3)
# temporarily use last line to determine contents type correctly if it contains, eg, init files
contents_type_dir=$(tar tf "${tar_file}" | tail -n 1 | cut -d/ -f2)
# need to find a file (not necessarily the first) whose path contains all components: VERSION/TYPE/OS/ARCH
# tar_first_file=$(tar tf "${tar_file}" | head -n 1)
tar_first_file=$(tar tf "${tar_file}" | head -n 4 | tail -n 1)
tar_top_level_dir=$(echo "${tar_first_file}" | cut -d/ -f1)
# Use the 2nd file/dir in the tarball, as the first one may be just "<version>/"
# tar_contents_type_dir=$(tar tf "${tar_file}" | head -n 2 | tail -n 1 | cut -d/ -f2)
# temporarily use last line to determine contents type correctly if it contains, eg, init files
tar_contents_type_dir=$(tar tf "${tar_file}" | tail -n 1 | cut -d/ -f2)

echo "tar_file_basename....: '${tar_file_basename}'"
echo "version..............: '${version}'"
echo "contents_type_dir....: '${contents_type_dir}'"
echo "tar_first_file.......: '${tar_first_file}'"
echo "tar_top_level_dir....: '${tar_top_level_dir}'"
echo "tar_contents_type_dir: '${tar_contents_type_dir}'"

# only check if contents_type_dir is software
if [ "x${contents_type_dir}" == "xsoftware" ]; then
  check_arch
  check_os
fi

# exit 0

# Check if we are running as the CVMFS repo owner, otherwise run cvmfs_server with sudo
is_repo_owner || cvmfs_server="sudo cvmfs_server"

### add more metadata to tag history
##echo "Adding metadata to tag history"
### cvmfs_server tag -x pilot.nessi.no
### generic-2022-11-16T07:52:56Z 00146ec1fc67287d8d0916ec4edd34f616a7e632 29696 36 1668585186 (default) Generate catalogs after ingesting eessi-2022.11-software-linux-x86_64-generic-1668253670.tar.gz
### trunk 00146ec1fc67287d8d0916ec4edd34f616a7e632 29696 36 1668585186 (default) current HEAD
### generic-2022-11-16T07:52:35Z 6977066e1491827e9be164a407e5230b6de17777 5087232 35 1668585171 (default)
### trunk-previous 6977066e1491827e9be164a407e5230b6de17777 5087232 35 1668585171 (default) default undo target
### generic-2022-11-15T21:55:49Z a5ef00196961b14d8b0d74a261bc909a800fbc29 28672 34 1668549349 (default) Generate catalogs after ingesting eessi-2022.11-software-linux-aarch64-generic-1668253729.tar.gz
##LAST_TAG=$(cvmfs_server tag -x ${repo} | head -n 1 | cut -f 1 -d ' ')
##cvmfs_server tag -a "${LAST_TAG}-meta" -m "TAR ${tar_file_basename} REPO ${GH_REPO} BRANCH ${BRANCH} PR/COMMIT ${PR_or_COMMIT} WHO ${WHO}" ${repo}
##

# Do some checks, and ingest the tarball
check_repo_vars
check_version
# Disable the call to check_contents_type, as it does not work for tarballs produced
# by our build bot that only contain init files (as they have "software" in the filename)
# check_contents_type
ingest_${tar_contents_type_dir}_tarball
