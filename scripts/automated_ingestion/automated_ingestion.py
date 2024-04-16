#!/usr/bin/env python3

from eessitarball import EessiTarball
from git import Repo
from pid.decorator import pidfile
from pid import PidFileError
# from shared_vars import gh_repo_cache

import argparse
import boto3
# import botocore
import configparser
import github
import logging
import os
import pid
import re
import sys

REQUIRED_CONFIG = {
    'secrets': ['aws_secret_access_key', 'aws_access_key_id', 'github_pat', 'github_user'],
    'paths': ['download_dir', 'ingestion_script', 'metadata_file_extension', 'repo_base_dir'],
    'aws': ['staging_bucket'],
    'github': ['staging_repo', 'failed_ingestion_issue_body', 'pr_body',
               'ingest_staged', 'ingest_pr_opened', 'ingest_approved', 'ingest_rejected', 'ingest_done'],
}

LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

TARBALL_ALL_STATES = ['new', 'staged', 'pr_opened', 'approved', 'rejected', 'ingested']
TARBALL_END_STATES = ['rejected', 'ingested']


def clone_staging_repo(config):
    """Clone a GitHub repository to local disk."""
    # check if repo already exists
    staging_repo = config['github']['staging_repo']
    repo_base_dir = config['paths']['repo_base_dir']

    repo_name = staging_repo.rstrip('/').split('/')[-1]
    local_repo_dir = os.path.join(repo_base_dir, repo_name)
    if os.path.exists(local_repo_dir):
        logging.info(f"directory {repo_base_dir} already contains directory {repo_name}")
        print(f"directory {repo_base_dir} already contains directory {repo_name}")
        return Repo(local_repo_dir)

    # if not, clone it
    logging.info(f"cloning {staging_repo} into {local_repo_dir}")
    print(f"cloning {staging_repo} into {local_repo_dir}")
    return Repo.clone_from(f'https://github.com/{staging_repo}', local_repo_dir)


def error(msg, code=1):
    """Print an error and exit."""
    logging.error(msg)
    sys.exit(code)


def fetch_pulls_staging_repo(repo):
    """Fetch pull requests from a GitHub repository to local disk."""
    logging.info(f"fetch refs/pull/* for {repo.remote('origin').name} in {repo.working_dir}")
    print(f"fetch refs/pull/* for {repo.remote('origin').name} in {repo.working_dir}")
    checkout_main_branch(repo)
    repo.remote('origin').fetch(refspec='+refs/pull/*:refs/remotes/origin/pull/*')


def find_tarballs(s3, bucket, state):
    """
    Return a list of all metadata files representing tarballs in an S3 bucket.

    Note, we don't check if a matching tarball exists. That would require additional
    queries to the S3 bucket. The check has to be done by subsequent functions.
    """
    # TODO: list_objects_v2 only returns up to 1000 objects
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=state)
    # make sure the list is not empty
    if objects and 'Contents' in objects:
        # return all information ... we will use more than just file names / keys
        return objects['Contents']
    else:
        return []


def parse_config(path):
    """Parse the configuration file."""
    config = configparser.ConfigParser()
    try:
        config.read(path)
    except Exception as err:
        error(f'Unable to read configuration file {path}! error "{err}"')

    # Check if all required configuration parameters/sections can be found.
    for section in REQUIRED_CONFIG.keys():
        if section not in config:
            error(f'Missing section "{section}" in configuration file {path}.')
        for item in REQUIRED_CONFIG[section]:
            if item not in config[section]:
                error(f'Missing configuration item "{item}" in section "{section}" of configuration file {path}.')
    return config


def parse_args():
    """Parse the command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, help='path to configuration file',
                        default='automated_ingestion.cfg', dest='config')
    parser.add_argument('-d', '--debug', help='enable debug mode', action='store_true', dest='debug')
    parser.add_argument('-l', '--list', help='only list available tarballs', action='store_true', dest='list_only')
    parser.add_argument('-p', '--pattern', type=str, help='only process tarballs matching pattern',
                        dest='pattern')
    parser.add_argument('-s', '--state', type=str, help='only process tarballs in given state',
                        dest='state', choices=TARBALL_ALL_STATES)
    parser.add_argument('-v', '--verbose', help='show more information', action='store_true', dest='verbose')
    args = parser.parse_args()
    return args


def prepare_env(config):
    """Prepare env dictionary to be used for accessing private staging repository."""
    # prepare env with credentials
    os.environ['GITHUB_USER'] = config['secrets']['github_user']
    os.environ['GITHUB_TOKEN'] = config['secrets']['github_pat']
    os.environ['GIT_CONFIG_COUNT'] = '1'
    os.environ['GIT_CONFIG_KEY_0'] = 'credential.helper'
    os.environ['GIT_CONFIG_VALUE_0'] = '!f() { echo "username=${GITHUB_USER}"; echo "password=${GITHUB_TOKEN}"; }; f'
    # return env


def checkout_main_branch(repo):
    """Checkout main branch in local repository."""
    local_repo_dir = repo.working_tree_dir
    print(f'\n  local repo dir: {local_repo_dir}')
    git = repo.git(C=local_repo_dir)
    # checkout main branch
    chkout_result = git.checkout('main')
    print(f'\n    checkout: "{chkout_result}"')


def update_staging_repo(repo):
    """Update a GitHub repository on local disk."""
    logging.info(f"pull updates for {repo.remote('origin').name} in {repo.working_dir}")
    print(f"pull updates for {repo.remote('origin').name} in {repo.working_dir}")
    checkout_main_branch(repo)
    repo.remote('origin').pull()


@pid.decorator.pidfile('automated_ingestion.pid')
def main():
    """Main function."""
    args = parse_args()
    config = parse_config(args.config)
    log_file = config['logging'].get('filename', None)
    log_format = config['logging'].get('format', '%(levelname)s:%(message)s')
    log_level = LOG_LEVELS.get(config['logging'].get('level', 'INFO').upper(), logging.WARN)
    log_level = logging.DEBUG if args.debug else log_level
    if args.debug:
        logging.basicConfig(
                format=log_format,
                level=log_level,
                handlers=[
                        logging.FileHandler(log_file),
                        logging.StreamHandler()
                ])
    else:
        logging.basicConfig(filename=log_file, format=log_format, level=log_level)
    # TODO: check configuration: secrets, paths, permissions on dirs, etc
    gh_pat = config['secrets']['github_pat']
    gh = github.Github(gh_pat)
    # gh_repo_cache = {}

    prepare_env(config)

    # obtain staging repo (only does what needs to be done)
    repo = clone_staging_repo(config)

    s3 = boto3.client(
        's3',
        aws_access_key_id=config['secrets']['aws_access_key_id'],
        aws_secret_access_key=config['secrets']['aws_secret_access_key'],
        endpoint_url=config['aws']['endpoint_url'],
        verify=config['aws']['verify_cert_path'],
    )

    states = TARBALL_ALL_STATES
    if args.state:
        states = [args.state]

    for state in states:
        print(f"state = {state}")

        update_staging_repo(repo)
        fetch_pulls_staging_repo(repo)

        object_list = find_tarballs(s3, config['aws']['staging_bucket'], state)
        print(f"number of tarballs in state '{state}': {len(object_list)}")

        metadata_ext = config['paths']['metadata_file_extension']
        if args.list_only:
            for num, obj in enumerate(object_list):
                metadata_file = obj['Key']
                tarball = metadata_file.replace(state, 'tarballs', 1).rstrip(metadata_ext)
                if args.pattern and not re.match(args.pattern, tarball):
                    print(f"tarball {tarball} does not match pattern {args.pattern}; skipping")
                    continue
                print(f'{num} ({state}): {obj["Key"]}')
        elif state not in TARBALL_END_STATES:
            for num, obj in enumerate(object_list):
                metadata_file = obj['Key']
                tarball = metadata_file.replace(state, 'tarballs', 1).rstrip(metadata_ext)
                if args.pattern and not re.match(args.pattern, tarball):
                    print(f"tarball {tarball} does not match pattern {args.pattern}; skipping")
                    continue

                print(f"init tarball...: {tarball}")
                indent = max(len('tarballs')-len(state), 0)
                print(f"  metadata file: {indent*' '}{metadata_file}")

                tar = EessiTarball(tarball, state, obj, config, gh, s3, repo)
                if args.verbose:
                    tar.display()
                print(f"processing tarball (state={tar.state}): {tarball}")
                tar.run_handler()
                print()


if __name__ == '__main__':
    try:
        main()
    except PidFileError:
        error('Another instance of this script is already running!')
