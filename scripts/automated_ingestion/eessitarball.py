# from git import Remote, Repo
from utils import send_slack_message, sha256sum

from pathlib import PurePosixPath

# import boto3
import github
import json
import logging
import os
import re
import subprocess
import tarfile
import time

from datetime import datetime, timezone
# from shared_vars import gh_comment_cache, gh_pr_cache, gh_repo_cache
from utils import get_gh_comment, get_gh_pr, get_gh_repo


class EessiTarball:
    """
    Class that represents an EESSI tarball containing software installations
    or a compatibility layer, and which is stored in an S3 bucket. It has
    several functions to handle the different states of such a tarball in the
    actual ingestion process, for which it interfaces with the S3 bucket,
    GitHub, and CVMFS.
    """

    def __init__(self, tarball_path, tarball_state, object_metadata, config, github, s3, local_repo):
        """Initialize the tarball object."""
        # init config, github, git staging repo and s3 objects
        t1_b = time.time()
        self.config = config
        self.github = github
        self.git_repo = get_gh_repo(config['github']['staging_repo'], github)
        self.s3 = s3
        self.local_repo = local_repo

        # store some of the object's metadata for later use
        self.s3_object_etag = object_metadata['ETag']

        # store remote path to metadata file and determine remote path to tarball
        # path to tarball is structured as follows:
        # 'tarballs/EESSI_PILOT_VERSION/LAYER/EESSI_OS_TYPE/EESSI_ARCH_SUBDIR/TIMESTAMP/TARBALL_NAME'
        self.remote_tarball_path = tarball_path
        # path to metadata file is structured as follows:
        # 'tarball_state/EESSI_PILOT_VERSION/LAYER/EESSI_OS_TYPE/EESSI_ARCH_SUBDIR/TIMESTAMP/TARBALL_NAME.meta.txt'
        metadata_ext = config['paths']['metadata_file_extension']
        self.remote_metadata_path = tarball_path.replace('tarballs', tarball_state, 1) + metadata_ext

        # set local paths to store metadata file and tarball
        self.local_tarball_path = os.path.join(config['paths']['download_dir'], os.path.basename(tarball_path))
        self.local_metadata_path = self.local_tarball_path + metadata_ext

        # init default values for some instance information
        self.metadata_raw = ''
        self.metadata_json = {}

        self.sw_repo_name = ''
        self.sw_repo = None

        self.sw_pr_number = -1
        self.sw_pr = None

        self.sw_pr_comment_id = -1
        self.sw_pr_comment = None

        self.tarball_name = ''
        # reference to PR in staging repo
        self.tar_pr = None

        # read metadata and init data structures
        t2_b = time.time()
        self.download()
        t2_e = time.time()
        if os.path.exists(self.local_metadata_path):
            t3_b = time.time()
            with open(self.local_metadata_path, 'r') as meta:
                self.metadata_raw = meta.read()
            self.metadata_json = json.loads(self.metadata_raw)

            self.sw_repo_name = self.metadata_json['link2pr']['repo']
            self.sw_repo = get_gh_repo(self.sw_repo_name, self.github)

            self.sw_pr_number = self.metadata_json['link2pr']['pr']
            self.sw_pr = get_gh_pr(self.sw_pr_number, self.sw_repo)

            if 'pr_comment_id' in self.metadata_json['link2pr']:
                self.sw_pr_comment_id = self.metadata_json['link2pr']['pr_comment_id']
                self.sw_pr_comment = get_gh_comment(self.sw_pr_comment_id, self.sw_pr)
            else:
                logging.warn("should we try to obtain the comment id via scanning all comments or should we wait?")

            self.tarball_name = self.metadata_json['payload']['filename']
            t3_e = time.time()
        else:
            t3_b = time.time()
            logging.warn(f"local metadata file '{self.local_metadata_path}' does not exist")
            # TODO should raise an exception
            t3_e = time.time()

#        self.url = f'https://{config["aws"]["staging_bucket"]}.s3.amazonaws.com/{object_name}'
        # TODO verify if staging bucket and object_name are added correctly
        self.bucket = config["aws"]["staging_bucket"]
        self.url = f'{config["aws"]["endpoint_url"]}/{self.bucket}/{tarball_path}'

        self.states = {
            'new': {'handler': self.handle_new_tarball, 'next_state': 'staged'},
            'staged': {'handler': self.open_approval_request, 'next_state': 'pr_opened'},
            'pr_opened': {'handler': self.check_pr_status},
            'approved': {'handler': self.ingest, 'next_state': 'ingested'},
            # 'unknown': {'handler': self.print_unknown},
        }

        # set the initial state of this tarball.
        self.state = tarball_state
        logging.info(f"state is {self.state}, tarball is {self.tarball_name}")
        t1_e = time.time()
        if self.metadata_json:
            tarball_size = int(self.metadata_json['payload']['size'])
            rate = tarball_size / (t2_e-t2_b)
        else:
            tarball_size = -1
            rate = 0.0
        logging.info("timings (EessiTarball::__init__)")
        logging.info(f"  download.....: {t2_e-t2_b:.2f} seconds, "
                     f"size {tarball_size/1000000:.3f} MB, rate {rate/1000000:.3f} MB/s")
        logging.info(f"  init metadata: {t3_e-t3_b:.2f} seconds")
        logging.info(f"  total........: {t1_e-t1_b:.2f} seconds")
        print("timings (EessiTarball::__init__)")
        print(f"  download.....: {t2_e-t2_b:.2f} seconds, "
              f"size {tarball_size/1000000:.3f} MB, rate {rate/1000000:.3f} MB/s")
        print(f"  init metadata: {t3_e-t3_b:.2f} seconds")
        print(f"  total........: {t1_e-t1_b:.2f} seconds")

    def check_pr_status(self):
        """
        Check status of pull request on GitHub (merged -> approved; closed -> rejected).
        """
        t1_b = time.time()
        print(f">> check_pr_status(): {self.remote_tarball_path}")

        filename = os.path.basename(self.remote_tarball_path)
        # TODO remove '_{next_state}' suffix
        git_branch = filename

        logging.info(f"get approval pr for {self.remote_tarball_path}")
        print(f"get approval pr for {self.remote_tarball_path}")
        t2a_b = time.time()
        pr = self.get_approval_pr(update=True)
        t2a_e = time.time()

        if pr:
            logging.info(f'PR {pr.number} found for {self.remote_tarball_path}')
            print(f'    PR {pr.number} found for {self.remote_tarball_path}')

            t2b_b = time.time()
            if pr.state == 'open':
                # The PR is still open, so it hasn't been reviewed yet: nothing to do.
                logging.info(f'PR {pr.number} is still open, skipping this tarball...')
                print(f'    PR {pr.number} is still open, skipping this tarball...')
            elif pr.state == 'closed' and not pr.merged:
                # The PR was closed but not merged, i.e. it was rejected for ingestion.
                logging.info(f'PR {pr.number} was rejected')
                print(f'    PR {pr.number} was rejected')
                # if PR was closed the changes in PR branch are not merged, hence the old_state
                # is the state when the branch was created
                self.mark_new_state('rejected', old_state='staged')
            else:
                # The PR was closed and merged, i.e. it was approved for ingestion.
                logging.info(f'PR {pr.number} was approved')
                print(f'    PR {pr.number} was approved')
                self.mark_new_state('approved')
            t2b_e = time.time()
        else:
            # There is a branch, but no PR for this tarball.
            # This is weird, so let's remove the branch and reprocess the tarball.
            logging.info(f'Tarball {self.remote_tarball_path} has a branch, but no PR.')
            logging.info('Removing existing branch...')
            print(f'    Tarball {self.remote_tarball_path} has a branch, but no PR.')
            print('    Removing existing branch...')

            t2b_b = time.time()
            ref = self.git_repo.get_git_ref(f'heads/{git_branch}')
            ref.delete()

            # move metadata file back to staged (only needed for S3, branch on github has been deleted)
            if self.s3_move_metadata_file('staged'):
                self.state = 'staged'
            else:
                print(f"something went wrong when moving metadata file from '{self.state}' to 'staged'")
                # TODO create an issue?
            t2b_e = time.time()

        t1_e = time.time()
        logging.info("timings (EessiTarball::check_pr_status)")
        logging.info(f"  obtain pr instance: {t2a_e-t2a_b:.2f} seconds")
        logging.info(f"  process pr state..: {t2b_e-t2b_b:.2f} seconds")
        logging.info(f"  total.............: {t1_e-t1_b:.2f} seconds")
        print("timings (EessiTarball::check_pr_status)")
        print(f"  obtain pr instance: {t2a_e-t2a_b:.2f} seconds")
        print(f"  process pr state..: {t2b_e-t2b_b:.2f} seconds")
        print(f"  total.............: {t1_e-t1_b:.2f} seconds")

    def download(self, force=False, metadata_force=False, tarball_force=False):
        """
        Download this tarball and its corresponding metadata file, if this hasn't been already done.
        """
        bucket = self.config['aws']['staging_bucket']

        if force or tarball_force or not os.path.exists(self.local_tarball_path):
            try:
                self.s3.download_file(bucket, self.remote_tarball_path, self.local_tarball_path)
            except Exception:
                logging.warn(
                    f'Failed to download tarball {self.remote_tarball_path} from {bucket} to {self.local_tarball_path}.'
                )
                self.local_tarball_path = None

        if force or metadata_force or not os.path.exists(self.local_metadata_path):
            try:
                self.s3.download_file(bucket, self.remote_metadata_path, self.local_metadata_path)
            except Exception:
                logging.warn(f'Failed to download metadata file {self.remote_metadata_path} '
                             f'from {bucket} to {self.local_metadata_path}.')
                self.local_metadata_path = None

    def find_state(self):
        """Find the state of this tarball by searching through the state directories in the git repository."""
        for state in list(self.states.keys()):
            # iterate through the state dirs and try to find the tarball's metadata file
            try:
                self.git_repo.get_contents(state + '/' + self.remote_metadata_path)
                return state
            except github.UnknownObjectException:
                # no metadata file found in this state's directory, so keep searching...
                continue
            except github.GithubException:
                # if there was some other (e.g. connection) issue, abort the search for this tarball
                logging.warning(f'Unable to determine the state of {self.remote_tarball_path}!')
                return "unknown"
        else:
            # if no state was found, we assume this is a new tarball that was ingested to the bucket
            return "new"

    def get_contents_overview(self):
        """Return an overview of what is included in the tarball."""
        logging.debug(f'get contents overview for "{self.local_tarball_path}"')
        tar = tarfile.open(self.local_tarball_path, 'r')
        members = tar.getmembers()
        tar_num_members = len(members)
        paths = sorted([m.path for m in members])

        if tar_num_members < 100:
            tar_members_desc = 'Full listing of the contents of the tarball:'
            members_list = paths
        else:
            tar_members_desc = 'Summarized overview of the contents of the tarball:'
            prefix = os.path.commonprefix(paths)
            # TODO: this only works for software tarballs, how to handle compat layer tarballs?
            swdirs = [  # all directory names with the pattern: <prefix>/software/<name>/<version>
                m.path
                for m in members
                if m.isdir() and PurePosixPath(m.path).match(os.path.join(prefix, 'software', '*', '*'))
            ]
            modfiles = [  # all filenames with the pattern: <prefix>/modules/<category>/<name>/*.lua
                m.path
                for m in members
                if m.isfile() and PurePosixPath(m.path).match(os.path.join(prefix, 'modules', '*', '*', '*.lua'))
            ]
            other = [  # anything that is not in <prefix>/software nor <prefix>/modules
                m.path
                for m in members
                if not PurePosixPath(prefix).joinpath('software') in PurePosixPath(m.path).parents and
                not PurePosixPath(prefix).joinpath('modules') in PurePosixPath(m.path).parents
                # if not fnmatch.fnmatch(m.path, os.path.join(prefix, 'software', '*'))
                # and not fnmatch.fnmatch(m.path, os.path.join(prefix, 'modules', '*'))
            ]
            members_list = sorted(swdirs + modfiles + other)

        # Construct the overview.
        tar_members = '\n'.join(members_list)
        overview = f'Total number of items in the tarball: {tar_num_members}'
        overview += f'\nURL to the tarball: {self.url}'
        overview += f'\n{tar_members_desc}\n'
        overview += f'```\n{tar_members}\n```'

        # Make sure that the overview does not exceed Github's maximum length (65536 characters).
        if len(overview) > 60000:
            overview = overview[:60000] + '\n\nWARNING: output exceeded the maximum length and was truncated!\n```'
        return overview

    def next_state(self, state):
        """Find the next state for this tarball."""
        if state in self.states and 'next_state' in self.states[state]:
            return self.states[state]['next_state']
        else:
            return None

    def run_handler(self):
        """Process this tarball by running the process function that corresponds to the current state."""
        if not self.state:
            logging.warning(f"tarball {self.remote_tarball_path} has no state set; skipping...")
            return
            # self.state = self.find_state()
        handler = self.states[self.state]['handler']
        handler()

    def verify_checksum(self):
        """Verify the checksum of the downloaded tarball with the one in its metadata file."""
        local_sha256 = sha256sum(self.local_tarball_path)
        meta_sha256 = self.metadata_json['payload']['sha256sum']
        logging.debug(f'Checksum of downloaded tarball: {local_sha256}')
        logging.debug(f'Checksum stored in metadata file: {meta_sha256}')
        return local_sha256 == meta_sha256

    def ingest(self):
        """Process a tarball that is ready to be ingested by running the ingestion script."""
        # TODO: check if there is an open issue for this tarball, and if there is, skip it.
        t1_b = time.time()
        logging.info(f'Tarball {self.remote_tarball_path} is ready to be ingested.')

        self.download()

        logging.info('Verifying its checksum...')
        if 'payload' in self.metadata_json and 'size' in self.metadata_json['payload']:
            size_str = f"{int(self.metadata_json['payload']['size']) / 1000000:.3f} MB"
        else:
            size_str = "N/A"
        print(f'  verifying tarball checksum (size {size_str})...')
        t4_b = time.time()
        if not self.verify_checksum():
            logging.error('Checksum of downloaded tarball does not match the one in its metadata file!')
            # TODO Open issue?
            print('Checksum of downloaded tarball does not match the one in its metadata file!')
            return
        else:
            logging.debug(f'Checksum of {self.remote_tarball_path} matches the one in its metadata file.')
        t4_e = time.time()

        script = self.config['paths']['ingestion_script']
        sudo = ['sudo'] if self.config['cvmfs'].getboolean('ingest_as_root', True) else []
        logging.info(f'Running the ingestion script for {self.remote_tarball_path}...')

        # TODO add additional parameters for more info in cvmfs_server tag history
        sw_branch = self.sw_pr.base.ref
        uploader = self.metadata_json['uploader']['username']

        ingest_cmd = sudo + [script, self.local_tarball_path, self.sw_repo_name, sw_branch, self.sw_pr_number, uploader]
        logging.info(f'ingesting with /{" ".join(ingest_cmd)}/')
        print(f'  ingesting tarball (size {size_str}) with "{" ".join(ingest_cmd)}"')
        t2_b = time.time()
        ingest_run = subprocess.run(
            ingest_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        t2_e = time.time()
        if ingest_run.returncode == 0:
            t3_b = time.time()
            if self.config.has_section('slack') and self.config['slack'].getboolean('ingestion_notification', False):
                send_slack_message(
                    self.config['secrets']['slack_webhook'],
                    self.config['slack']['ingestion_message'].format(tarball=os.path.basename(self.remote_tarball_path))
                )
            logging.info(f'ingesting stdout: /{ingest_run.stdout.decode("UTF-8")}/')
            logging.info(f'ingesting stderr: /{ingest_run.stderr.decode("UTF-8")}/')

            # update comment in software-layer repo: ingested
            self.update_sw_repo_comment('ingest_done', prefix=self.determine_tarball_prefix())

            next_state = self.next_state(self.state)

            # move metadata file to next_state (ingested)
            self.git_move_metadata_file(self.state, next_state)
            if self.s3_move_metadata_file(next_state):
                self.state = next_state
            else:
                logging.warning(f"something went wrong when moving metadata file from '{self.state}' to {next_state}")
                print(f"something went wrong when moving metadata file from '{self.state}' to {next_state}")
                # TODO create an issue?
            t3_e = time.time()
        else:
            t3_b = time.time()
            issue_title = f'Failed to ingest {self.remote_tarball_path}'
            issue_body = self.config['github']['failed_ingestion_issue_body'].format(
                command=' '.join(ingest_run.args),
                tarball=self.remote_tarball_path,
                return_code=ingest_run.returncode,
                stdout=ingest_run.stdout.decode('UTF-8'),
                stderr=ingest_run.stderr.decode('UTF-8'),
            )
            if self.issue_exists(issue_title, state='open'):
                logging.info(f'Failed to ingest {self.remote_tarball_path}, '
                             'but an open issue already exists, skipping...')
            else:
                self.git_repo.create_issue(title=issue_title, body=issue_body)
            t3_e = time.time()
        t1_e = time.time()
        if self.metadata_json:
            tarball_size = int(self.metadata_json['payload']['size'])
            rate = tarball_size / (t2_e-t2_b)
            rate_v = tarball_size / (t4_e-t4_b)
        else:
            tarball_size = -1
            rate = 0.0
        logging.info("timings (EessiTarball::ingest)")
        logging.info(f"  verify checksum.......: {t4_e-t4_b:.2f} seconds, "
                     f"size {tarball_size/1000000:.3f} MB, rate {rate_v/1000000:.3f} MB/s")
        logging.info(f"  run ingest script.....: {t2_e-t2_b:.2f} seconds, "
                     f"size {tarball_size/1000000:.3f} MB, rate {rate/1000000:.3f} MB/s")
        print("timings (EessiTarball::ingest)")
        print(f"  verify checksum.......: {t4_e-t4_b:.2f} seconds, "
              f"size {tarball_size/1000000:.3f} MB, rate {rate_v/1000000:.3f} MB/s")
        print(f"  run ingest script.....: {t2_e-t2_b:.2f} seconds, "
              f"size {tarball_size/1000000:.3f} MB, rate {rate/1000000:.3f} MB/s")
        if ingest_run.returncode == 0:
            logging.info(f"  upd PR + move metadata: {t3_e-t3_b:.2f} seconds")
            print(f"  upd PR + move metadata: {t3_e-t3_b:.2f} seconds")
        else:
            logging.info(f"  open issue............: {t3_e-t3_b:.2f} seconds")
            print(f"  open issue............: {t3_e-t3_b:.2f} seconds")
        logging.info(f"  total.................: {t1_e-t1_b:.2f} seconds")
        print(f"  total.................: {t1_e-t1_b:.2f} seconds")

    def s3_move_metadata_file(self, new_state):
        """Moves a remote metadata file from its current state directory to the new_state directory."""
        # copy metadata file in S3 bucket from directory {self.state} to directory {new_state} and
        # delete metadata file in S3 bucket in directory {self.state}
        original_path = self.remote_metadata_path
        new_state_path = self.remote_metadata_path.replace(self.state, new_state, 1)
        bucket = self.config['aws']['staging_bucket']

        logging.info(f"copying metadata file from {original_path} to {new_state_path} in bucket {bucket}")
        print(f"copying metadata file from {original_path} to {new_state_path} in bucket {bucket}")

        # print('\nSKIPPING move in S3 bucket')

        try:
            response = self.s3.copy_object(
                Bucket=bucket,
                CopySource=f'{bucket}/{original_path}',
                Key=new_state_path,
            )
        except Exception as err:
            logging.warning(f"failed to copy metadata file from {original_path} to {new_state_path} with error {err}")
            print(f"failed to copy metadata file from {original_path} to {new_state_path} with error {err}")
            return False

        # verify that object was copied
        if response is None or 'CopyObjectResult' not in response:
            logging.warn(f"copying metatdata file returned no response data; not deleting original at {original_path}")
            print(f"copying metatdata file returned no response data; not deleting original at {original_path}")
            return False

        etag_org = self.s3_object_etag
        etag_new = response['CopyObjectResult']['ETag']
        if etag_org == etag_new:
            logging.info(f"copying of metadata file {original_path} to {new_state_path} succeeded; deleting original")
            print(f"copying of metadata file {original_path} to {new_state_path} succeeded; deleting original")
            self.s3.delete_object(
                Bucket=bucket,
                Key=original_path,
            )
        else:
            logging.warning(f"ETags of metatdata files differ (original={etag_org}, copied={etag_new});"
                            f"\n  not deleting original metadata file at {original_path}")
            print(f"ETags of metatdata files differ (original={etag_org}, copied={etag_new});"
                  f"\n  not deleting original metadata file at {original_path}")
            return False
        self.remote_metadata_path = new_state_path
        return True

    def mark_new_state(self, new_state, old_state=None):
        """
        Mark new state of a tarball.
        """
        t1_b = time.time()
        # update comment in software-layer repo
        # TODO ensure that setting 'ingest_{new_state}' exists or use default
        t2_b = time.time()
        self.update_sw_repo_comment(f'ingest_{new_state}')
        t2_e = time.time()

        # move metadata file to {new_state} top level dir
        t3_b = time.time()
        if not old_state:
            old_state = self.state
        # print(f"SKIP moving metadata file from '{old_state}' to '{new_state}'")
        self.git_move_metadata_file(old_state, new_state)
        if self.s3_move_metadata_file(new_state):
            self.state = new_state
        else:
            logging.warning(f"something went wrong when moving metadata file from '{old_state}' to '{new_state}'")
            print(f"something went wrong when moving metadata file from '{old_state}' to '{new_state}'")
            # TODO create an issue?
        t3_e = time.time()
        t1_e = time.time()
        logging.info(f"timings (EessiTarball::mark_new_state(new_state={new_state})")
        logging.info(f"  update PR comment..: {t2_e-t2_b:.2f} seconds")
        logging.info(f"  move metadata files: {t3_e-t3_b:.2f} seconds")
        logging.info(f"  total........: {t1_e-t1_b:.2f} seconds")
        print(f"timings (EessiTarball::mark_new_state(new_state={new_state})")
        print(f"  update PR comment..: {t2_e-t2_b:.2f} seconds")
        print(f"  move metadata files: {t3_e-t3_b:.2f} seconds")
        print(f"  total........: {t1_e-t1_b:.2f} seconds")

    def create_branch_for_tarball(self):
        """Creates a branch in the local repository for the tarball."""
        t2_b = time.time()

        # use filename as branch name
        filename = os.path.basename(self.remote_tarball_path)

        local_repo_dir = self.local_repo.working_tree_dir
        print(f'\n  local repo dir: {local_repo_dir}')
        git = self.local_repo.git(C=local_repo_dir)

        nwbr_result = git.branch(filename, 'origin/main')
        print(f'\n    new branch: "{nwbr_result}"')

        t2_e = time.time()
        logging.info(f"  create branch.......: {t2_e-t2_b:.2f} seconds")
        print(f"  create branch.......: {t2_e-t2_b:.2f} seconds")

        return filename

    def create_file_in_local_repo(self, file_path, file_contents, commit_msg, branch='main'):
        """Creates a file in the local repo in the given branch."""
        t1_b = time.time()

        local_repo_dir = self.local_repo.working_tree_dir
        git = self.local_repo.git(C=local_repo_dir)
        chkout_result = git.checkout(branch)

        full_path = os.path.join(local_repo_dir, file_path)
        directory = os.path.dirname(full_path)
        try:
            os.makedirs(directory, exist_ok=True)
            with open(full_path, 'w') as local_metadata_file:
                local_metadata_file.writelines(file_contents)
        except Exception as err:
            print(f'caught exception when trying to create/write file: {err}')

        # add + commit
        add_result = git.add(full_path)
        commit_result = git.commit("-m", commit_msg)

        t1_e = time.time()
        logging.info(f"  create file in local repo: {t1_e-t1_b:.2f} seconds")
        print(f"  create file in local repo: {t1_e-t1_b:.2f} seconds")

    def push_branch_to_remote_repo(self, pr_branch):
        """Push branch to remote repo."""
        t1_b = time.time()

        local_repo_dir = self.local_repo.working_tree_dir
        git = self.local_repo.git(C=local_repo_dir)
        push_result = git.push("origin", pr_branch)
        print(f'  git.push -> "{push_result}"')

        t1_e = time.time()
        logging.info(f"  create file in local repo: {t1_e-t1_b:.2f} seconds")
        print(f"  create file in local repo: {t1_e-t1_b:.2f} seconds")

    def handle_new_tarball(self):
        """Process a new tarball that was added to the staging bucket."""
        t1_b = time.time()

        next_state = self.next_state(self.state)
        logging.info(f'Found new tarball {self.remote_tarball_path}, downloading it...')
        print(f'    Found new tarball {self.remote_tarball_path}, downloading it...')

        # Download the tarball and its metadata file.
        self.download()
        if not self.local_tarball_path or not self.local_metadata_path:
            logging.info('Skipping this tarball...')
            print('    Skipping this tarball...')
            return

        pr_branch = self.create_branch_for_tarball()

        logging.info(f'Adding tarball\'s metadata to the "{next_state}" folder of the git repository.')
        print(f'    Adding tarball\'s metadata to the "{next_state}" folder of the git repository.')
        file_path_staged = self.remote_metadata_path.replace(self.state, next_state, 1)
        t2_b = time.time()
        # replace next line by creating file locally in pr_branch and pushing branch to remote repo
        # self.git_repo.create_file(file_path_staged, 'new tarball staged', self.metadata_raw, branch=pr_branch)
        print(f'    file_path_staged = "{file_path_staged}"')
        self.create_file_in_local_repo(file_path_staged, self.metadata_raw, 'new tarball staged', pr_branch)
        self.push_branch_to_remote_repo(pr_branch)
        
        t2_e = time.time()

        # move metadata file to staged
        t3_b = time.time()
        logging.info(f'Moving tarball\'s metadata to the "{next_state}" folder of the S3 bucket.')
        print(f'    Moving tarball\'s metadata to the "{next_state}" folder of the S3 bucket.')
        if self.s3_move_metadata_file(next_state):
            self.state = next_state
        else:
            logging.warn(f"something went wrong when moving metadata file from '{self.state}' to '{next_state}'")
            print(f"    something went wrong when moving metadata file from '{self.state}' to '{next_state}'")
            # TODO create an issue?
        t3_e = time.time()

        t4_b = time.time()
        self.update_sw_repo_comment('ingest_staged')
        t4_e = time.time()
        t1_e = time.time()
        logging.info("timings (EessiTarball::handle_new_tarball)")
        logging.info(f"  add metadata file to GitHub: {t2_e-t2_b:.2f} seconds")
        logging.info(f"  move metadata file on S3...: {t3_e-t3_b:.2f} seconds")
        logging.info(f"  update PR comment..........: {t4_e-t4_b:.2f} seconds")
        logging.info(f"  total........: {t1_e-t1_b:.2f} seconds")
        print("timings (EessiTarball::handle_new_tarball)")
        print(f"  add metadata file to GitHub: {t2_e-t2_b:.2f} seconds")
        print(f"  move metadata file on S3...: {t3_e-t3_b:.2f} seconds")
        print(f"  update PR comment..........: {t4_e-t4_b:.2f} seconds")
        print(f"  total........: {t1_e-t1_b:.2f} seconds")

    def print_unknown(self):
        """Process a tarball which has an unknown state."""
        logging.info("The state of this tarball could not be determined, so we're skipping it.")

    def find_comment(self, pull_request, tarball_name):
        """Find comment in pull request that contains name of a tarball.
        Args:
            pull_request (object): PullRequest object (PyGithub) representing
                                   a pull request.
            tarball_name (string): Name of tarball used to identify a comment.
        Returns:
            issue_comment (object): IssueComment object (PyGithub) representing
                                    an issue comment.
        """
        comments = pull_request.get_issue_comments()
        for comment in comments:
            cms = f".*{tarball_name}.*"
            comment_match = re.search(cms, comment.body)
            if comment_match:
                return comment
        return None

    def get_approval_pr(self, update=False):
        """Find approval PR if any exists."""
        if self.tar_pr and not update:
            return self.tar_pr

        filename = os.path.basename(self.remote_tarball_path)
        # TODO remove '_approved'
        pr_branch = filename

        all_refs = self.local_repo.remote().refs

        # obtain commit for pr_branch
        #   iterate over all_refs,
        #   keep those where remote_head matches pr_branch, and
        #   use only the first commit
        commits = [ref.commit for ref in all_refs if ref.remote_head == pr_branch]
        if not commits:
            return None
        commit = commits[0]

        # obtain pr for pr_branch
        #   iterate over all_refs (again),
        #   keep those where the commit equals the one of the branch and if 'pull' is in the ref.remote_head
        #   only use middle element of remote_head which is something like 'pull/NUMBER/head'
        pulls = [ref.remote_head.split('/')[1]
                 for ref in all_refs if ref.commit == commit and 'pull' in ref.remote_head]
        if not pulls:
            return None

        # obtain pr instance from GitHub (also contains status information)
        gh_pr = self.git_repo.get_pull(int(pulls[0]))

        self.tar_pr = gh_pr
        return gh_pr

    def get_pr_url(self):
        """Return URL to approval PR."""
        self.tar_pr = self.get_approval_pr()

        if self.tar_pr:
            return self.tar_pr.html_url
        else:
            return None

    def determine_sw_repo_pr_comment(self, tarball_name):
        """Determine PR comment."""
        if self.sw_pr_comment:
            return self.sw_pr_comment
        else:
            return self.find_comment(self.sw_pr, tarball_name)

    def determine_tarball_prefix(self):
        """Determine common prefix of tarball."""
        tar = tarfile.open(self.local_tarball_path, 'r')
        members = tar.getmembers()
        paths = sorted([m.path for m in members])

        return os.path.commonprefix(paths)

    def update_sw_repo_comment(self, comment_template, prefix=None):
        """Update comment in PR of software-layer repository.
        """
        # obtain issue_comment (use previously stored value in self or determine via tarball_name)
        issue_comment = self.determine_sw_repo_pr_comment(self.tarball_name)

        if issue_comment:
            comment_update = self.config['github'][comment_template].format(
                date=datetime.now(timezone.utc).strftime('%b %d %X %Z %Y'),
                tarball=self.tarball_name,
                approval_pr=self.get_pr_url(),
                prefix=prefix,
                )
            logging.info(f'Comment found (id: {issue_comment.id}); '
                         f'adding row "{comment_update}"')
            # get current data/time
            issue_comment.edit(issue_comment.body + "\n" + comment_update)
        else:
            logging.info('Failed to find a comment for tarball '
                         f'{self.tarball_name} in pull request '
                         f'#{self.sw_pr_number} in repo {self.sw_repo_name}.')

    def open_approval_request(self):
        """Process a staged tarball by opening a pull request for ingestion approval."""
        t1_b = time.time()

        next_state = self.next_state(self.state)

        t3_b = time.time()
        filename = os.path.basename(self.remote_tarball_path)
        # move metadata file to next_state top level dir in branch named {filename}
        self.git_move_metadata_file(self.state, next_state, branch=filename)

        # Move the file to the top-level directory of the next stage in the S3 bucket
        self.s3_move_metadata_file(next_state)
        t3_e = time.time()

        # Try to get the tarball contents and open a PR to get approval for the ingestion
        try:
            t4_b = time.time()
            exception = False
            tarball_contents = self.get_contents_overview()
            pr_body = self.config['github']['pr_body'].format(
                tar_overview=tarball_contents,
                metadata=self.metadata_raw,
            )
            t4_i = time.time()
        except Exception as err:
            exception = True
            print(f'caught an exception "{err}"')
        t4_e = time.time()

        try:
            t5_b = time.time()
            if not exception:
                self.tar_pr = self.git_repo.create_pull(title='Ingest ' + filename,
                                                        body=pr_body,
                                                        head=filename,
                                                        base='main')
                t5_i = time.time()

                # update comment in pull request of softwares-layer repo
                self.update_sw_repo_comment('ingest_pr_opened')

        except Exception as err:
            exception = True
            print(f'caught an exception "{err}"')
            issue_title = f'Failed to get contents of {self.remote_tarball_path}'
            issue_body = self.config['github']['failed_tarball_overview_issue_body'].format(
                tarball=self.remote_tarball_path,
                error=err
            )
            if len([i for i in self.git_repo.get_issues(state='open') if i.title == issue_title]) == 0:
                self.git_repo.create_issue(title=issue_title, body=issue_body)
            else:
                logging.info('Failed to create tarball overview, but an issue already exists.')
                print('Failed to create tarball overview, but an issue already exists.')
        t5_e = time.time()
        t1_e = time.time()
        logging.info("timings (EessiTarball::open_approval_request)")
        logging.info(f"  move metadata files.: {t3_e-t3_b:.2f} seconds")
        print("timings (EessiTarball::open_approval_request)")
        print(f"  move metadata files.: {t3_e-t3_b:.2f} seconds")
        if not exception:
            if self.metadata_json:
                tarball_size = int(self.metadata_json['payload']['size'])
                rate = tarball_size / (t4_i-t4_b)
            else:
                tarball_size = -1
                rate = 0.0
            logging.info(f"  analyse tarball.....: {t4_e-t4_b:.2f} seconds")
            logging.info(f"  - analyse tarball...: {t4_i-t4_b:.2f} seconds, "
                         f"size {tarball_size/1000000:.3f} MB, rate {rate/1000000:.3f} MB/s")
            logging.info(f"  created pull request: {t5_e-t5_b:.2f} seconds")
            logging.info(f"  - open pull request.: {t5_i-t5_b:.2f} seconds")
            logging.info(f"  - update PR comment.: {t5_e-t5_i:.2f} seconds")
            print(f"  analyse tarball.....: {t4_e-t4_b:.2f} seconds")
            print(f"  - analyse tarball...: {t4_i-t4_b:.2f} seconds, "
                  f"size {tarball_size/1000000:.3f} MB, rate {rate/1000000:.3f} MB/s")
            print(f"  created pull request: {t5_e-t5_b:.2f} seconds")
            print(f"  - open pull request.: {t5_i-t5_b:.2f} seconds")
            print(f"  - update PR comment.: {t5_e-t5_i:.2f} seconds")
        else:
            logging.info(f"  exception -> issue..: {t5_e-t4_b:.2f} seconds")
            print(f"  exception -> issue..: {t5_e-t4_b:.2f} seconds")
        print(f"  total...............: {t1_e-t1_b:.2f} seconds")
        logging.info(f"  total...............: {t1_e-t1_b:.2f} seconds")

    def git_move_metadata_file(self, old_state, new_state, branch='main'):
        """Move the metadata file of a tarball from an old state's directory to a new state's directory."""
        metadata_path = '/'.join(self.remote_metadata_path.split('/')[1:])
        file_path_old = old_state + '/' + metadata_path
        file_path_new = new_state + '/' + metadata_path
        logging.debug(f'Moving metadata file {metadata_path} from {file_path_old} '
                      f'to {file_path_new} in branch {branch}.')
        print(f'\nMoving metadata file {metadata_path}\n  from {file_path_old}'
              f'\n    to {file_path_new}\n    in branch {branch}')

        # tarball_metadata = self.git_repo.get_contents(file_path_old)
        # # TODO maybe first create file, then remove? if remove succeeds and create fails, it may be lost
        # # Remove the metadata file from the old state's directory...
        # self.git_repo.delete_file(file_path_old, 'remove from ' + old_state, sha=tarball_metadata.sha, branch=branch)
        # # and move it to the new state's directory
        # self.git_repo.create_file(file_path_new, 'move to ' + new_state, tarball_metadata.decoded_content,
        #                           branch=branch)
        # USE git commands locally via self.local_repo.git() and push to remote
        local_repo_dir = self.local_repo.working_tree_dir
        print(f'\n  local repo dir: {local_repo_dir}')
        prefixed_file_path_old = os.path.join(local_repo_dir, file_path_old)

        git = self.local_repo.git(C=local_repo_dir)
        # check branch
        br_result = git.branch()
        print(f'\n    branch: "{br_result}"')
        # checkout branch
        chkout_result = git.checkout(branch)
        print(f'\n    checkout: "{chkout_result}"')
        # check branch again
        bra_result = git.branch()
        print(f'\n    branch: "{bra_result}"')

        if os.path.exists(prefixed_file_path_old):
            print(f'\n  moving old file path {file_path_old} with "git mv"')
            # make sure target directory exists
            target_directory = os.path.dirname(file_path_new)
            print(f'\n    target directory: {target_directory}')
            prefixed_target_directory = os.path.join(local_repo_dir, target_directory)
            if not os.path.exists(prefixed_target_directory):
                print('\n    target directory does not exist yet ... creating it')
                os.makedirs(prefixed_target_directory, exist_ok=True)
            mv_result = git.mv(file_path_old, file_path_new)
            print(f'\n    mv_result: "{mv_result}"')
            status_result = git.status()
            print(f'\n    status_result: "{status_result}"')
            # commit_result = git.commit(m=f'change state from {old_state} to {new_state}')
            self.local_repo.index.commit(f'change state from {old_state} to {new_state}')
            # commit result is not of type str, so we can't print it
            push_result = git.push('origin', branch)
            # print(f'\n    type(push_result): {type(push_result)}')
            print(f'\n    push_result: "{push_result}"')
            status_result = git.status()
            # print(f'\n    type(status_result): {type(status_result)}')
            print(f'\n    status_result: "{status_result}"')
        else:
            print(f'\nold file path {file_path_old} does not exist in branch "{branch}"')

    def reject(self):
        """Reject a tarball for ingestion."""
        # Let's move the the tarball to the directory for rejected tarballs.
        logging.info(f'Marking tarball {self.remote_tarball_path} as rejected...')
        next_state = 'rejected'
        self.git_move_metadata_file(self.state, next_state)
        # update comment in software-layer repo: rejected
        self.update_sw_repo_comment('ingest_rejected')

    def issue_exists(self, title, state='open'):
        """Check if an issue with the given title and state already exists."""
        issues = self.git_repo.get_issues(state=state)
        for issue in issues:
            if issue.title == title and issue.state == state:
                return True
        else:
            return False

    def display(self):
        """Print overview of object settings."""
        print(f"config: {self.config}")
        print(f"self.github: {self.github}")
        print(f"self.git_repo: {self.git_repo}")
        print(f"self.s3: {self.s3}")
        print(f"self.s3_object_etag: {self.s3_object_etag}")
        print(f"self.remote_tarball_path: {self.remote_tarball_path}")
        print(f"self.remote_metadata_path: {self.remote_metadata_path}")
        print(f"self.local_tarball_path: {self.local_tarball_path}")
        print(f"self.local_metadata_path: {self.local_metadata_path}")
        print(f"self.metadata_raw: {self.metadata_raw}")
        print(f"self.metadata_json: {self.metadata_json}")
        print(f"self.sw_repo_name: {self.sw_repo_name}")
        print(f"self.sw_repo: {self.sw_repo}")
        print(f"self.sw_pr_number: {self.sw_pr_number}")
        print(f"self.sw_pr: {self.sw_pr}")
        print(f"self.sw_pr_comment_id: {self.sw_pr_comment_id}")
        print(f"self.sw_pr_comment: {self.sw_pr_comment}")
        print(f"self.tarball_name: {self.tarball_name}")
        print(f"self.tar_pr: {self.tar_pr}")
        print(f"self.bucket: {self.bucket}")
        print(f"self.url: {self.url}")
        print(f"self.state: {self.state}")
