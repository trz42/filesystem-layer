import hashlib
import json
import requests

from shared_vars import gh_comment_cache, gh_pr_cache, gh_repo_cache


def get_gh_repo(full_repo_name, github_instance):
    """Return and cache pygithub.repository object for full_repo_name."""
    if full_repo_name not in gh_repo_cache:
        repo = github_instance.get_repo(full_repo_name)
        gh_repo_cache[full_repo_name] = repo
    return gh_repo_cache[full_repo_name]


def get_gh_pr(pr_number, repo_instance):
    """Return and cache pygithub.pull_request object for pr_number."""
    if pr_number not in gh_pr_cache:
        pr = repo_instance.get_pull(int(pr_number))
        gh_pr_cache[pr_number] = pr
    return gh_pr_cache[pr_number]


def get_gh_comment(pr_comment_id, pr_instance):
    """Return and cache pygithub.issue_comment object for pr_comment_id."""
    if pr_comment_id not in gh_comment_cache:
        comment = pr_instance.get_issue_comment(int(pr_comment_id))
        gh_comment_cache[pr_comment_id] = comment
    return gh_comment_cache[pr_comment_id]


def send_slack_message(webhook, msg):
    """Send a Slack message."""
    slack_data = {'text': msg}
    response = requests.post(
        webhook, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )


def sha256sum(path):
    """Calculate the sha256 checksum of a given file."""
    sha256_hash = hashlib.sha256()
    with open(path, 'rb') as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(8192), b''):
            sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
