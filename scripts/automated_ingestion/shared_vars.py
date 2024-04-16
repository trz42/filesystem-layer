# dictionary that links a pull request comment id to an issue comment object to minimize requests to GitHub API
gh_comment_cache = {}

# dictionary that links a pull request number to a pull request object to minimize requests to GitHub API
gh_pr_cache = {}

# dictionary that maps a repository name to a repository object to minimize requests to GitHub API
gh_repo_cache = {}
