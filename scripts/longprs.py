import requests
import os
from datetime import datetime, timedelta

use_headers = {'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28',
               'Authorization': 'Bearer ' + os.environ['TOKEN']}


def get_all(url, headers=use_headers, params={}):
    all_issues = requests.get(url, headers=headers, params=params)
    issues_data = all_issues.json()
    while 'next' in all_issues.links.keys():
        all_issues = requests.get(all_issues.links['next']['url'], headers=headers)
        issues_data = issues_data + all_issues.json()
    return issues_data


def no_api(url_string):
    return url_string.replace('https://api.', 'https://www.').replace('/repos/', '/').replace('pulls', 'pull')


def process_diff(diff_lines):
    current_path = ""
    adds = 0
    dels = 0
    del_run = 0
    comment_run = 0
    diff_by_file = dict()
    for line in diff_lines.splitlines():
        if line.startswith("diff"):
            if len(current_path) > 0:
                diff_by_file[current_path] = (adds, dels)
            current_path = line.split()[-1]
            adds = 0
            dels = 0
            del_run = 0
            comment_run = 0
        elif line.startswith("+") and not line.startswith("+++"):
            del_run = 0

            # Ignore comment lines
            if line[1:].lstrip().startswith("#"):
                continue

            adds = adds + 1
            # Count runs of multi-line comments and subtract from total adds
            # We do not want to penalize comments.
            if comment_run > 0:
                comment_run = comment_run + 1
            if '"""' in line:
                if comment_run > 0:
                    adds = adds - comment_run
                    comment_run = 0
                else:
                    comment_run = 1
        elif line.startswith("-") and not line.startswith("---"):
            comment_run = 0
            del_run = del_run + 1
            # If we're seeing a large block of deletes, discount it
            # We do not want to penalize diffs of large blocks of code.
            if del_run < 30:
                dels = dels + 1
        else:
            del_run = 0
            comment_run = 0
    if adds > 0 or dels > 0:
        diff_by_file[current_path] = (adds, dels)

    return diff_by_file


stale = datetime.today() - timedelta(days=10)
comment_limit = 50
diff_limit = 450
prs = get_all('https://api.github.com/repos/snowflakedb/snowpandas/pulls')

# Flag PRS that are older than stale, or have more than comment_limit comments
# or have a diff more than diff_limit lines
# Find all comments, and all comments more recent than 6 days.
summaries = []
diff_fetch_header = use_headers.copy()
diff_fetch_header['Accept'] = 'application/vnd.github.v3.diff'

for pr in prs:
    url = pr['url']
    pr_detail = get_all(url)
    data = requests.get(url, headers=diff_fetch_header, params={})
    diff_by_file = process_diff(data.text)
    num_delta = 0
    for file_name, diff in diff_by_file.items():
        if "test" not in file_name:
            num_delta += diff[0] + diff[1]

    num_comments = pr_detail['review_comments']
    created_at = datetime.strptime(pr["created_at"], '%Y-%m-%dT%H:%M:%SZ')
    summaries.append((no_api(url), num_delta, num_comments, created_at,))


report = ""
for summary in summaries:
    url = summary[0]
    num_delta = summary[1]
    num_comments = summary[2]
    created_at = summary[3]
    if num_comments > comment_limit:
        report += (f"{url} has a large number ({num_comments}) of comments. Consider an offline discussion to "
                   f"resolve quickly, or additional design reviews.\n")
    elif num_delta > diff_limit:
        report += f"{url} has a large diff ({num_delta} lines). Consider breaking it up\n"
    elif created_at < stale:
        report += f"{url} has been open since {created_at}. Should this be closed or revised?\n"

if len(report) > 0:
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as fh:
        print(report, file=fh)
