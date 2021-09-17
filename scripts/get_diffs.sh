#! /bin/bash

set -e

git rev-parse HEAD >> commit.txt
git rev-parse --short HEAD >> short_commit.txt

commit=$(cat commit.txt)

echo "Commit:   $commit"
echo "Base:     ${BASE_BRANCH}"
if [[ ! -z "$PR_NUMBER" ]]; then
  echo "Pull Request: #${PR_NUMBER}"
fi

git config user.email "<>"
git config user.name "git"
git remote | xargs -n1 git remote remove
git remote add origin ${BASE_REPO_URL}
git remote -vv

{
  git branch -D base-branch
} || {
  printf "\n"
}
{
  git branch -D feature
} || {
  printf "\n"
}

if [[ ! -z "${PR_NUMBER}" ]]; then
  git fetch origin "pull/${PR_NUMBER}/head":feature
  git fetch --unshallow
else
  git branch -m feature
fi

git fetch origin "${BASE_BRANCH}":base-branch
git checkout base-branch
git diff --name-only feature...base-branch > POTENTIAL_CONFLICTS.txt
git merge --no-ff feature
git status


#! /bin/bash
DIFF=$(git diff --name-only origin/"${BASE_BRANCH}"...HEAD)

DIFF_TEAM=""
DIFF_LIST=""


for file in $DIFF
do
  if [[ "$file" =~ ^teams/([^/]*)/([^/]*)/([^/]*)/ ]]
  then
    DIFF_TEAM+="${BASH_REMATCH[1]}\n"
    DIFF_LIST+="${BASH_REMATCH[0]}\n"
  fi
done

printf "${DIFF_TEAM}" | sort | uniq > DIFF_TEAM.txt
printf "${DIFF_LIST}" | sort | uniq > DIFF_LIST.txt

printf "\nDIFF_LIST:\n"
cat DIFF_LIST.txt
