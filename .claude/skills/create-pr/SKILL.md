---
name: create-pr
description: Draft a PR title and summary following the repo's pull request template, and open the PR if asked. Use when the user wants a PR description written, or wants to open a PR.
---

# Create PR

Write a PR for the current branch following `.github/pull_request_template.md`.

## 1. Understand the change

Look at `git log main..HEAD` and `git diff main...HEAD` to see what actually changed. Base the summary on the real diff, not just commit messages.

## 2. Write the summary

- Provide a terse and concise summary.
- Plain, simple language. Explain what changed and why, the way you'd explain it to a teammate, not a changelog entry.
- Short, natural sentences. No em dashes, en dashes, or semicolons. Avoid parentheses where possible.
- No filler, no LLM stock phrases, no restating the obvious.
- Use simple words, simple explanations, and simple sentence structure. A human should be easily able to parse and understand the writing.
- Don't mention that tests, linting, or type checks were run. That's assumed for anything going into a PR. Only call out testing or validation if it's actually central to what the PR is about (e.g. it's the thing being fixed or added).
- If the PR closes or relates to an open issue, link it as the template asks. Don't include the . at the end of the Closes #NNN.

## 3. Fill out the checklist

Keep the checklist from the template as-is, checking off what applies and deleting items that don't, per the template's own instruction.

## 4. Draft vs. open

Default to drafting only: print the title and body in a fenced block and stop.

Only push and open the PR if the user's request makes that clear (e.g. "open a PR", "create the PR", "submit it"). In that case:
- Push the branch if it isn't already pushed (`git push -u origin <branch>`).
- Run `gh pr create --title "..." --body "..."` with the drafted content.
- Print the resulting PR URL.
