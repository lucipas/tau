# Deliver a tau ticket (GitHub Issues)

> This is the skill the factory's hourly ticket loop runs on. It lives in
> the repo so the procedure travels with the project: a fresh agent
> session reads this file, picks one open GitHub issue, and delivers it
> as a branch + PR. It was written from the loop's own failure log —
> every rule below is a mistake the loop actually made.
>
> Name: `swefactory-tau-delivery`. Use it when a session is asked to
> work a tau ticket end to end. Stop and ask the human when a ticket
> conflicts with AGENTS.md, when a Qodo finding needs a product
> decision, or when the loop stalls.

## Take the ticket and open the branch

- Read the tau AGENTS.md file first and follow it for every change in the tau repo.

- List the open issues on lucipas/tau with the gh CLI and read the full description of the ticket you pick.

- Before you pick a ticket, list the full open-PR queue with gh pr list --limit 100 and skip any issue that already has an open PR for it.

- Do not rely on the default short PR listing, because it shows only the newest PRs and misses older ones. A run listed only 20 open PRs, missed PR 222, and duplicated it as PR 274, and the same miss overlapped PR 184 with PR 249.

- Pick a feature ticket first. Take a bug fix only when no feature candidate is workable or when a bug blocks a feature in flight.

- Create a new branch from the latest origin main before you edit any file and name it after the ticket.

- Comment on the issue that work is starting, naming the branch.

- When the queue is empty, think of one useful feature for tau, create it as a new GitHub issue on lucipas/tau, and work on that issue.

- Open PRs with the gh CLI, authenticated as lucipas since 2026-11-05. Use the GitHub connector only as a fallback.

## Implement and validate on the branch

- Implement only what the ticket asks for and stay inside its scope.

- Add or update tests wherever the change is testable.

- Run make lint and make test and treat either failing as a blocker.

- Run the integration suite when the change touches node, protocol, mailbox or Tan behavior.

- Commit with a message that references the GitHub issue number.

- Push the branch and verify the remote head matches your local head before you report the push as done.

- Never merge the PR and never push to main directly.

## Open the PR and re-check the Qodo review

- Open the PR to main with gh pr create, include Closes N in the PR body so the user merge closes the issue, and comment the PR link on the issue.

- Wait for Qodo to post before you call the PR done. Qodo is the only review bot for this loop since 2026-11-16. Do not wait for CodeRabbit or Codex and do not request a review from either one.

- Re-check the PR after a few minutes when Qodo has not posted yet, rather than reporting done on a PR Qodo has not seen.

- Re-read the Qodo review itself when you re-check the PR. Qodo can update its review in place, so a new comment is not the only sign that a re-review landed. Treat an updated review with a new timestamp and changed findings as a landed re-review.

- When a re-review needs to be requested after fix commits, request it from Qodo with agentic_review on the PR. Include targeted guidelines that name the risky area for that PR, such as test determinism, error masking, or migration ordering. Targeted guidelines turned a clean Qodo review on PR 459 into one verified Warning about a migration race.

- If Qodo posts nothing, say so plainly instead of claiming a clean review.

- Triage each Qodo finding on its merits and fix the valid ones with new commits on the same branch.

- Read the diff of every Qodo autofix before you accept it, and re-run the validation after it lands.

- Reject or repair an autofix that corrupts a file or changes behavior beyond the finding it claims to fix, and record what it changed before you report the PR as done. A Qodo autofix on PR 66 duplicated the main block in cli.py, and a Qodo autofix on PR 99 changed read receipts from queueing to skipping for peers without read support.

- Re-run the validation after any fix and confirm the Qodo threads resolve or are answered before you report the PR as done.

- Do not report the PR as done while an unresolved Qodo Critical or Major finding is open.

- Respect a finding the user has accepted as a known wart and do not re-raise it.

## Report the result

- Before you send the report, re-check the live PR and issue state with gh and report only what that check shows.

- Do not trust the run summary or your memory of earlier steps for whether a PR is open, whether tests are green, or whether Qodo posted.

- Never attribute a comment to the user unless the user wrote it in chat or on GitHub. Automation comments posted by a ticket run, a merge run, or a bot are not the user, even when they appear under the user account.

- Never claim the user decided something, requested a review, or fixed a file unless the user said so or did so. Name the run or bot that actually posted the comment or made the fix.

- Send one short message with the ticket id and title, one line on what was done, the PR link and the test results.

- Name any Qodo finding that is still open and what you did about the rest.

- If the run attempted a ticket but could not finish it, send one short message naming the ticket and the blocker instead of opening a broken PR.

## Available connectors

- GitHub via the gh CLI for issues, PRs and reading PR reviews. The GitHub connector is the fallback.

- Use muse.connector_search to find other connectors and muse.skill_search to find related skills.

- If nothing here fits, look on the web for a maintained API or MCP server and set it up with Custom Connectors and MCPs, or build a custom connector.

## Alternatives for better results

- If the GitHub connector fallback is used and an approval prompt appears, ask the user to approve it promptly, because an expired prompt blocks the whole delivery.

- Point the user at the repo AGENTS.md when a ticket conflicts with it, instead of quietly bending the rule.

- Suggest a smaller ticket when the best one is too large or underspecified for a single PR.
