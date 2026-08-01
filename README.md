<p align="center">
  <img src="docs/res/github-graph.png">
</p>

# GitHub Actions Runner

[![Actions Status](https://github.com/actions/runner/workflows/Runner%20CI/badge.svg)](https://github.com/actions/runner/actions)

The runner is the application that runs a job from a GitHub Actions workflow. It is used by GitHub Actions in the [hosted virtual environments](https://github.com/actions/virtual-environments), or you can [self-host the runner](https://help.github.com/en/actions/automating-your-workflow-with-github-actions/about-self-hosted-runners) in your own environment.

## Multi-repository fork

This fork lets **one runner install directory serve multiple repositories** (macOS and Linux). Personal GitHub accounts cannot register org-level runners, so normally each repository needs its own runner install; this fork multiplexes one machine across several repositories with one process, running one job at a time.

### Usage

Run `./config.sh` once per repository in the same directory:

```bash
./config.sh --url https://github.com/you/repo-1 --token AAA...
# Runner will listen to https://github.com/you/repo-1
./config.sh --url https://github.com/you/repo-2 --token BBB...
# Runner will listen to https://github.com/you/repo-2
```

Configuring a repository twice fails with `Error! This runner is already listening to <url>`. Each registration is stored under `.runners/<owner-repo>/` with its own credentials and RSA key; an existing classic single-repo configuration is migrated into that layout automatically when you add a second repository.

Then start the runner interactively:

```bash
./run.sh
```

All registered runners show **Online**. When a job arrives for one repository, the others' sessions are closed so their runners show **Offline** — this lets GitHub route their queued jobs to other available runners while this machine is busy. When the job finishes, all sessions are recreated and listening resumes.

To remove one repository (keep the others):

```bash
./config.sh remove --url https://github.com/you/repo-1 --token CCC...
```

### Limitations

- **Automatic self-update is disabled** for all registrations in multi-repository mode — an automatic update would replace this fork's binaries with stock runner builds. The runner warns at startup (and daily) when a newer runner version exists; rebase and rebuild this fork to update, or GitHub may eventually refuse the old version.
- `--ephemeral`, `--once`, and `--jitconfig` are not supported in multi-repository mode; Windows is single-repository only.
- All registrations share one work folder (`--work` must match), so repositories with the same short name under different owners share a checkout directory (workspaces re-sync between jobs — correct, but slower).
- Service install (`svc.sh`/launchd/systemd) is not reworked for multi-repository mode; run interactively via `./run.sh` (or wrap it in your own supervisor).
- Don't run `config.sh`/`config.sh remove` while `run.sh` is live; restart the runner after configuration changes.

## Get Started

For more information about installing and using self-hosted runners, see [Adding self-hosted runners](https://help.github.com/en/actions/automating-your-workflow-with-github-actions/adding-self-hosted-runners) and [Using self-hosted runners in a workflow](https://help.github.com/en/actions/automating-your-workflow-with-github-actions/using-self-hosted-runners-in-a-workflow)

Runner releases:

![win](docs/res/win_sm.png) [Pre-reqs](docs/start/envwin.md) | [Download](https://github.com/actions/runner/releases)  

![macOS](docs/res/apple_sm.png)  [Pre-reqs](docs/start/envosx.md) | [Download](https://github.com/actions/runner/releases)  

![linux](docs/res/linux_sm.png)  [Pre-reqs](docs/start/envlinux.md) | [Download](https://github.com/actions/runner/releases)

### Note

Thank you for your interest in this GitHub repo, however, right now we are not taking contributions. 

We continue to focus our resources on strategic areas that help our customers be successful while making developers' lives easier. While GitHub Actions remains a key part of this vision, we are allocating resources towards other areas of Actions and are not taking contributions to this repository at this time. The GitHub public roadmap is the best place to follow along for any updates on features we’re working on and what stage they’re in.

We are taking the following steps to better direct requests related to GitHub Actions, including:

1. We will be directing questions and support requests to our [Community Discussions area](https://github.com/orgs/community/discussions/categories/actions)

2. High Priority bugs can be reported through Community Discussions or you can report these to our support team https://support.github.com/contact/bug-report.

3. Security Issues should be handled as per our [SECURITY.md](https://github.com/actions/runner?tab=security-ov-file)

We will still provide security updates for this project and fix major breaking changes during this time.

You are welcome to still raise bugs in this repo.
