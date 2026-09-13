## 语言要求

本项目面向中文用户。与用户交流、代码注释、文档撰写、向用户提问，均应使用中文。

向用户提问时，应使用 `agentassistant-mcp` 服务器的 `ask_question` 工具（而非内置的 `ask_user_question`）。调用时需传入 `project_directory`（当前项目目录）、`agent_name`、`reasoning_model_name` 以及 `questions` 数组，问题内容用中文。

## Agent skills

### Issue tracker

Issues are tracked as GitHub issues in this repo (uses the `gh` CLI). See `docs/agents/issue-tracker.md`.

### Triage labels

Five canonical triage labels, each label string equal to its name: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
