# Codex CLI

Codex CLI is OpenAI's coding agent that you can run locally from your terminal. It can read, change, and run code on your machine in the selected directory.
It's [open source](https://github.com/openai/codex) and built in Rust for speed and efficiency.

Codex is included with ChatGPT Plus, Pro, Business, Edu, and Enterprise plans. Learn more about [what's included](https://developers.openai.com/codex/pricing).

<YouTubeEmbed
  title="Codex CLI overview"
  videoId="iqNzfK4_meQ"
  class="max-w-md"
/>
<br />

## CLI setup

<CliSetupSteps client:load />

<DocsTip>
  The Codex CLI is available on macOS and Linux. Windows support is
  experimental. For the best Windows experience, use Codex in a WSL workspace
  and follow our <a href="/codex/windows">Windows setup guide</a>.
</DocsTip>

---

## Work with the Codex CLI

<BentoContainer>
  <BentoContent href="/codex/cli/features#running-in-interactive-mode">

### Run Codex interactively

Run `codex` to start an interactive terminal UI (TUI) session.

  </BentoContent>
  <BentoContent href="/codex/cli/features#models-reasoning">

### Control model and reasoning

Use `/model` to switch between GPT-5-Codex and GPT-5, or adjust reasoning levels.

  </BentoContent>
  <BentoContent href="/codex/cli/features#image-inputs">

### Image inputs

Attach screenshots or design specs so Codex reads them alongside your prompt.

  </BentoContent>

  <BentoContent href="/codex/cli/features#running-local-code-review">

### Run local code review

Get your code reviewed by a separate Codex agent before you commit or push your changes.

  </BentoContent>

  <BentoContent href="/codex/cli/features#web-search">

### Web search

Use Codex to search the web and get up-to-date information for your task.

  </BentoContent>

  <BentoContent href="/codex/cli/features#working-with-codex-cloud">

### Codex Cloud tasks

Launch a Codex Cloud task, choose environments, and apply the resulting diffs without leaving your terminal.

  </BentoContent>

  <BentoContent href="/codex/sdk#using-codex-cli-programmatically">

### Scripting Codex

Automate repeatable workflows by scripting Codex with the `exec` command.

  </BentoContent>
  <BentoContent href="/codex/mcp">

### Model Context Protocol

Give Codex access to additional third-party tools and context with Model Context Protocol (MCP).

  </BentoContent>
  
  <BentoContent href="/codex/cli/features#approval-modes">

### Approval modes

Choose the approval mode that matches your comfort level before Codex edits or runs commands.

  </BentoContent>
</BentoContainer>


---


# Codex CLI features

Codex supports workflows beyond chat. Use this guide to learn what each one unlocks and when to use it.

## Running in interactive mode

Codex launches into a full-screen terminal UI that can read your repository, make edits, and run commands as you iterate together. Use it whenever you want a conversational workflow where you can review Codex's actions in real time.

```bash
codex
```

You can also specify an initial prompt on the command line.

```bash
codex "Explain this codebase to me"
```

Once the session is open, you can:

- Send prompts, code snippets, or screenshots (see [image inputs](#image-inputs)) directly into the composer.
- Watch Codex explain its plan before making a change, and approve or reject steps inline.
- Press <kbd>Ctrl</kbd>+<kbd>C</kbd> or use `/exit` to close the interactive session when you're done.

## Resuming conversations

Codex stores your transcripts locally so you can pick up where you left off instead of repeating context. Use the `resume` subcommand when you want to reopen an earlier thread with the same repository state and instructions.

- `codex resume` launches a picker of recent interactive sessions. Highlight a run to see its summary and press <kbd>Enter</kbd> to reopen it.
- `codex resume --all` shows sessions beyond the current working directory, so you can reopen any local run.
- `codex resume --last` skips the picker and jumps straight to your most recent session from the current working directory (add `--all` to ignore the cwd filter).
- `codex resume <SESSION_ID>` targets a specific run. You can copy the ID from the picker, `/status`, or the files under `~/.codex/sessions/`.

Non-interactive automation runs can resume too:

```bash
codex exec resume --last "Fix the race conditions you found"
codex exec resume 7f9f9a2e-1b3c-4c7a-9b0e-.... "Implement the plan"
```

Each resumed run keeps the original transcript, plan history, and approvals, so Codex can use prior context while you supply new instructions. Override the working directory with `--cd` or add extra roots with `--add-dir` if you need to steer the environment before resuming.

## Models and reasoning

Codex defaults to `gpt-5-codex` on macOS and Linux, and `gpt-5` on Windows. Switch models mid-session with the `/model` command, or specify one when launching the CLI.

```bash
codex --model gpt-5-codex
```

[Learn more about the models available in Codex](https://developers.openai.com/codex/models).

## Image inputs

Attach screenshots or design specs so Codex can read image details alongside your prompt. You can paste images into the interactive composer or provide files on the command line.

```bash
codex -i screenshot.png "Explain this error"
```

```bash
codex --image img1.png,img2.jpg "Summarize these diagrams"
```

Codex accepts common formats such as PNG and JPEG. Use comma-separated filenames for two or more images, and combine them with text instructions to add context.

## Running local code review

Type `/review` in the CLI to open Codex's review presets. The CLI launches a dedicated reviewer that reads the diff you select and reports prioritized, actionable findings without touching your working tree. By default it uses the current session model; set `review_model` in `config.toml` to override.

- **Review against a base branch** lets you pick a local branch; Codex finds the merge base against its upstream, diffs your work, and highlights the biggest risks before you open a pull request.
- **Review uncommitted changes** inspects everything that's staged, not staged, or not tracked so you can address issues before committing.
- **Review a commit** lists recent commits and has Codex read the exact change set for the SHA you choose.
- **Custom review instructions** accepts your own wording (for example, "Focus on accessibility regressions") and runs the same reviewer with that prompt.

Each run shows up as its own turn in the transcript, so you can rerun reviews as the code evolves and compare the feedback.

## Web search

Codex ships with a first-party web search tool that stays off until you opt in. Enable it in `~/.codex/config.toml` (or pass the `--search` flag). If you're running in the default sandbox, you can also allow network access:

```toml
[features]
web_search_request = true

[sandbox_workspace_write]
network_access = true
```

Once enabled, Codex can call the search tool when it needs fresh context. You'll see `web_search` items in the transcript or `codex exec --json` output whenever Codex looks something up.

## Running with an input prompt

When you just need a quick answer, run Codex with a single prompt and skip the interactive UI.

```bash
codex "explain this codebase"
```

Codex will read the working directory, craft a plan, and stream the response back to your terminal before exiting. Pair this with flags like `--path` to target a specific directory or `--model` to dial in the behavior up front.

## Shell completions

Speed up everyday usage by installing the generated completion scripts for your shell:

```bash
codex completion bash
codex completion zsh
codex completion fish
```

Run the completion script in your shell configuration file to set up completions for new sessions. For example, if you use `zsh`, you can add the following to the end of your `~/.zshrc` file:

```bash
# ~/.zshrc
eval "$(codex completion zsh)"
```

Start a new session, type `codex`, and press <kbd>Tab</kbd> to see the completions. If you see a `command not found: compdef` error, add `autoload -Uz compinit && compinit` to your `~/.zshrc` file before the `eval "$(codex completion zsh)"` line, then restart your shell.

## Approval modes

Approval modes define how much Codex can do without stopping for confirmation. Use `/approvals` inside an interactive session to switch modes as your comfort level changes.

- **Auto** (default) lets Codex read files, edit, and run commands within the working directory. It still asks before touching anything outside that scope or using the network.
- **Read-only** keeps Codex in a consultative mode. It can browse files but won't make changes or run commands until you approve a plan.
- **Full Access** grants Codex the ability to work across your machine, including network access, without asking. Use it sparingly and only when you trust the repository and task.

Codex always surfaces a transcript of its actions, so you can review or roll back changes with your usual git workflow.

## Scripting Codex

Automate workflows or wire Codex into your existing scripts with the `exec` subcommand. This runs Codex non-interactively, piping the final plan and results back to `stdout`.

```bash
codex exec "fix the CI failure"
```

Combine `exec` with shell scripting to build custom workflows, such as automatically updating changelogs, sorting issues, or enforcing editorial checks before a PR ships.

## Working with Codex cloud

The `codex cloud` command lets you triage and launch [Codex cloud tasks](https://developers.openai.com/codex/cloud) without leaving the terminal. Run it with no arguments to open an interactive picker, browse active or finished tasks, and apply the changes to your local project.

You can also start a task directly from the terminal:

```bash
codex cloud exec --env ENV_ID "Summarize open bugs"
```

Add `--attempts` (1–4) to request best-of-N runs when you want Codex cloud to generate more than one solution. For example, `codex cloud exec --env ENV_ID --attempts 3 "Summarize open bugs"`.

Environment IDs come from your Codex cloud configuration—use `codex cloud` and press <kbd>Ctrl</kbd>+<kbd>O</kbd> to choose an environment or the web dashboard to confirm the exact value. Authentication follows your existing CLI login, and the command exits non-zero if submission fails so you can wire it into scripts or CI.

## Slash commands

Slash commands give you quick access to specialized workflows like `/review`, `/fork`, or your own reusable prompts. Codex ships with a curated set of built-ins, and you can create custom ones for team-specific tasks or personal shortcuts.

See the [slash commands guide](https://developers.openai.com/codex/guides/slash-commands) to browse the catalog of built-ins, learn how to author custom commands, and understand where they live on disk.

## Prompt editor

When you're drafting a longer prompt, it can be easier to switch to a full editor and then send the result back to the composer.

In the prompt input, press <kbd>Ctrl</kbd>+<kbd>G</kbd> to open the editor defined by the `VISUAL` environment variable (or `EDITOR` if `VISUAL` isn't set).

## Model Context Protocol (MCP)

Connect Codex to more tools by configuring Model Context Protocol servers. Add STDIO or streaming HTTP servers in `~/.codex/config.toml`, or manage them with the `codex mcp` CLI commands—Codex launches them automatically when a session starts and exposes their tools next to the built-ins. You can even run Codex itself as an MCP server when you need it inside another agent.

See [Model Context Protocol](https://developers.openai.com/codex/mcp) for example configurations, supported auth flows, and a more detailed guide.

## Tips and shortcuts

- Type `@` in the composer to open a fuzzy file search over the workspace root; press <kbd>Tab</kbd> or <kbd>Enter</kbd> to drop the highlighted path into your message.
- Press <kbd>Enter</kbd> while Codex is running to inject new instructions into the current turn, or press <kbd>Tab</kbd> to queue a follow-up prompt for the next turn.
- Prefix a line with `!` to run a local shell command (for example, `!ls`). Codex treats the output like a user-provided command result and still applies your approval and sandbox settings.
- Tap <kbd>Esc</kbd> twice while the composer is empty to edit your previous user message. Continue pressing <kbd>Esc</kbd> to walk further back in the transcript, then hit <kbd>Enter</kbd> to fork from that point.
- Launch Codex from any directory using `codex --cd <path>` to set the working root without running `cd` first. The active path appears in the TUI header.
- Expose more writable roots with `--add-dir` (for example, `codex --cd apps/frontend --add-dir ../backend --add-dir ../shared`) when you need to coordinate changes across more than one project.
- Make sure your environment is already set up before launching Codex so it does not spend tokens probing what to activate. For example, source your Python venv (or other language runtimes), start any required daemons, and export the env vars you expect to use ahead of time.


---


# Command line options

export const globalFlagOptions = [
  {
    key: "PROMPT",
    type: "string",
    description:
      "Optional text instruction to start the session. Omit to launch the TUI without a pre-filled message.",
  },
  {
    key: "--image, -i",
    type: "path[,path...]",
    description:
      "Attach one or more image files to the initial prompt. Separate multiple paths with commas or repeat the flag.",
  },
  {
    key: "--model, -m",
    type: "string",
    description:
      "Override the model set in configuration (for example `gpt-5-codex`).",
  },
  {
    key: "--oss",
    type: "boolean",
    defaultValue: "false",
    description:
      'Use the local open source model provider (equivalent to `-c model_provider="oss"`). Validates that Ollama is running.',
  },
  {
    key: "--profile, -p",
    type: "string",
    description:
      "Configuration profile name to load from `~/.codex/config.toml`.",
  },
  {
    key: "--sandbox, -s",
    type: "read-only | workspace-write | danger-full-access",
    description:
      "Select the sandbox policy for model-generated shell commands.",
  },
  {
    key: "--ask-for-approval, -a",
    type: "untrusted | on-failure | on-request | never",
    description:
      "Control when Codex pauses for human approval before running a command.",
  },
  {
    key: "--full-auto",
    type: "boolean",
    defaultValue: "false",
    description:
      "Shortcut for low-friction local work: sets `--ask-for-approval on-request` and `--sandbox workspace-write`.",
  },
  {
    key: "--dangerously-bypass-approvals-and-sandbox, --yolo",
    type: "boolean",
    defaultValue: "false",
    description:
      "Run every command without approvals or sandboxing. Only use inside an externally hardened environment.",
  },
  {
    key: "--cd, -C",
    type: "path",
    description:
      "Set the working directory for the agent before it starts processing your request.",
  },
  {
    key: "--search",
    type: "boolean",
    defaultValue: "false",
    description:
      "Enable web search. When true, the agent can call the `web_search` tool without asking every time.",
  },
  {
    key: "--add-dir",
    type: "path",
    description:
      "Grant additional directories write access alongside the main workspace. Repeat for multiple paths.",
  },
  {
    key: "--no-alt-screen",
    type: "boolean",
    defaultValue: "false",
    description:
      "Disable alternate screen mode for the TUI (overrides `tui.alternate_screen` for this run).",
  },
  {
    key: "--enable",
    type: "feature",
    description:
      "Force-enable a feature flag (translates to `-c features.<name>=true`). Repeatable.",
  },
  {
    key: "--disable",
    type: "feature",
    description:
      "Force-disable a feature flag (translates to `-c features.<name>=false`). Repeatable.",
  },
  {
    key: "--config, -c",
    type: "key=value",
    description:
      "Override configuration values. Values parse as JSON if possible; otherwise the literal string is used.",
  },
];

export const commandOverview = [
  {
    key: "codex",
    href: "/codex/cli/reference#codex-interactive",
    type: "stable",
    description:
      "Launch the terminal UI. Accepts the global flags above plus an optional prompt or image attachments.",
  },
  {
    key: "codex app-server",
    href: "/codex/cli/reference#codex-app-server",
    type: "experimental",
    description:
      "Launch the Codex app server for local development or debugging.",
  },
  {
    key: "codex apply",
    href: "/codex/cli/reference#codex-apply",
    type: "stable",
    description:
      "Apply the latest diff generated by a Codex Cloud task to your local working tree. Alias: `codex a`.",
  },
  {
    key: "codex cloud",
    href: "/codex/cli/reference#codex-cloud",
    type: "experimental",
    description:
      "Browse or execute Codex Cloud tasks from the terminal without opening the TUI. Alias: `codex cloud-tasks`.",
  },
  {
    key: "codex completion",
    href: "/codex/cli/reference#codex-completion",
    type: "stable",
    description:
      "Generate shell completion scripts for Bash, Zsh, Fish, or PowerShell.",
  },
  {
    key: "codex exec",
    href: "/codex/cli/reference#codex-exec",
    type: "stable",
    description:
      "Run Codex non-interactively. Alias: `codex e`. Stream results to stdout or JSONL and optionally resume previous sessions.",
  },
  {
    key: "codex execpolicy",
    href: "/codex/cli/reference#codex-execpolicy",
    type: "experimental",
    description:
      "Evaluate execpolicy rule files and see whether a command would be allowed, prompted, or blocked.",
  },
  {
    key: "codex login",
    href: "/codex/cli/reference#codex-login",
    type: "stable",
    description:
      "Authenticate Codex using ChatGPT OAuth, device auth, or an API key piped over stdin.",
  },
  {
    key: "codex logout",
    href: "/codex/cli/reference#codex-logout",
    type: "stable",
    description: "Remove stored authentication credentials.",
  },
  {
    key: "codex mcp",
    href: "/codex/cli/reference#codex-mcp",
    type: "experimental",
    description:
      "Manage Model Context Protocol servers (list, add, remove, authenticate).",
  },
  {
    key: "codex mcp-server",
    href: "/codex/cli/reference#codex-mcp-server",
    type: "experimental",
    description:
      "Run Codex itself as an MCP server over stdio. Useful when another agent consumes Codex.",
  },
  {
    key: "codex resume",
    href: "/codex/cli/reference#codex-resume",
    type: "stable",
    description:
      "Continue a previous interactive session by ID or resume the most recent conversation.",
  },
  {
    key: "codex fork",
    href: "/codex/cli/reference#codex-fork",
    type: "stable",
    description:
      "Fork a previous interactive session into a new thread, preserving the original transcript.",
  },
  {
    key: "codex sandbox",
    href: "/codex/cli/reference#codex-sandbox",
    type: "experimental",
    description:
      "Run arbitrary commands inside Codex-provided macOS seatbelt or Linux landlock sandboxes.",
  },
];

export const execOptions = [
  {
    key: "PROMPT",
    type: "string | - (read stdin)",
    description:
      "Initial instruction for the task. Use `-` to pipe the prompt from stdin.",
  },
  {
    key: "--image, -i",
    type: "path[,path...]",
    description:
      "Attach images to the first message. Repeatable; supports comma-separated lists.",
  },
  {
    key: "--model, -m",
    type: "string",
    description: "Override the configured model for this run.",
  },
  {
    key: "--oss",
    type: "boolean",
    defaultValue: "false",
    description:
      "Use the local open source provider (requires a running Ollama instance).",
  },
  {
    key: "--sandbox, -s",
    type: "read-only | workspace-write | danger-full-access",
    description:
      "Sandbox policy for model-generated commands. Defaults to configuration.",
  },
  {
    key: "--profile, -p",
    type: "string",
    description: "Select a configuration profile defined in config.toml.",
  },
  {
    key: "--full-auto",
    type: "boolean",
    defaultValue: "false",
    description:
      "Apply the low-friction automation preset (`workspace-write` sandbox and `on-request` approvals).",
  },
  {
    key: "--dangerously-bypass-approvals-and-sandbox, --yolo",
    type: "boolean",
    defaultValue: "false",
    description:
      "Bypass approval prompts and sandboxing. Dangerous—only use inside an isolated runner.",
  },
  {
    key: "--cd, -C",
    type: "path",
    description: "Set the workspace root before executing the task.",
  },
  {
    key: "--skip-git-repo-check",
    type: "boolean",
    defaultValue: "false",
    description:
      "Allow running outside a Git repository (useful for one-off directories).",
  },
  {
    key: "--output-schema",
    type: "path",
    description:
      "JSON Schema file describing the expected final response shape. Codex validates tool output against it.",
  },
  {
    key: "--color",
    type: "always | never | auto",
    defaultValue: "auto",
    description: "Control ANSI color in stdout.",
  },
  {
    key: "--json, --experimental-json",
    type: "boolean",
    defaultValue: "false",
    description:
      "Print newline-delimited JSON events instead of formatted text.",
  },
  {
    key: "--output-last-message, -o",
    type: "path",
    description:
      "Write the assistant’s final message to a file. Useful for downstream scripting.",
  },
  {
    key: "Resume subcommand",
    type: "codex exec resume [SESSION_ID]",
    description:
      "Resume an exec session by ID or add `--last` to continue the most recent session from the current working directory. Add `--all` to consider sessions from any directory. Accepts an optional follow-up prompt.",
  },
  {
    key: "-c, --config",
    type: "key=value",
    description:
      "Inline configuration override for the non-interactive run (repeatable).",
  },
];

export const resumeOptions = [
  {
    key: "SESSION_ID",
    type: "uuid",
    description:
      "Resume the specified session. Omit and use `--last` to continue the most recent session.",
  },
  {
    key: "--last",
    type: "boolean",
    defaultValue: "false",
    description:
      "Skip the picker and resume the most recent conversation from the current working directory.",
  },
  {
    key: "--all",
    type: "boolean",
    defaultValue: "false",
    description:
      "Include sessions outside the current working directory when selecting the most recent session.",
  },
];

export const execResumeOptions = [
  {
    key: "SESSION_ID",
    type: "uuid",
    description:
      "Resume the specified session. Omit and use `--last` to continue the most recent session.",
  },
  {
    key: "--last",
    type: "boolean",
    defaultValue: "false",
    description:
      "Resume the most recent conversation from the current working directory.",
  },
  {
    key: "--all",
    type: "boolean",
    defaultValue: "false",
    description:
      "Include sessions outside the current working directory when selecting the most recent session.",
  },
  {
    key: "--image, -i",
    type: "path[,path...]",
    description:
      "Attach one or more images to the follow-up prompt. Separate multiple paths with commas or repeat the flag.",
  },
  {
    key: "PROMPT",
    type: "string | - (read stdin)",
    description:
      "Optional follow-up instruction sent immediately after resuming.",
  },
];

export const forkOptions = [
  {
    key: "SESSION_ID",
    type: "uuid",
    description:
      "Fork the specified session. Omit and use `--last` to fork the most recent session.",
  },
  {
    key: "--last",
    type: "boolean",
    defaultValue: "false",
    description:
      "Skip the picker and fork the most recent conversation automatically.",
  },
  {
    key: "--all",
    type: "boolean",
    defaultValue: "false",
    description:
      "Show sessions beyond the current working directory in the picker.",
  },
];

export const execpolicyOptions = [
  {
    key: "--rules, -r",
    type: "path (repeatable)",
    description:
      "Path to an execpolicy rule file to evaluate. Provide multiple flags to combine rules across files.",
  },
  {
    key: "--pretty",
    type: "boolean",
    defaultValue: "false",
    description: "Pretty-print the JSON result.",
  },
  {
    key: "COMMAND...",
    type: "var-args",
    description: "Command to be checked against the specified policies.",
  },
];

export const loginOptions = [
  {
    key: "--with-api-key",
    type: "boolean",
    description:
      "Read an API key from stdin (for example `printenv OPENAI_API_KEY | codex login --with-api-key`).",
  },
  {
    key: "--device-auth",
    type: "boolean",
    description:
      "Use OAuth device code flow instead of launching a browser window.",
  },
  {
    key: "status subcommand",
    type: "codex login status",
    description:
      "Print the active authentication mode and exit with 0 when logged in.",
  },
];

export const applyOptions = [
  {
    key: "TASK_ID",
    type: "string",
    description:
      "Identifier of the Codex Cloud task whose diff should be applied.",
  },
];

export const sandboxMacOptions = [
  {
    key: "--full-auto",
    type: "boolean",
    defaultValue: "false",
    description:
      "Grant write access to the current workspace and `/tmp` without approvals.",
  },
  {
    key: "--config, -c",
    type: "key=value",
    description:
      "Pass configuration overrides into the sandboxed run (repeatable).",
  },
  {
    key: "COMMAND...",
    type: "var-args",
    description:
      "Shell command to execute under macOS Seatbelt. Everything after `--` is forwarded.",
  },
];

export const sandboxLinuxOptions = [
  {
    key: "--full-auto",
    type: "boolean",
    defaultValue: "false",
    description:
      "Grant write access to the current workspace and `/tmp` inside the Landlock sandbox.",
  },
  {
    key: "--config, -c",
    type: "key=value",
    description:
      "Configuration overrides applied before launching the sandbox (repeatable).",
  },
  {
    key: "COMMAND...",
    type: "var-args",
    description:
      "Command to execute under Landlock + seccomp. Provide the executable after `--`.",
  },
];

export const completionOptions = [
  {
    key: "SHELL",
    type: "bash | zsh | fish | power-shell | elvish",
    defaultValue: "bash",
    description: "Shell to generate completions for. Output prints to stdout.",
  },
];

export const cloudExecOptions = [
  {
    key: "QUERY",
    type: "string",
    description:
      "Task prompt. If omitted, Codex prompts interactively for details.",
  },
  {
    key: "--env",
    type: "ENV_ID",
    description:
      "Target Codex Cloud environment identifier (required). Use `codex cloud` to list options.",
  },
  {
    key: "--attempts",
    type: "1-4",
    defaultValue: "1",
    description:
      "Number of assistant attempts (best-of-N) Codex Cloud should run.",
  },
];

export const cloudListOptions = [
  {
    key: "--env",
    type: "ENV_ID",
    description: "Filter tasks by environment identifier.",
  },
  {
    key: "--limit",
    type: "1-20",
    defaultValue: "20",
    description: "Maximum number of tasks to return.",
  },
  {
    key: "--cursor",
    type: "string",
    description: "Pagination cursor returned by a previous request.",
  },
  {
    key: "--json",
    type: "boolean",
    defaultValue: "false",
    description: "Emit machine-readable JSON instead of plain text.",
  },
];

export const mcpCommands = [
  {
    key: "list",
    type: "--json",
    description:
      "List configured MCP servers. Add `--json` for machine-readable output.",
  },
  {
    key: "get <name>",
    type: "--json",
    description:
      "Show a specific server configuration. `--json` prints the raw config entry.",
  },
  {
    key: "add <name>",
    type: "-- <command...> | --url <value>",
    description:
      "Register a server using a stdio launcher command or a streamable HTTP URL. Supports `--env KEY=VALUE` for stdio transports.",
  },
  {
    key: "remove <name>",
    description: "Delete a stored MCP server definition.",
  },
  {
    key: "login <name>",
    type: "--scopes scope1,scope2",
    description:
      "Start an OAuth login for a streamable HTTP server (servers that support OAuth only).",
  },
  {
    key: "logout <name>",
    description:
      "Remove stored OAuth credentials for a streamable HTTP server.",
  },
];

export const mcpAddOptions = [
  {
    key: "COMMAND...",
    type: "stdio transport",
    description:
      "Executable plus arguments to launch the MCP server. Provide after `--`.",
  },
  {
    key: "--env KEY=VALUE",
    type: "repeatable",
    description:
      "Environment variable assignments applied when launching a stdio server.",
  },
  {
    key: "--url",
    type: "https://…",
    description:
      "Register a streamable HTTP server instead of stdio. Mutually exclusive with `COMMAND...`.",
  },
  {
    key: "--bearer-token-env-var",
    type: "ENV_VAR",
    description:
      "Environment variable whose value is sent as a bearer token when connecting to a streamable HTTP server.",
  },
];

## How to read this reference

This page catalogs every documented Codex CLI command and flag. Use the interactive tables to search by key or description. Each section indicates whether the option is stable or experimental and calls out risky combinations.

<DocsTip>
  The CLI inherits most defaults from <code>~/.codex/config.toml</code>. Any
  <code>-c key=value</code> overrides you pass at the command line take
  precedence for that invocation. See [Config
  basics](https://developers.openai.com/codex/config-basic#configuration-precedence) for more information.
</DocsTip>

## Global flags

<ConfigTable client:load options={globalFlagOptions} />

These options apply to the base `codex` command and propagate to each subcommand unless a section below specifies otherwise.
When using subcommands, place global flags after the subcommand (for example, `codex exec --oss ...`) to ensure they are applied as intended.

## Command overview

<DocsTip>
  The Maturity column uses feature maturity labels such as Experimental, Beta,
  and Stable. See [Feature Maturity](https://developers.openai.com/codex/feature-maturity) for how to
  interpret these labels.
</DocsTip>

<ConfigTable
  client:load
  options={commandOverview}
  secondColumnTitle="Maturity"
  secondColumnVariant="maturity"
/>

## Command details

### `codex` (interactive)

Running `codex` with no subcommand launches the interactive terminal UI (TUI). The agent accepts the global flags above plus image attachments. Use `--search` to enable web browsing and `--full-auto` to let Codex run most commands without prompts.

### `codex app-server`

Launch the Codex app server locally. This is primarily for development and debugging and may change without notice.

### `codex apply`

Apply the most recent diff from a Codex cloud task to your local repository. You must authenticate and have access to the task.

<ConfigTable client:load options={applyOptions} />

Codex prints the patched files and exits non-zero if `git apply` fails (for example, due to conflicts).

### `codex cloud`

Interact with Codex cloud tasks from the terminal. The default command opens an interactive picker; `codex cloud exec` submits a task directly, and `codex cloud list` returns recent tasks for scripting or quick inspection.

<ConfigTable client:load options={cloudExecOptions} />

Authentication follows the same credentials as the main CLI. Codex exits non-zero if the task submission fails.

#### `codex cloud list`

List recent Codex Cloud tasks with optional filtering and pagination.

<ConfigTable client:load options={cloudListOptions} />

Plain-text output prints a task URL followed by status details. Use `--json` for automation. The JSON payload contains a `tasks` array plus an optional `cursor` value. Each task includes `id`, `url`, `title`, `status`, `updated_at`, `environment_id`, `environment_label`, `summary`, `is_review`, and `attempt_total`.

### `codex completion`

Generate shell completion scripts and redirect the output to the appropriate location, for example `codex completion zsh > "${fpath[1]}/_codex"`.

<ConfigTable client:load options={completionOptions} />

### `codex exec`

Use `codex exec` (or the short form `codex e`) for scripted or CI-style runs that should finish without human interaction.

<ConfigTable client:load options={execOptions} />

Codex writes formatted output by default. Add `--json` to receive newline-delimited JSON events (one per state change). The optional `resume` subcommand lets you continue non-interactive tasks. Use `--last` to pick the most recent session from the current working directory, or add `--all` to search across all sessions:

<ConfigTable client:load options={execResumeOptions} />

### `codex execpolicy`

Check `execpolicy` rule files before you save them. `codex execpolicy check` accepts one or more `--rules` flags (for example, files under `~/.codex/rules`) and emits JSON showing the strictest decision and any matching rules. Add `--pretty` to format the output. The `execpolicy` command is currently in preview.

<ConfigTable client:load options={execpolicyOptions} />

### `codex login`

Authenticate the CLI with a ChatGPT account or API key. With no flags, Codex opens a browser for the ChatGPT OAuth flow.

<ConfigTable client:load options={loginOptions} />

`codex login status` exits with `0` when credentials are present, which is helpful in automation scripts.

### `codex logout`

Remove saved credentials for both API key and ChatGPT authentication. This command has no flags.

### `codex mcp`

Manage Model Context Protocol server entries stored in `~/.codex/config.toml`.

<ConfigTable client:load options={mcpCommands} />

The `add` subcommand supports both stdio and streamable HTTP transports:

<ConfigTable client:load options={mcpAddOptions} />

OAuth actions (`login`, `logout`) only work with streamable HTTP servers (and only when the server supports OAuth).

### `codex mcp-server`

Run Codex as an MCP server over stdio so that other tools can connect. This command inherits global configuration overrides and exits when the downstream client closes the connection.

### `codex resume`

Continue an interactive session by ID or resume the most recent conversation. `codex resume` scopes `--last` to the current working directory unless you pass `--all`. It accepts the same global flags as `codex`, including model and sandbox overrides.

<ConfigTable client:load options={resumeOptions} />

### `codex fork`

Fork a previous interactive session into a new thread. By default, `codex fork` opens the session picker; add `--last` to fork your most recent session instead.

<ConfigTable client:load options={forkOptions} />

### `codex sandbox`

Use the sandbox helper to run a command under the same policies Codex uses internally.

#### macOS seatbelt

<ConfigTable client:load options={sandboxMacOptions} />

#### Linux Landlock

<ConfigTable client:load options={sandboxLinuxOptions} />

## Flag combinations and safety tips

- Set `--full-auto` for unattended local work, but avoid combining it with `--dangerously-bypass-approvals-and-sandbox` unless you are inside a dedicated sandbox VM.
- When you need to grant Codex write access to more directories, prefer `--add-dir` rather than forcing `--sandbox danger-full-access`.
- Pair `--json` with `--output-last-message` in CI to capture machine-readable progress and a final natural-language summary.

## Related resources

- [Codex CLI overview](https://developers.openai.com/codex/cli): installation, upgrades, and quick tips.
- [Config basics](https://developers.openai.com/codex/config-basic): persist defaults like the model and provider.
- [Advanced Config](https://developers.openai.com/codex/config-advanced): profiles, providers, sandbox tuning, and integrations.
- [AGENTS.md](https://developers.openai.com/codex/guides/agents-md): conceptual overview of Codex agent capabilities and best practices.


---


# Slash commands in Codex CLI

Slash commands give you fast, keyboard-first control over Codex. Type `/` in the composer to open the slash popup, choose a command, and Codex will perform actions such as switching models, adjusting approvals, or summarizing long conversations without leaving the terminal.

This guide shows you how to:

- Find the right built-in slash command for a task
- Steer an active session with commands like `/model`, `/approvals`, and `/status`

## Built-in slash commands

Codex ships with the following commands. Open the slash popup and start typing the command name to filter the list.

| Command                                                 | Purpose                                                         | When to use it                                                                                            |
| ------------------------------------------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| [`/approvals`](#update-approval-rules-with-approvals)   | Set what Codex can do without asking first.                     | Relax or tighten approval requirements mid-session, such as switching between Auto and Read Only.         |
| [`/compact`](#keep-transcripts-lean-with-compact)       | Summarize the visible conversation to free tokens.              | Use after long runs so Codex retains key points without blowing the context window.                       |
| [`/diff`](#review-changes-with-diff)                    | Show the Git diff, including files Git isn't tracking yet.      | Review Codex's edits before you commit or run tests.                                                      |
| [`/exit`](#exit-the-cli-with-quit-or-exit)              | Exit the CLI (same as `/quit`).                                 | Alternative spelling; both commands exit the session.                                                     |
| [`/feedback`](#send-feedback-with-feedback)             | Send logs to the Codex maintainers.                             | Report issues or share diagnostics with support.                                                          |
| [`/init`](#generate-agentsmd-with-init)                 | Generate an `AGENTS.md` scaffold in the current directory.      | Capture persistent instructions for the repository or subdirectory you're working in.                     |
| [`/logout`](#sign-out-with-logout)                      | Sign out of Codex.                                              | Clear local credentials when using a shared machine.                                                      |
| [`/mcp`](#list-mcp-tools-with-mcp)                      | List configured Model Context Protocol (MCP) tools.             | Check which external tools Codex can call during the session.                                             |
| [`/mention`](#highlight-files-with-mention)             | Attach a file to the conversation.                              | Point Codex at specific files or folders you want it to inspect next.                                     |
| [`/model`](#set-the-active-model-with-model)            | Choose the active model (and reasoning effort, when available). | Switch between general-purpose models (`gpt-4.1-mini`) and deeper reasoning models before running a task. |
| [`/fork`](#fork-the-current-conversation-with-fork)     | Fork the current conversation into a new thread.                | Branch the active session to explore a new approach without losing the current transcript.                |
| [`/resume`](#resume-a-saved-conversation-with-resume)   | Resume a saved conversation from your session list.             | Continue work from a previous CLI session without starting over.                                          |
| [`/new`](#start-a-new-conversation-with-new)            | Start a new conversation inside the same CLI session.           | Reset the chat context without leaving the CLI when you want a fresh prompt in the same repo.             |
| [`/quit`](#exit-the-cli-with-quit-or-exit)              | Exit the CLI.                                                   | Leave the session immediately.                                                                            |
| [`/review`](#ask-for-a-working-tree-review-with-review) | Ask Codex to review your working tree.                          | Run after Codex completes work or when you want a second set of eyes on local changes.                    |
| [`/status`](#inspect-the-session-with-status)           | Display session configuration and token usage.                  | Confirm the active model, approval policy, writable roots, and remaining context capacity.                |

`/quit` and `/exit` both exit the CLI. Use them only after you have saved or committed any important work.

## Control your session with slash commands

The following workflows keep your session on track without restarting Codex.

### Set the active model with `/model`

1. Start Codex and open the composer.
2. Type `/model` and press Enter.
3. Choose a model such as `gpt-4.1-mini` or `gpt-4.1` from the popup.

Expected: Codex confirms the new model in the transcript. Run `/status` to verify the change.

### Update approval rules with `/approvals`

1. Type `/approvals` and press Enter.
2. Select the approval preset that matches your comfort level, for example `Auto` for hands-off runs or `Read Only` to review edits.

Expected: Codex announces the updated policy. Future actions respect the new approval mode until you change it again.

### Inspect the session with `/status`

1. In any conversation, type `/status`.
2. Review the output for the active model, approval policy, writable roots, and current token usage.

Expected: You see a summary like what `codex status` prints in the shell, confirming Codex is operating where you expect.

### Keep transcripts lean with `/compact`

1. After a long exchange, type `/compact`.
2. Confirm when Codex offers to summarize the conversation so far.

Expected: Codex replaces earlier turns with a concise summary, freeing context while keeping critical details.

### Review changes with `/diff`

1. Type `/diff` to inspect the Git diff.
2. Scroll through the output inside the CLI to review edits and added files.

Expected: Codex shows changes you've staged, changes you haven't staged yet, and files Git hasn't started tracking, so you can decide what to keep.

### Highlight files with `/mention`

1. Type `/mention` followed by a path, for example `/mention src/lib/api.ts`.
2. Select the matching result from the popup.

Expected: Codex adds the file to the conversation, ensuring follow-up turns reference it directly.

### Start a new conversation with `/new`

1. Type `/new` and press Enter.

Expected: Codex starts a fresh conversation in the same CLI session, so you can switch tasks without leaving your terminal.

### Resume a saved conversation with `/resume`

1. Type `/resume` and press Enter.
2. Choose the session you want from the saved-session picker.

Expected: Codex reloads the selected conversation’s transcript so you can pick up where you left off, keeping the original history intact.

### Fork the current conversation with `/fork`

1. Type `/fork` and press Enter.

Expected: Codex clones the current conversation into a new thread with a fresh ID, leaving the original transcript untouched so you can explore an alternative approach in parallel.

If you need to fork a saved session instead of the current one, run `codex fork` in your terminal to open the session picker.

### Generate `AGENTS.md` with `/init`

1. Run `/init` in the directory where you want Codex to look for persistent instructions.
2. Review the generated `AGENTS.md`, then edit it to match your repository conventions.

Expected: Codex creates an `AGENTS.md` scaffold you can refine and commit for future sessions.

### Ask for a working tree review with `/review`

1. Type `/review`.
2. Follow up with `/diff` if you want to inspect the exact file changes.

Expected: Codex summarizes issues it finds in your working tree, focusing on behavior changes and missing tests. It uses the current session model unless you set `review_model` in `config.toml`.

### List MCP tools with `/mcp`

1. Type `/mcp`.
2. Review the list to confirm which MCP servers and tools are available.

Expected: You see the configured Model Context Protocol (MCP) tools Codex can call in this session.

### Send feedback with `/feedback`

1. Type `/feedback` and press Enter.
2. Follow the prompts to include logs or diagnostics.

Expected: Codex collects the requested diagnostics and submits them to the maintainers.

### Sign out with `/logout`

1. Type `/logout` and press Enter.

Expected: Codex clears local credentials for the current user session.

### Exit the CLI with `/quit` or `/exit`

1. Type `/quit` (or `/exit`) and press Enter.

Expected: Codex exits immediately. Save or commit any important work first.


---


# Config basics

Codex reads default settings from [Team Config](https://developers.openai.com/codex/team-config) layers. Your personal defaults live in `~/.codex/config.toml`. Use this file to change defaults (like the model), set approval and sandbox behavior, and configure MCP servers.

## Codex configuration file

Codex stores your user configuration at `~/.codex/config.toml`.

To open the configuration file from the Codex IDE extension, select the gear icon in the top-right corner, then select **Codex Settings > Open config.toml**.

The CLI and IDE extension share the same `config.toml` file. To share defaults across a repo or team, store a `config.toml` under `.codex/` in a Team Config location. You can use `config.toml` to:

- Set the default model and provider.
- Configure [approval policies and sandbox settings](https://developers.openai.com/codex/security).
- Configure [MCP servers](https://developers.openai.com/codex/mcp).

## Configuration precedence

Codex builds the configuration stack from [Team Config](https://developers.openai.com/codex/team-config) locations, then resolves values in this order:

1. CLI flags (for example, `--model`)
2. [Profile](https://developers.openai.com/codex/config-advanced#profiles) values (from `--profile <name>`)
3. Values in `config.toml` (merged across Team Config locations)
4. Built-in defaults

When Codex merges profile and root-level values, it resolves Team Config locations in this order (highest to lowest):

1. `$CWD/.codex/` (current working directory)
2. `$CWD/../.codex/` (parent folders above CWD when inside a repo)
3. `$REPO_ROOT/.codex/` (repo root when inside a repo)
4. `$CODEX_HOME` (defaults to `~/.codex/`)
5. `/etc/codex/`

Use that precedence to set shared defaults at the top level and keep profiles focused on the values that differ.

For one-off overrides via `-c`/`--config` (including TOML quoting rules), see [Advanced Config](https://developers.openai.com/codex/config-advanced#one-off-overrides-from-the-cli).

<DocsTip>
  On managed machines, your organization may also enforce constraints via
  `requirements.toml` (for example, disallowing `approval_policy = "never"` or
  `sandbox_mode = "danger-full-access"`). See [Security](https://developers.openai.com/codex/security).
</DocsTip>

## Common configuration options

Here are a few options people change most often:

#### Default model

Choose the model Codex uses by default in the CLI and IDE.

```toml
model = "gpt-5.2"
```

#### Approval prompts

Control when Codex pauses to ask before running generated commands.

```toml
approval_policy = "on-request"
```

#### Sandbox level

Adjust how much filesystem and network access Codex has while executing commands.

```toml
sandbox_mode = "workspace-write"
```

#### Reasoning effort

Tune how much reasoning effort the model applies when supported.

```toml
model_reasoning_effort = "high"
```

#### Command environment

Restrict or expand which environment variables are forwarded to spawned commands.

```toml
[shell_environment_policy]
include_only = ["PATH", "HOME"]
```

## Feature flags

Optional and experimental capabilities are toggled via the `[features]` table in `config.toml`.

```toml
[features]
shell_snapshot = true           # Speed up repeated commands
web_search_request = true       # Allow the model to request web searches
```

### Supported features

| Key                            | Default | Maturity     | Description                                                   |
| ------------------------------ | :-----: | ------------ | ------------------------------------------------------------- |
| `apply_patch_freeform`         |  false  | Experimental | Include the freeform `apply_patch` tool                       |
| `elevated_windows_sandbox`     |  false  | Experimental | Use the elevated Windows sandbox pipeline                     |
| `exec_policy`                  |  true   | Experimental | Enforce rules checks for `shell`/`unified_exec`               |
| `experimental_windows_sandbox` |  false  | Experimental | Use the Windows restricted-token sandbox                      |
| `remote_compaction`            |  true   | Experimental | Enable remote compaction (ChatGPT auth only)                  |
| `remote_models`                |  false  | Experimental | Refresh remote model list before showing readiness            |
| `shell_snapshot`               |  false  | Beta         | Snapshot your shell environment to speed up repeated commands |
| `shell_tool`                   |  true   | Stable       | Enable the default `shell` tool                               |
| `unified_exec`                 |  false  | Beta         | Use the unified PTY-backed exec tool                          |
| `undo`                         |  true   | Stable       | Enable undo via per-turn git ghost snapshots                  |
| `web_search_request`           |  false  | Stable       | Allow the model to issue web searches                         |

<DocsTip>
  The Maturity column uses feature maturity labels such as Experimental, Beta,
  and Stable. See [Feature Maturity](https://developers.openai.com/codex/feature-maturity) for how to
  interpret these labels.
</DocsTip>

<DocsTip>Omit feature keys to keep their defaults.</DocsTip>

### Enabling features quickly

- In `config.toml`, add `feature_name = true` under `[features]`.
- From the CLI, run `codex --enable feature_name`.
- To enable multiple features, run `codex --enable feature_a --enable feature_b`.
- To disable a feature, set the key to `false` in `config.toml`.


---


# Team Config

Team Config groups the files teams use to standardize Codex for their organization. Use it to share defaults, rules, and skills without duplicating setup in every local configuration.

## What Team Config includes

| Type                                 | Path          | Use it to                                                                    |
| ------------------------------------ | ------------- | ---------------------------------------------------------------------------- |
| [Config basics](https://developers.openai.com/codex/config-basic) | `config.toml` | Set defaults for sandbox mode, approvals, model, reasoning effort, and more. |
| [Rules](https://developers.openai.com/codex/rules)                | `rules/`      | Control which commands Codex can run outside the sandbox.                    |
| [Skills](https://developers.openai.com/codex/skills)              | `skills/`     | Make shared skills available to your team.                                   |

## Where Team Config lives

Codex loads Team Config from these locations in order of precedence (highest to lowest):

1. `$CWD/.codex/` (current working directory)
2. `$CWD/../.codex/` (parent folders above CWD when inside a repo)
3. `$REPO_ROOT/.codex/` (repo root when inside a repo)
4. `$CODEX_HOME` (defaults to `~/.codex/`)
5. `/etc/codex/`

Each location can include `config.toml`, `rules/`, and `skills/`.

## Requirements enforce constraints

Use `requirements.toml` to constrain defaults like `sandbox_mode` and `approval_policy`. Requirements override all defaults, regardless of location, and the UI prevents selecting conflicting values.

<DocsTip>For platform-specific setup and examples, see <a href="/codex/security#admin-enforced-requirements-requirementstoml">Security</a>.</DocsTip>

## Supported clients

Team Config works in:

- CLI
- IDE extensions (VS Code, Cursor, and the Codex app)

## FAQ

### Difference between requirements and admin defaults

Admin defaults are a baseline for new machines or automation. Higher-precedence layers or in-session UI changes can override them.

Requirements are constraints that enforce allowed values for a limited set of properties like sandbox mode and approval policy. Users can't run Codex with conflicting values.


---


# Advanced Configuration

Use these options when you need more control over providers, policies, and integrations. For a quick start, see [Config basics](https://developers.openai.com/codex/config-basic).

## Profiles

Profiles let you save named sets of configuration values and switch between them from the CLI.

<DocsTip>
  Profiles are experimental and may change or be removed in future releases.
</DocsTip>

<DocsTip>
  Profiles are not currently supported in the Codex IDE extension.
</DocsTip>

Define profiles under `[profiles.<name>]` in `config.toml`, then run `codex --profile <name>`:

```toml
model = "gpt-5-codex"
approval_policy = "on-request"

[profiles.deep-review]
model = "gpt-5-pro"
model_reasoning_effort = "high"
approval_policy = "never"

[profiles.lightweight]
model = "gpt-4.1"
approval_policy = "untrusted"
```

To make a profile the default, add `profile = "deep-review"` at the top level of `config.toml`. Codex loads that profile unless you override it on the command line.

## One-off overrides from the CLI

In addition to editing `~/.codex/config.toml`, you can override configuration for a single run from the CLI:

- Prefer dedicated flags when they exist (for example, `--model`).
- Use `-c` / `--config` when you need to override an arbitrary key.

Examples:

```shell
# Dedicated flag
codex --model gpt-5.2

# Generic key/value override (value is TOML, not JSON)
codex --config model='"gpt-5.2"'
codex --config sandbox_workspace_write.network_access=true
codex --config 'shell_environment_policy.include_only=["PATH","HOME"]'
```

Notes:

- Keys can use dot notation to set nested values (for example, `mcp_servers.context7.enabled=false`).
- `--config` values are parsed as TOML. When in doubt, quote the value so your shell doesn't split it on spaces.
- If the value can't be parsed as TOML, Codex treats it as a string.

## Config and state locations

Codex stores its local state under `CODEX_HOME` (defaults to `~/.codex`).

Common files you may see there:

- `config.toml` (your local configuration)
- `auth.json` (if you use file-based credential storage) or your OS keychain/keyring
- `history.jsonl` (if history persistence is enabled)
- Other per-user state such as logs and caches

For authentication details (including credential storage modes), see [Authentication](https://developers.openai.com/codex/auth). For the full list of configuration keys, see [Configuration Reference](https://developers.openai.com/codex/config-reference).

For shared defaults, rules, and skills checked into repos or system paths, see [Team Config](https://developers.openai.com/codex/team-config).

## Project root detection

Codex discovers project configuration (for example, `.codex/` layers and `AGENTS.md`) by walking up from the working directory until it reaches a "project root".

By default, Codex treats a directory containing `.git` as the project root. To customize this behavior, set `project_root_markers` in `config.toml`:

```toml
# Treat a directory as the project root when it contains any of these markers.
project_root_markers = [".git", ".hg", ".sl"]
```

Set `project_root_markers = []` to skip searching parent directories and treat the current working directory as the project root.

## Custom model providers

A model provider defines how Codex connects to a model (base URL, wire API, and optional HTTP headers).

Define additional providers and point `model_provider` at them:

```toml
model = "gpt-5.1"
model_provider = "proxy"

[model_providers.proxy]
name = "OpenAI using LLM proxy"
base_url = "http://proxy.example.com"
env_key = "OPENAI_API_KEY"

[model_providers.ollama]
name = "Ollama"
base_url = "http://localhost:11434/v1"

[model_providers.mistral]
name = "Mistral"
base_url = "https://api.mistral.ai/v1"
env_key = "MISTRAL_API_KEY"
```

Add request headers when needed:

```toml
[model_providers.example]
http_headers = { "X-Example-Header" = "example-value" }
env_http_headers = { "X-Example-Features" = "EXAMPLE_FEATURES" }
```

## OpenAI base URL override

If you just need to point the built-in OpenAI provider at an LLM proxy or router, set `OPENAI_BASE_URL` instead of defining a new provider. This overrides the default OpenAI endpoint without a `config.toml` change.

```shell
export OPENAI_BASE_URL="https://api.openai.com/v1"
codex
```

## OSS mode (local providers)

Codex can run against a local "open source" provider (for example, Ollama or LM Studio) when you pass `--oss`. If you pass `--oss` without specifying a provider, Codex uses `oss_provider` as the default.

```toml
# Default local provider used with `--oss`
oss_provider = "ollama" # or "lmstudio"
```

## Azure provider and per-provider tuning

```toml
[model_providers.azure]
name = "Azure"
base_url = "https://YOUR_PROJECT_NAME.openai.azure.com/openai"
env_key = "AZURE_OPENAI_API_KEY"
query_params = { api-version = "2025-04-01-preview" }
wire_api = "responses"

[model_providers.openai]
request_max_retries = 4
stream_max_retries = 10
stream_idle_timeout_ms = 300000
```

## Model reasoning, verbosity, and limits

```toml
model_reasoning_summary = "none"          # Disable summaries
model_verbosity = "low"                   # Shorten responses
model_supports_reasoning_summaries = true # Force reasoning
model_context_window = 128000             # Context window size
```

`model_verbosity` applies only to providers using the Responses API. Chat Completions providers will ignore the setting.

## Approval policies and sandbox modes

Pick approval strictness (affects when Codex pauses) and sandbox level (affects file/network access). See [Sandbox & approvals](https://developers.openai.com/codex/security) for deeper examples.

```toml
approval_policy = "untrusted"   # Other options: on-request, on-failure, never
sandbox_mode = "workspace-write"

[sandbox_workspace_write]
exclude_tmpdir_env_var = false  # Allow $TMPDIR
exclude_slash_tmp = false       # Allow /tmp
writable_roots = ["/Users/YOU/.pyenv/shims"]
network_access = false          # Opt in to outbound network
```

<DocsTip>
  In workspace-write mode, some environments keep `.git/` and `.codex/`
  read-only even when the rest of the workspace is writable. This is why
  commands like `git commit` may still require approval to run outside the
  sandbox. If you want Codex to skip specific commands (for example, block `git
  commit` outside the sandbox), use
  <a href="/codex/rules">rules</a>.
</DocsTip>

Disable sandboxing entirely (use only if your environment already isolates processes):

```toml
sandbox_mode = "danger-full-access"
```

## Shell environment policy

`shell_environment_policy` controls which environment variables Codex passes to any subprocess it launches (for example, when running a tool-command the model proposes). Start from a clean slate (`inherit = "none"`) or a trimmed set (`inherit = "core"`), then layer on excludes, includes, and overrides to avoid leaking secrets while still providing the paths, keys, or flags your tasks need.

```toml
[shell_environment_policy]
inherit = "none"
set = { PATH = "/usr/bin", MY_FLAG = "1" }
ignore_default_excludes = false
exclude = ["AWS_*", "AZURE_*"]
include_only = ["PATH", "HOME"]
```

Patterns are case-insensitive globs (`*`, `?`, `[A-Z]`); `ignore_default_excludes = false` keeps the automatic KEY/SECRET/TOKEN filter before your includes/excludes run.

## MCP servers

See the dedicated [MCP documentation](https://developers.openai.com/codex/mcp) for configuration details.

## Observability and telemetry

Enable OpenTelemetry (OTel) log export to track Codex runs (API requests, SSE/events, prompts, tool approvals/results). Disabled by default; opt in via `[otel]`:

```toml
[otel]
environment = "staging"   # defaults to "dev"
exporter = "none"         # set to otlp-http or otlp-grpc to send events
log_user_prompt = false   # redact user prompts unless explicitly enabled
```

Choose an exporter:

```toml
[otel]
exporter = { otlp-http = {
  endpoint = "https://otel.example.com/v1/logs",
  protocol = "binary",
  headers = { "x-otlp-api-key" = "${OTLP_TOKEN}" }
}}
```

```toml
[otel]
exporter = { otlp-grpc = {
  endpoint = "https://otel.example.com:4317",
  headers = { "x-otlp-meta" = "abc123" }
}}
```

If `exporter = "none"` Codex records events but sends nothing. Exporters batch asynchronously and flush on shutdown. Event metadata includes service name, CLI version, env tag, conversation id, model, sandbox/approval settings, and per-event fields (see [Config Reference](https://developers.openai.com/codex/config-reference)).

### What gets emitted

Codex emits structured log events for runs and tool usage. Representative event types include:

- `codex.conversation_starts` (model, reasoning settings, sandbox/approval policy)
- `codex.api_request` and `codex.sse_event` (durations, status, token counts)
- `codex.user_prompt` (length; content redacted unless explicitly enabled)
- `codex.tool_decision` (approved/denied and whether the decision came from config vs user)
- `codex.tool_result` (duration, success, output snippet)

For more security and privacy guidance around telemetry, see [Security](https://developers.openai.com/codex/security#monitoring-and-telemetry).

### Metrics

By default, Codex periodically sends a small amount of anonymous usage and health data back to OpenAI. This helps detect when Codex isn't working correctly and shows what features and configuration options are being used, so the Codex team can focus on what matters most. These metrics do not contain any personally identifiable information (PII). Metrics collection is independent of OTEL log/trace export.

If you want to disable metrics collection entirely across Codex surfaces on a machine, set the analytics flag in your config:

```toml
[analytics]
enabled = false
```

Each metric includes its own fields plus the default context fields below.

#### Default context fields (applies to every event/metric)

- `auth_mode`: `swic` | `api` | `unknown`.
- `model`: name of the model used.
- `app.version`: Codex version.

#### Metrics catalog

Each metric includes the required fields plus the default context fields above. Every metric is prefixed by `codex.`.
If a metric includes the `tool` field, it is populated by the internal tool used (`apply_patch`, `shell`, ...) and does not contain the actual shell command or patch `codex` is trying to apply.

| Metric                                   | Type      | Fields             | Description                                                                                                      |
| ---------------------------------------- | --------- | ------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `feature.state`                          | counter   | `feature`, `value` | Feature values that differ from defaults (emit one row per non-default).                                         |
| `thread.started`                         | counter   | `is_git`           | New thread created.                                                                                              |
| `task.compact`                           | counter   | `type`             | Number of compactions per type (`remote` or `local`), including manual and auto.                                 |
| `task.user_shell`                        | counter   |                    | Number of user shell actions (`!` in the TUI for example).                                                       |
| `task.review`                            | counter   |                    | Number of reviews triggered.                                                                                     |
| `task.undo`                              | counter   |                    | Number of undo actions triggered.                                                                                |
| `approval.requested`                     | counter   | `tool`, `approved` | Tool approval request result (`approved`, `approved_with_amendment`, `approved_for_session`, `denied`, `abort`). |
| `conversation.turn.count`                | counter   |                    | User/assistant turns per thread, recorded at the end of the thread.                                              |
| `turn.e2e_duration_ms`                   | histogram |                    | End-to-end time for a full turn.                                                                                 |
| `mcp.call`                               | counter   | `status`           | MCP tool invocation result (`ok` or error string).                                                               |
| `model_warning`                          | counter   |                    | Warning sent to the model.                                                                                       |
| `tool.call`                              | counter   | `tool`, `success`  | Tool invocation result (`success`: `true` or `false`).                                                           |
| `tool.call.duration_ms`                  | histogram | `tool`, `success`  | Tool execution time.                                                                                             |
| `remote_models.fetch_update.duration_ms` | histogram |                    | Time to fetch remote model definitions.                                                                          |
| `remote_models.load_cache.duration_ms`   | histogram |                    | Time to load the remote model cache.                                                                             |
| `shell_snapshot`                         | counter   | `success`          | Whether taking a shell snapshot succeeded.                                                                       |
| `shell_snapshot.duration_ms`             | histogram | `success`          | Time to take a shell snapshot.                                                                                   |

### Feedback controls

By default, Codex lets users send feedback from `/feedback`. To disable feedback collection across Codex surfaces on a machine, update your config:

```toml
[feedback]
enabled = false
```

When disabled, `/feedback` shows a disabled message and Codex rejects feedback submissions.

### Hide or surface reasoning events

If you want to reduce noisy "reasoning" output (for example in CI logs), you can suppress it:

```toml
hide_agent_reasoning = true
```

If you want to surface raw reasoning content when a model emits it:

```toml
show_raw_agent_reasoning = true
```

Enable raw reasoning only if it's acceptable for your workflow. Some models/providers (like `gpt-oss`) do not emit raw reasoning; in that case, this setting has no visible effect.

## Notifications

Use `notify` to trigger an external program whenever Codex emits supported events (currently only `agent-turn-complete`). This is handy for desktop toasts, chat webhooks, CI updates, or any side-channel alerting that the built-in TUI notifications don't cover.

```toml
notify = ["python3", "/path/to/notify.py"]
```

Example `notify.py` (truncated) that reacts to `agent-turn-complete`:

```python
#!/usr/bin/env python3
import json, subprocess, sys

def main() -> int:
    notification = json.loads(sys.argv[1])
    if notification.get("type") != "agent-turn-complete":
        return 0
    title = f"Codex: {notification.get('last-assistant-message', 'Turn Complete!')}"
    message = " ".join(notification.get("input-messages", []))
    subprocess.check_output([
        "terminal-notifier",
        "-title", title,
        "-message", message,
        "-group", "codex-" + notification.get("thread-id", ""),
        "-activate", "com.googlecode.iterm2",
    ])
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

The script receives a single JSON argument. Common fields include:

- `type` (currently `agent-turn-complete`)
- `thread-id` (session identifier)
- `turn-id` (turn identifier)
- `cwd` (working directory)
- `input-messages` (user messages that led to the turn)
- `last-assistant-message` (last assistant message text)

Place the script somewhere on disk and point `notify` to it.

#### `notify` vs `tui.notifications`

- `notify` runs an external program (good for webhooks, desktop notifiers, CI hooks).
- `tui.notifications` is built in to the TUI and can optionally filter by event type (for example, `agent-turn-complete` and `approval-requested`).

See [Configuration Reference](https://developers.openai.com/codex/config-reference) for the exact keys.

## History persistence

By default, Codex saves local session transcripts under `CODEX_HOME` (for example, `~/.codex/history.jsonl`). To disable local history persistence:

```toml
[history]
persistence = "none"
```

To cap the history file size, set `history.max_bytes`. When the file exceeds the cap, Codex drops the oldest entries and compacts the file while keeping the newest records.

```toml
[history]
max_bytes = 104857600 # 100 MiB
```

## Clickable citations

If you use a terminal/editor integration that supports it, Codex can render file citations as clickable links. Configure `file_opener` to pick the URI scheme Codex uses:

```toml
file_opener = "vscode" # or cursor, windsurf, vscode-insiders, none
```

Example: a citation like `/home/user/project/main.py:42` can be rewritten into a clickable `vscode://file/...:42` link.

## Project instructions discovery

Codex reads `AGENTS.md` (and related files) and includes a limited amount of project guidance in the first turn of a session. Two knobs control how this works:

- `project_doc_max_bytes`: how much to read from each `AGENTS.md` file
- `project_doc_fallback_filenames`: additional filenames to try when `AGENTS.md` is missing at a directory level

For a detailed walkthrough, see [Custom instructions with AGENTS.md](https://developers.openai.com/codex/guides/agents-md).

## TUI options

Running `codex` with no subcommand launches the interactive terminal UI (TUI). Codex exposes some TUI-specific configuration under `[tui]`, including:

- `tui.notifications`: enable/disable notifications (or restrict to specific types)
- `tui.animations`: enable/disable ASCII animations and shimmer effects
- `tui.alternate_screen`: control alternate screen usage (set to `never` to keep terminal scrollback)
- `tui.scroll_*`: tune wheel/trackpad scroll behavior in the TUI2 viewport

See [Configuration Reference](https://developers.openai.com/codex/config-reference) for the full key list.


---


# Configuration Reference

Use this page as a searchable reference for Codex configuration files. For conceptual guidance and examples, start with [Config basics](https://developers.openai.com/codex/config-basic) and [Advanced Config](https://developers.openai.com/codex/config-advanced).

## `config.toml`

User-level configuration lives in `~/.codex/config.toml`. Teams can layer shared defaults via [Team Config](https://developers.openai.com/codex/team-config).

<ConfigTable
  options={[
    {
      key: "model",
      type: "string",
      description: "Model to use (e.g., `gpt-5-codex`).",
    },
    {
      key: "review_model",
      type: "string",
      description:
        "Optional model override used by `/review` (defaults to the current session model).",
    },
    {
      key: "model_provider",
      type: "string",
      description: "Provider id from `model_providers` (default: `openai`).",
    },
    {
      key: "model_context_window",
      type: "number",
      description: "Context window tokens available to the active model.",
    },
    {
      key: "model_auto_compact_token_limit",
      type: "number",
      description:
        "Token threshold that triggers automatic history compaction (unset uses model defaults).",
    },
    {
      key: "oss_provider",
      type: "lmstudio | ollama",
      description:
        "Default local provider used when running with `--oss` (defaults to prompting if unset).",
    },
    {
      key: "approval_policy",
      type: "untrusted | on-failure | on-request | never",
      description:
        "Controls when Codex pauses for approval before executing commands.",
    },
    {
      key: "sandbox_mode",
      type: "read-only | workspace-write | danger-full-access",
      description:
        "Sandbox policy for filesystem and network access during command execution.",
    },
    {
      key: "sandbox_workspace_write.writable_roots",
      type: "array<string>",
      description:
        'Additional writable roots when `sandbox_mode = "workspace-write"`.',
    },
    {
      key: "sandbox_workspace_write.network_access",
      type: "boolean",
      description:
        "Allow outbound network access inside the workspace-write sandbox.",
    },
    {
      key: "sandbox_workspace_write.exclude_tmpdir_env_var",
      type: "boolean",
      description:
        "Exclude `$TMPDIR` from writable roots in workspace-write mode.",
    },
    {
      key: "sandbox_workspace_write.exclude_slash_tmp",
      type: "boolean",
      description:
        "Exclude `/tmp` from writable roots in workspace-write mode.",
    },
    {
      key: "notify",
      type: "array<string>",
      description:
        "Command invoked for notifications; receives a JSON payload from Codex.",
    },
    {
      key: "check_for_update_on_startup",
      type: "boolean",
      description:
        "Check for Codex updates on startup (set to false only when updates are centrally managed).",
    },
    {
      key: "feedback.enabled",
      type: "boolean",
      description:
        "Enable feedback submission via `/feedback` across Codex surfaces (default: true).",
    },
    {
      key: "instructions",
      type: "string",
      description:
        "Reserved for future use; prefer `model_instructions_file` or `AGENTS.md`.",
    },
    {
      key: "developer_instructions",
      type: "string",
      description:
        "Additional developer instructions injected into the session (optional).",
    },
    {
      key: "compact_prompt",
      type: "string",
      description: "Inline override for the history compaction prompt.",
    },
    {
      key: "model_instructions_file",
      type: "string (path)",
      description:
        "Replacement for built-in instructions instead of `AGENTS.md`.",
    },
    {
      key: "experimental_compact_prompt_file",
      type: "string (path)",
      description:
        "Load the compaction prompt override from a file (experimental).",
    },
    {
      key: "skills.config",
      type: "array<object>",
      description: "Per-skill enablement overrides stored in config.toml.",
    },
    {
      key: "skills.config.<index>.path",
      type: "string (path)",
      description: "Path to a skill folder containing `SKILL.md`.",
    },
    {
      key: "skills.config.<index>.enabled",
      type: "boolean",
      description: "Enable or disable the referenced skill.",
    },
    {
      key: "mcp_servers.<id>.command",
      type: "string",
      description: "Launcher command for an MCP stdio server.",
    },
    {
      key: "mcp_servers.<id>.args",
      type: "array<string>",
      description: "Arguments passed to the MCP stdio server command.",
    },
    {
      key: "mcp_servers.<id>.env",
      type: "map<string,string>",
      description: "Environment variables forwarded to the MCP stdio server.",
    },
    {
      key: "mcp_servers.<id>.env_vars",
      type: "array<string>",
      description:
        "Additional environment variables to whitelist for an MCP stdio server.",
    },
    {
      key: "mcp_servers.<id>.cwd",
      type: "string",
      description: "Working directory for the MCP stdio server process.",
    },
    {
      key: "mcp_servers.<id>.url",
      type: "string",
      description: "Endpoint for an MCP streamable HTTP server.",
    },
    {
      key: "mcp_servers.<id>.bearer_token_env_var",
      type: "string",
      description:
        "Environment variable sourcing the bearer token for an MCP HTTP server.",
    },
    {
      key: "mcp_servers.<id>.http_headers",
      type: "map<string,string>",
      description: "Static HTTP headers included with each MCP HTTP request.",
    },
    {
      key: "mcp_servers.<id>.env_http_headers",
      type: "map<string,string>",
      description:
        "HTTP headers populated from environment variables for an MCP HTTP server.",
    },
    {
      key: "mcp_servers.<id>.enabled",
      type: "boolean",
      description: "Disable an MCP server without removing its configuration.",
    },
    {
      key: "mcp_servers.<id>.startup_timeout_sec",
      type: "number",
      description:
        "Override the default 10s startup timeout for an MCP server.",
    },
    {
      key: "mcp_servers.<id>.startup_timeout_ms",
      type: "number",
      description: "Alias for `startup_timeout_sec` in milliseconds.",
    },
    {
      key: "mcp_servers.<id>.tool_timeout_sec",
      type: "number",
      description:
        "Override the default 60s per-tool timeout for an MCP server.",
    },
    {
      key: "mcp_servers.<id>.enabled_tools",
      type: "array<string>",
      description: "Allow list of tool names exposed by the MCP server.",
    },
    {
      key: "mcp_servers.<id>.disabled_tools",
      type: "array<string>",
      description:
        "Deny list applied after `enabled_tools` for the MCP server.",
    },
    {
      key: "features.unified_exec",
      type: "boolean",
      description: "Use the unified PTY-backed exec tool (beta).",
    },
    {
      key: "features.shell_snapshot",
      type: "boolean",
      description:
        "Snapshot shell environment to speed up repeated commands (beta).",
    },
    {
      key: "features.apply_patch_freeform",
      type: "boolean",
      description: "Expose the freeform `apply_patch` tool (experimental).",
    },
    {
      key: "features.web_search_request",
      type: "boolean",
      description: "Allow the model to issue web searches (stable).",
    },
    {
      key: "features.shell_tool",
      type: "boolean",
      description:
        "Enable the default `shell` tool for running commands (stable; on by default).",
    },
    {
      key: "features.exec_policy",
      type: "boolean",
      description:
        "Enforce rules checks for `shell`/`unified_exec` (experimental; on by default).",
    },
    {
      key: "features.experimental_windows_sandbox",
      type: "boolean",
      description: "Run the Windows restricted-token sandbox (experimental).",
    },
    {
      key: "features.elevated_windows_sandbox",
      type: "boolean",
      description:
        "Enable the elevated Windows sandbox pipeline (experimental).",
    },
    {
      key: "features.remote_compaction",
      type: "boolean",
      description:
        "Enable remote compaction (ChatGPT auth only; experimental; on by default).",
    },
    {
      key: "features.remote_models",
      type: "boolean",
      description:
        "Refresh remote model list before showing readiness (experimental).",
    },
    {
      key: "features.powershell_utf8",
      type: "boolean",
      description: "Force PowerShell UTF-8 output (defaults to true).",
    },
    {
      key: "features.child_agents_md",
      type: "boolean",
      description:
        "Append AGENTS.md scope/precedence guidance even when no AGENTS.md is present (experimental).",
    },
    {
      key: "model_providers.<id>.name",
      type: "string",
      description: "Display name for a custom model provider.",
    },
    {
      key: "model_providers.<id>.base_url",
      type: "string",
      description: "API base URL for the model provider.",
    },
    {
      key: "model_providers.<id>.env_key",
      type: "string",
      description: "Environment variable supplying the provider API key.",
    },
    {
      key: "model_providers.<id>.env_key_instructions",
      type: "string",
      description: "Optional setup guidance for the provider API key.",
    },
    {
      key: "model_providers.<id>.experimental_bearer_token",
      type: "string",
      description:
        "Direct bearer token for the provider (discouraged; use `env_key`).",
    },
    {
      key: "model_providers.<id>.requires_openai_auth",
      type: "boolean",
      description:
        "The provider uses OpenAI authentication (defaults to false).",
    },
    {
      key: "model_providers.<id>.wire_api",
      type: "chat | responses",
      description:
        "Protocol used by the provider (defaults to `chat` if omitted).",
    },
    {
      key: "model_providers.<id>.query_params",
      type: "map<string,string>",
      description: "Extra query parameters appended to provider requests.",
    },
    {
      key: "model_providers.<id>.http_headers",
      type: "map<string,string>",
      description: "Static HTTP headers added to provider requests.",
    },
    {
      key: "model_providers.<id>.env_http_headers",
      type: "map<string,string>",
      description:
        "HTTP headers populated from environment variables when present.",
    },
    {
      key: "model_providers.<id>.request_max_retries",
      type: "number",
      description:
        "Retry count for HTTP requests to the provider (default: 4).",
    },
    {
      key: "model_providers.<id>.stream_max_retries",
      type: "number",
      description: "Retry count for SSE streaming interruptions (default: 5).",
    },
    {
      key: "model_providers.<id>.stream_idle_timeout_ms",
      type: "number",
      description:
        "Idle timeout for SSE streams in milliseconds (default: 300000).",
    },
    {
      key: "model_reasoning_effort",
      type: "minimal | low | medium | high | xhigh",
      description:
        "Adjust reasoning effort for supported models (Responses API only; `xhigh` is model-dependent).",
    },
    {
      key: "model_reasoning_summary",
      type: "auto | concise | detailed | none",
      description:
        "Select reasoning summary detail or disable summaries entirely.",
    },
    {
      key: "model_verbosity",
      type: "low | medium | high",
      description:
        "Control GPT-5 Responses API verbosity (defaults to `medium`).",
    },
    {
      key: "model_supports_reasoning_summaries",
      type: "boolean",
      description:
        "Force Codex to send reasoning metadata even for unknown models.",
    },
    {
      key: "shell_environment_policy.inherit",
      type: "all | core | none",
      description:
        "Baseline environment inheritance when spawning subprocesses.",
    },
    {
      key: "shell_environment_policy.ignore_default_excludes",
      type: "boolean",
      description:
        "Keep variables containing KEY/SECRET/TOKEN before other filters run.",
    },
    {
      key: "shell_environment_policy.exclude",
      type: "array<string>",
      description:
        "Glob patterns for removing environment variables after the defaults.",
    },
    {
      key: "shell_environment_policy.include_only",
      type: "array<string>",
      description:
        "Whitelist of patterns; when set only matching variables are kept.",
    },
    {
      key: "shell_environment_policy.set",
      type: "map<string,string>",
      description:
        "Explicit environment overrides injected into every subprocess.",
    },
    {
      key: "shell_environment_policy.experimental_use_profile",
      type: "boolean",
      description: "Use the user shell profile when spawning subprocesses.",
    },
    {
      key: "project_root_markers",
      type: "array<string>",
      description:
        "List of project root marker filenames; used when searching parent directories for the project root.",
    },
    {
      key: "project_doc_max_bytes",
      type: "number",
      description:
        "Maximum bytes read from `AGENTS.md` when building project instructions.",
    },
    {
      key: "project_doc_fallback_filenames",
      type: "array<string>",
      description: "Additional filenames to try when `AGENTS.md` is missing.",
    },
    {
      key: "profile",
      type: "string",
      description:
        "Default profile applied at startup (equivalent to `--profile`).",
    },
    {
      key: "profiles.<name>.*",
      type: "various",
      description:
        "Profile-scoped overrides for any of the supported configuration keys.",
    },
    {
      key: "profiles.<name>.include_apply_patch_tool",
      type: "boolean",
      description:
        "Legacy name for enabling freeform apply_patch; prefer `[features].apply_patch_freeform`.",
    },
    {
      key: "profiles.<name>.experimental_use_unified_exec_tool",
      type: "boolean",
      description:
        "Legacy name for enabling unified exec; prefer `[features].unified_exec`.",
    },
    {
      key: "profiles.<name>.experimental_use_freeform_apply_patch",
      type: "boolean",
      description:
        "Legacy name for enabling freeform apply_patch; prefer `[features].apply_patch_freeform`.",
    },
    {
      key: "profiles.<name>.oss_provider",
      type: "lmstudio | ollama",
      description: "Profile-scoped OSS provider for `--oss` sessions.",
    },
    {
      key: "history.persistence",
      type: "save-all | none",
      description:
        "Control whether Codex saves session transcripts to history.jsonl.",
    },
    {
      key: "tool_output_token_limit",
      type: "number",
      description:
        "Token budget for storing individual tool/function outputs in history.",
    },
    {
      key: "history.max_bytes",
      type: "number",
      description:
        "If set, caps the history file size in bytes by dropping oldest entries.",
    },
    {
      key: "file_opener",
      type: "vscode | vscode-insiders | windsurf | cursor | none",
      description:
        "URI scheme used to open citations from Codex output (default: `vscode`).",
    },
    {
      key: "otel.environment",
      type: "string",
      description:
        "Environment tag applied to emitted OpenTelemetry events (default: `dev`).",
    },
    {
      key: "otel.exporter",
      type: "none | otlp-http | otlp-grpc",
      description:
        "Select the OpenTelemetry exporter and provide any endpoint metadata.",
    },
    {
      key: "otel.trace_exporter",
      type: "none | otlp-http | otlp-grpc",
      description:
        "Select the OpenTelemetry trace exporter and provide any endpoint metadata.",
    },
    {
      key: "otel.log_user_prompt",
      type: "boolean",
      description:
        "Opt in to exporting raw user prompts with OpenTelemetry logs.",
    },
    {
      key: "otel.exporter.<id>.endpoint",
      type: "string",
      description: "Exporter endpoint for OTEL logs.",
    },
    {
      key: "otel.exporter.<id>.protocol",
      type: "binary | json",
      description: "Protocol used by the OTLP/HTTP exporter.",
    },
    {
      key: "otel.exporter.<id>.headers",
      type: "map<string,string>",
      description: "Static headers included with OTEL exporter requests.",
    },
    {
      key: "otel.trace_exporter.<id>.endpoint",
      type: "string",
      description: "Trace exporter endpoint for OTEL logs.",
    },
    {
      key: "otel.trace_exporter.<id>.protocol",
      type: "binary | json",
      description: "Protocol used by the OTLP/HTTP trace exporter.",
    },
    {
      key: "otel.trace_exporter.<id>.headers",
      type: "map<string,string>",
      description: "Static headers included with OTEL trace exporter requests.",
    },
    {
      key: "otel.exporter.<id>.tls.ca-certificate",
      type: "string",
      description: "CA certificate path for OTEL exporter TLS.",
    },
    {
      key: "otel.exporter.<id>.tls.client-certificate",
      type: "string",
      description: "Client certificate path for OTEL exporter TLS.",
    },
    {
      key: "otel.exporter.<id>.tls.client-private-key",
      type: "string",
      description: "Client private key path for OTEL exporter TLS.",
    },
    {
      key: "otel.trace_exporter.<id>.tls.ca-certificate",
      type: "string",
      description: "CA certificate path for OTEL trace exporter TLS.",
    },
    {
      key: "otel.trace_exporter.<id>.tls.client-certificate",
      type: "string",
      description: "Client certificate path for OTEL trace exporter TLS.",
    },
    {
      key: "otel.trace_exporter.<id>.tls.client-private-key",
      type: "string",
      description: "Client private key path for OTEL trace exporter TLS.",
    },
    {
      key: "tui",
      type: "table",
      description:
        "TUI-specific options such as enabling inline desktop notifications.",
    },
    {
      key: "tui.notifications",
      type: "boolean | array<string>",
      description:
        "Enable TUI notifications; optionally restrict to specific event types.",
    },
    {
      key: "tui.animations",
      type: "boolean",
      description:
        "Enable terminal animations (welcome screen, shimmer, spinner) (default: true).",
    },
    {
      key: "tui.alternate_screen",
      type: "auto | always | never",
      description:
        "Control alternate screen usage for the TUI (default: auto; auto skips it in Zellij to preserve scrollback).",
    },
    {
      key: "tui.show_tooltips",
      type: "boolean",
      description:
        "Show onboarding tooltips in the TUI welcome screen (default: true).",
    },
    {
      key: "tui.scroll_events_per_tick",
      type: "number",
      description: "Wheel event density used to normalize TUI2 scrolling.",
    },
    {
      key: "tui.scroll_wheel_lines",
      type: "number",
      description: "Lines per wheel notch for TUI2 scrolling.",
    },
    {
      key: "tui.scroll_trackpad_lines",
      type: "number",
      description: "Baseline trackpad scroll sensitivity for TUI2.",
    },
    {
      key: "tui.scroll_trackpad_accel_events",
      type: "number",
      description: "Trackpad events required to gain +1x acceleration.",
    },
    {
      key: "tui.scroll_trackpad_accel_max",
      type: "number",
      description: "Maximum acceleration multiplier for trackpad scrolling.",
    },
    {
      key: "tui.scroll_mode",
      type: "auto | wheel | trackpad",
      description: "Scroll interpretation mode for TUI2.",
    },
    {
      key: "tui.scroll_wheel_tick_detect_max_ms",
      type: "number",
      description: "Auto-mode wheel tick detection threshold (ms).",
    },
    {
      key: "tui.scroll_wheel_like_max_duration_ms",
      type: "number",
      description: "Auto-mode wheel fallback duration threshold (ms).",
    },
    {
      key: "tui.scroll_invert",
      type: "boolean",
      description: "Invert mouse scroll direction in TUI2.",
    },
    {
      key: "hide_agent_reasoning",
      type: "boolean",
      description:
        "Suppress reasoning events in both the TUI and `codex exec` output.",
    },
    {
      key: "show_raw_agent_reasoning",
      type: "boolean",
      description:
        "Surface raw reasoning content when the active model emits it.",
    },
    {
      key: "disable_paste_burst",
      type: "boolean",
      description: "Disable burst-paste detection in the TUI.",
    },
    {
      key: "windows_wsl_setup_acknowledged",
      type: "boolean",
      description: "Track Windows onboarding acknowledgement (Windows only).",
    },
    {
      key: "chatgpt_base_url",
      type: "string",
      description: "Override the base URL used during the ChatGPT login flow.",
    },
    {
      key: "cli_auth_credentials_store",
      type: "file | keyring | auto",
      description:
        "Control where the CLI stores cached credentials (file-based auth.json vs OS keychain).",
    },
    {
      key: "mcp_oauth_credentials_store",
      type: "auto | file | keyring",
      description: "Preferred store for MCP OAuth credentials.",
    },
    {
      key: "mcp_oauth_callback_port",
      type: "integer",
      description:
        "Optional fixed port for the local HTTP callback server used during MCP OAuth login. When unset, Codex binds to an ephemeral port chosen by the OS.",
    },
    {
      key: "experimental_use_unified_exec_tool",
      type: "boolean",
      description:
        "Legacy name for enabling unified exec; prefer `[features].unified_exec` or `codex --enable unified_exec`.",
    },
    {
      key: "experimental_use_freeform_apply_patch",
      type: "boolean",
      description:
        "Legacy name for enabling freeform apply_patch; prefer `[features].apply_patch_freeform` or `codex --enable apply_patch_freeform`.",
    },
    {
      key: "include_apply_patch_tool",
      type: "boolean",
      description:
        "Legacy name for enabling freeform apply_patch; prefer `[features].apply_patch_freeform`.",
    },
    {
      key: "projects.<path>.trust_level",
      type: "string",
      description:
        'Mark a project or worktree as trusted or untrusted (`"trusted"` | `"untrusted"`).',
    },
    {
      key: "notice.hide_full_access_warning",
      type: "boolean",
      description: "Track acknowledgement of the full access warning prompt.",
    },
    {
      key: "notice.hide_world_writable_warning",
      type: "boolean",
      description:
        "Track acknowledgement of the Windows world-writable directories warning.",
    },
    {
      key: "notice.hide_rate_limit_model_nudge",
      type: "boolean",
      description: "Track opt-out of the rate limit model switch reminder.",
    },
    {
      key: "notice.hide_gpt5_1_migration_prompt",
      type: "boolean",
      description: "Track acknowledgement of the GPT-5.1 migration prompt.",
    },
    {
      key: "notice.hide_gpt-5.1-codex-max_migration_prompt",
      type: "boolean",
      description:
        "Track acknowledgement of the gpt-5.1-codex-max migration prompt.",
    },
    {
      key: "notice.model_migrations",
      type: "map<string,string>",
      description: "Track acknowledged model migrations as old->new mappings.",
    },
    {
      key: "forced_login_method",
      type: "chatgpt | api",
      description: "Restrict Codex to a specific authentication method.",
    },
    {
      key: "forced_chatgpt_workspace_id",
      type: "string (uuid)",
      description: "Limit ChatGPT logins to a specific workspace identifier.",
    },
  ]}
  client:load
/>

Note: `experimental_instructions_file` has been renamed to `model_instructions_file`. The old key is deprecated; update existing configs to the new name.

## `requirements.toml`

`requirements.toml` is an admin-enforced configuration file that constrains security-sensitive settings users can't override. For details, locations, and examples, see [Admin-enforced requirements](https://developers.openai.com/codex/security#admin-enforced-requirements-requirementstoml).

<ConfigTable
  options={[
    {
      key: "allowed_approval_policies",
      type: "array<string>",
      description: "Allowed values for `approval_policy`.",
    },
    {
      key: "allowed_sandbox_modes",
      type: "array<string>",
      description: "Allowed values for `sandbox_mode`.",
    },
    {
      key: "mcp_servers",
      type: "table",
      description:
        "Allowlist of MCP servers that may be enabled. Both the server name (`<id>`) and its identity must match for the MCP server to be enabled. Any configured MCP server not in the allowlist (or with a mismatched identity) is disabled.",
    },
    {
      key: "mcp_servers.<id>.identity",
      type: "table",
      description:
        "Identity rule for a single MCP server. Set either `command` (stdio) or `url` (streamable HTTP).",
    },
    {
      key: "mcp_servers.<id>.identity.command",
      type: "string",
      description:
        "Allow an MCP stdio server when its `mcp_servers.<id>.command` matches this command.",
    },
    {
      key: "mcp_servers.<id>.identity.url",
      type: "string",
      description:
        "Allow an MCP streamable HTTP server when its `mcp_servers.<id>.url` matches this URL.",
    },
  ]}
  client:load
/>


---


# Sample Configuration

Use this example configuration as a starting point. It includes most keys Codex reads from `config.toml`, along with defaults and short notes.

For explanations and guidance, see:

- [Config basics](https://developers.openai.com/codex/config-basic)
- [Advanced Config](https://developers.openai.com/codex/config-advanced)
- [Config Reference](https://developers.openai.com/codex/config-reference)

Use the snippet below as a reference. Copy only the keys and sections you need into `~/.codex/config.toml`, then adjust values for your setup. To share defaults across a team, place the file in a [Team Config](https://developers.openai.com/codex/team-config) location instead.

```toml
# Codex example configuration (config.toml)
#
# This file lists all keys Codex reads from config.toml, their default values,
# and concise explanations. Values here mirror the effective defaults compiled
# into the CLI. Adjust as needed.
#
# Notes
# - Root keys must appear before tables in TOML.
# - Optional keys that default to "unset" are shown commented out with notes.
# - MCP servers, profiles, and model providers are examples; remove or edit.

################################################################################
# Core Model Selection
################################################################################

# Primary model used by Codex. Default: "gpt-5.2-codex" on all platforms.
model = "gpt-5.2-codex"

# Optional model override for /review. Default: unset (uses current session model).
# review_model = "gpt-5.2-codex"

# Provider id selected from [model_providers]. Default: "openai".
model_provider = "openai"

# Default OSS provider for --oss sessions. When unset, Codex prompts. Default: unset.
# oss_provider = "ollama"

# Optional manual model metadata. When unset, Codex auto-detects from model.
# Uncomment to force values.
# model_context_window = 128000       # tokens; default: auto for model
# model_auto_compact_token_limit = 0  # tokens; unset uses model defaults
# tool_output_token_limit = 10000     # tokens stored per tool output; default: 10000 for gpt-5.2-codex

################################################################################
# Reasoning & Verbosity (Responses API capable models)
################################################################################

# Reasoning effort: minimal | low | medium | high | xhigh (default: medium; xhigh on gpt-5.2-codex and gpt-5.2)
model_reasoning_effort = "medium"

# Reasoning summary: auto | concise | detailed | none (default: auto)
model_reasoning_summary = "auto"

# Text verbosity for GPT-5 family (Responses API): low | medium | high (default: medium)
model_verbosity = "medium"

# Force-enable reasoning summaries for current model (default: false)
model_supports_reasoning_summaries = false

################################################################################
# Instruction Overrides
################################################################################

# Additional user instructions are injected before AGENTS.md. Default: unset.
# developer_instructions = ""

# (Ignored) Optional legacy base instructions override (prefer AGENTS.md). Default: unset.
# instructions = ""

# Inline override for the history compaction prompt. Default: unset.
# compact_prompt = ""

# Override built-in base instructions with a file path. Default: unset.
# model_instructions_file = "/absolute/or/relative/path/to/instructions.txt"

# Migration note: experimental_instructions_file was renamed to model_instructions_file (deprecated).

# Load the compact prompt override from a file. Default: unset.
# experimental_compact_prompt_file = "/absolute/or/relative/path/to/compact_prompt.txt"


################################################################################
# Notifications
################################################################################

# External notifier program (argv array). When unset: disabled.
# Example: notify = ["notify-send", "Codex"]
notify = [ ]


################################################################################
# Approval & Sandbox
################################################################################

# When to ask for command approval:
# - untrusted: only known-safe read-only commands auto-run; others prompt
# - on-failure: auto-run in sandbox; prompt only on failure for escalation
# - on-request: model decides when to ask (default)
# - never: never prompt (risky)
approval_policy = "on-request"

# Filesystem/network sandbox policy for tool calls:
# - read-only (default)
# - workspace-write
# - danger-full-access (no sandbox; extremely risky)
sandbox_mode = "read-only"

################################################################################
# Authentication & Login
################################################################################

# Where to persist CLI login credentials: file (default) | keyring | auto
cli_auth_credentials_store = "file"

# Base URL for ChatGPT auth flow (not OpenAI API). Default:
chatgpt_base_url = "https://chatgpt.com/backend-api/"

# Restrict ChatGPT login to a specific workspace id. Default: unset.
# forced_chatgpt_workspace_id = ""

# Force login mechanism when Codex would normally auto-select. Default: unset.
# Allowed values: chatgpt | api
# forced_login_method = "chatgpt"

# Preferred store for MCP OAuth credentials: auto (default) | file | keyring
mcp_oauth_credentials_store = "auto"

# Optional fixed port for MCP OAuth callback: 1-65535. Default: unset.
# mcp_oauth_callback_port = 4321

################################################################################
# Project Documentation Controls
################################################################################

# Max bytes from AGENTS.md to embed into first-turn instructions. Default: 32768
project_doc_max_bytes = 32768

# Ordered fallbacks when AGENTS.md is missing at a directory level. Default: []
project_doc_fallback_filenames = []

# Project root marker filenames used when searching parent directories. Default: [".git"]
# project_root_markers = [".git"]

################################################################################
# History & File Opener
################################################################################

# URI scheme for clickable citations: vscode (default) | vscode-insiders | windsurf | cursor | none
file_opener = "vscode"

################################################################################
# UI, Notifications, and Misc
################################################################################

# Suppress internal reasoning events from output. Default: false
hide_agent_reasoning = false

# Show raw reasoning content when available. Default: false
show_raw_agent_reasoning = false

# Disable burst-paste detection in the TUI. Default: false
disable_paste_burst = false

# Track Windows onboarding acknowledgement (Windows only). Default: false
windows_wsl_setup_acknowledged = false

# Check for updates on startup. Default: true
check_for_update_on_startup = true

################################################################################
# Profiles (named presets)
################################################################################

# Active profile name. When unset, no profile is applied.
# profile = "default"

################################################################################
# Skills (per-skill overrides)
################################################################################

# Disable or re-enable a specific skill without deleting it.
[[skills.config]]
# path = "/path/to/skill"
# enabled = false

################################################################################
# Experimental toggles (legacy; prefer [features])
################################################################################

experimental_use_unified_exec_tool = false

# Include apply_patch via freeform editing path (affects default tool set). Default: false
experimental_use_freeform_apply_patch = false

################################################################################
# Sandbox settings (tables)
################################################################################

# Extra settings used only when sandbox_mode = "workspace-write".
[sandbox_workspace_write]
# Additional writable roots beyond the workspace (cwd). Default: []
writable_roots = []
# Allow outbound network access inside the sandbox. Default: false
network_access = false
# Exclude $TMPDIR from writable roots. Default: false
exclude_tmpdir_env_var = false
# Exclude /tmp from writable roots. Default: false
exclude_slash_tmp = false

################################################################################
# Shell Environment Policy for spawned processes (table)
################################################################################

[shell_environment_policy]
# inherit: all (default) | core | none
inherit = "all"
# Skip default excludes for names containing KEY/SECRET/TOKEN (case-insensitive). Default: true
ignore_default_excludes = true
# Case-insensitive glob patterns to remove (e.g., "AWS_*", "AZURE_*"). Default: []
exclude = []
# Explicit key/value overrides (always win). Default: {}
set = {}
# Whitelist; if non-empty, keep only matching vars. Default: []
include_only = []
# Experimental: run via user shell profile. Default: false
experimental_use_profile = false

################################################################################
# History (table)
################################################################################

[history]
# save-all (default) | none
persistence = "save-all"
# Maximum bytes for history file; oldest entries are trimmed when exceeded. Example: 5242880
# max_bytes = 0

################################################################################
# UI, Notifications, and Misc (tables)
################################################################################

[tui]
# Desktop notifications from the TUI: boolean or filtered list. Default: true
# Examples: false | ["agent-turn-complete", "approval-requested"]
notifications = false

# Enables welcome/status/spinner animations. Default: true
animations = true

# Show onboarding tooltips in the welcome screen. Default: true
show_tooltips = true

# Control alternate screen usage (auto skips it in Zellij to preserve scrollback).
# alternate_screen = "auto"

# Control whether users can submit feedback from `/feedback`. Default: true
[feedback]
enabled = true

# In-product notices (mostly set automatically by Codex).
[notice]
# hide_full_access_warning = true
# hide_world_writable_warning = true
# hide_rate_limit_model_nudge = true
# hide_gpt5_1_migration_prompt = true
# "hide_gpt-5.1-codex-max_migration_prompt" = true
# model_migrations = { "gpt-4.1" = "gpt-5.1" }

################################################################################
# Centralized Feature Flags (preferred)
################################################################################

[features]
# Leave this table empty to accept defaults. Set explicit booleans to opt in/out.
shell_tool = true
web_search_request = false
unified_exec = false
shell_snapshot = false
apply_patch_freeform = false
exec_policy = true
experimental_windows_sandbox = false
elevated_windows_sandbox = false
remote_compaction = true
remote_models = false
powershell_utf8 = true
child_agents_md = false

################################################################################
# Define MCP servers under this table. Leave empty to disable.
################################################################################

[mcp_servers]

# --- Example: STDIO transport ---
# [mcp_servers.docs]
# enabled = true                       # optional; default true
# command = "docs-server"                 # required
# args = ["--port", "4000"]               # optional
# env = { "API_KEY" = "value" }           # optional key/value pairs copied as-is
# env_vars = ["ANOTHER_SECRET"]            # optional: forward these from the parent env
# cwd = "/path/to/server"                 # optional working directory override
# startup_timeout_sec = 10.0               # optional; default 10.0 seconds
# # startup_timeout_ms = 10000              # optional alias for startup timeout (milliseconds)
# tool_timeout_sec = 60.0                  # optional; default 60.0 seconds
# enabled_tools = ["search", "summarize"]  # optional allow-list
# disabled_tools = ["slow-tool"]           # optional deny-list (applied after allow-list)

# --- Example: Streamable HTTP transport ---
# [mcp_servers.github]
# enabled = true                          # optional; default true
# url = "https://github-mcp.example.com/mcp"  # required
# bearer_token_env_var = "GITHUB_TOKEN"        # optional; Authorization: Bearer <token>
# http_headers = { "X-Example" = "value" }    # optional static headers
# env_http_headers = { "X-Auth" = "AUTH_ENV" } # optional headers populated from env vars
# startup_timeout_sec = 10.0                   # optional
# tool_timeout_sec = 60.0                      # optional
# enabled_tools = ["list_issues"]             # optional allow-list

################################################################################
# Model Providers (extend/override built-ins)
################################################################################

# Built-ins include:
# - openai (Responses API; requires login or OPENAI_API_KEY via auth flow)
# - oss (Chat Completions API; defaults to http://localhost:11434/v1)

[model_providers]

# --- Example: override OpenAI with explicit base URL or headers ---
# [model_providers.openai]
# name = "OpenAI"
# base_url = "https://api.openai.com/v1"         # default if unset
# wire_api = "responses"                         # "responses" | "chat" (default varies)
# # requires_openai_auth = true                    # built-in OpenAI defaults to true
# # request_max_retries = 4                        # default 4; max 100
# # stream_max_retries = 5                         # default 5;  max 100
# # stream_idle_timeout_ms = 300000                # default 300_000 (5m)
# # experimental_bearer_token = "sk-example"      # optional dev-only direct bearer token
# # http_headers = { "X-Example" = "value" }
# # env_http_headers = { "OpenAI-Organization" = "OPENAI_ORGANIZATION", "OpenAI-Project" = "OPENAI_PROJECT" }

# --- Example: Azure (Chat/Responses depending on endpoint) ---
# [model_providers.azure]
# name = "Azure"
# base_url = "https://YOUR_PROJECT_NAME.openai.azure.com/openai"
# wire_api = "responses"                          # or "chat" per endpoint
# query_params = { api-version = "2025-04-01-preview" }
# env_key = "AZURE_OPENAI_API_KEY"
# # env_key_instructions = "Set AZURE_OPENAI_API_KEY in your environment"

# --- Example: Local OSS (e.g., Ollama-compatible) ---
# [model_providers.ollama]
# name = "Ollama"
# base_url = "http://localhost:11434/v1"
# wire_api = "chat"

################################################################################
# Profiles (named presets)
################################################################################

[profiles]

# [profiles.default]
# model = "gpt-5.2-codex"
# model_provider = "openai"
# approval_policy = "on-request"
# sandbox_mode = "read-only"
# oss_provider = "ollama"
# model_reasoning_effort = "medium"
# model_reasoning_summary = "auto"
# model_verbosity = "medium"
# chatgpt_base_url = "https://chatgpt.com/backend-api/"
# experimental_compact_prompt_file = "./compact_prompt.txt"
# include_apply_patch_tool = false
# experimental_use_unified_exec_tool = false
# experimental_use_freeform_apply_patch = false
# tools_web_search = false
# features = { unified_exec = false }

################################################################################
# Projects (trust levels)
################################################################################

# Mark specific worktrees as trusted or untrusted.
[projects]
# [projects."/absolute/path/to/project"]
# trust_level = "trusted"  # or "untrusted"

################################################################################
# OpenTelemetry (OTEL) - disabled by default
################################################################################

[otel]
# Include user prompt text in logs. Default: false
log_user_prompt = false
# Environment label applied to telemetry. Default: "dev"
environment = "dev"
# Exporter: none (default) | otlp-http | otlp-grpc
exporter = "none"
# Trace exporter: none (default) | otlp-http | otlp-grpc
trace_exporter = "none"

# Example OTLP/HTTP exporter configuration
# [otel.exporter."otlp-http"]
# endpoint = "https://otel.example.com/v1/logs"
# protocol = "binary"                         # "binary" | "json"

# [otel.exporter."otlp-http".headers]
# "x-otlp-api-key" = "${OTLP_TOKEN}"

# Example OTLP/gRPC exporter configuration
# [otel.exporter."otlp-grpc"]
# endpoint = "https://otel.example.com:4317",
# headers = { "x-otlp-meta" = "abc123" }

# Example OTLP exporter with mutual TLS
# [otel.exporter."otlp-http"]
# endpoint = "https://otel.example.com/v1/logs"
# protocol = "binary"

# [otel.exporter."otlp-http".headers]
# "x-otlp-api-key" = "${OTLP_TOKEN}"

# [otel.exporter."otlp-http".tls]
# ca-certificate = "certs/otel-ca.pem"
# client-certificate = "/etc/codex/certs/client.pem"
# client-private-key = "/etc/codex/certs/client-key.pem"
```


---


# Rules

Use rules to control which commands Codex can run outside the sandbox.

<DocsTip>Rules are experimental and may change.</DocsTip>

## Create a rules file

1. Create a `.rules` file under `rules/` in a [Team Config](https://developers.openai.com/codex/team-config) location (for example, `~/.codex/rules/default.rules`).
2. Add a rule. This example prompts before allowing `gh pr view` to run outside the sandbox.

   ```python
   # Prompt before running commands with the prefix `gh pr view` outside the sandbox.
   prefix_rule(
       # The prefix to match.
       pattern = ["gh", "pr", "view"],

       # The action to take when Codex requests to run a matching command.
       decision = "prompt",

       # Optional rationale for why this rule exists.
       justification = "Viewing PRs is allowed with approval",

       # `match` and `not_match` are optional "inline unit tests" where you can
       # provide examples of commands that should (or should not) match this rule.
       match = [
           "gh pr view 7888",
           "gh pr view --repo openai/codex",
           "gh pr view 7888 --json title,body,comments",
       ],
       not_match = [
           # Does not match because the `pattern` must be an exact prefix.
           "gh pr --repo openai/codex view 7888",
       ],
   )
   ```

3. Restart Codex.

Codex scans `rules/` under every [Team Config](https://developers.openai.com/codex/team-config) location at startup. When you add a command to the allow list in the TUI, Codex writes to the user layer at `~/.codex/rules/default.rules` so future runs can skip the prompt.

## Understand rule fields

`prefix_rule()` supports these fields:

- `pattern` **(required)**: A non-empty list that defines the command prefix to match. Each element is either:
  - A literal string (for example, `"pr"`).
  - A union of literals (for example, `["view", "list"]`) to match alternatives at that argument position.
- `decision` **(defaults to `"allow"`)**: The action to take when the rule matches. Codex applies the most restrictive decision when more than one rule matches (`forbidden` > `prompt` > `allow`).
  - `allow`: Run the command outside the sandbox without prompting.
  - `prompt`: Prompt before each matching invocation.
  - `forbidden`: Block the request without prompting.
- `justification` **(optional)**: A non-empty, human-readable reason for the rule. Codex may surface it in approval prompts or rejection messages. When you use `forbidden`, include a recommended alternative in the justification when appropriate (for example, `"Use \`rg\` instead of \`grep\`."`).
- `match` and `not_match` **(defaults to `[]`)**: Examples that Codex validates when it loads your rules. Use these to catch mistakes before a rule takes effect.

When Codex considers a command to run, it compares the command's argument list to `pattern`. Internally, Codex treats the command as a list of arguments (like what `execvp(3)` receives).

## Shell wrappers and compound commands

Some tools wrap several shell commands into a single invocation, for example:

```text
["bash", "-lc", "git add . && rm -rf /"]
```

Because this kind of command can hide multiple actions inside one string, Codex treats `bash -lc`, `bash -c`, and their `zsh` / `sh` equivalents specially.

### When Codex can safely split the script

If the shell script is a linear chain of commands made only of:

- plain words (no variable expansion, no `VAR=...`, `$FOO`, `*`, etc.)
- joined by safe operators (`&&`, `||`, `;`, or `|`)

then Codex parses it (using tree-sitter) and splits it into individual commands before applying your rules.

The script above is treated as two separate commands:

- `["git", "add", "."]`
- `["rm", "-rf", "/"]`

Codex then evaluates each command against your rules, and the most restrictive result wins.

Even if you allow `pattern=["git", "add"]`, Codex won't auto allow `git add . && rm -rf /`, because the `rm -rf /` portion is evaluated separately and prevents the whole invocation from being auto allowed.

This prevents dangerous commands from being smuggled in alongside safe ones.

### When Codex does not split the script

If the script uses more advanced shell features, such as:

- redirection (`>`, `>>`, `<`)
- substitutions (`$(...)`, `...`)
- environment variables (`FOO=bar`)
- wildcard patterns (`*`, `?`)
- control flow (`if`, `for`, `&&` with assignments, etc.)

then Codex doesn't try to interpret or split it.

In those cases, the entire invocation is treated as:

```text
["bash", "-lc", "<full script>"]
```

and your rules are applied to that **single** invocation.

With this handling, you get the security of per-command evaluation when it's safe to do so, and conservative behavior when it isn't.

## Test a rule file

Use `codex execpolicy check` to test how your rules apply to a command:

```shell
codex execpolicy check --pretty \
  --rules ~/.codex/rules/default.rules \
  -- gh pr view 7888 --json title,body,comments
```

The command emits JSON showing the strictest decision and any matching rules, including any `justification` values from matched rules. Use more than one `--rules` flag to combine files, and add `--pretty` to format the output.

## Understand the rules language

The `.rules` file format uses `Starlark` (see the [language spec](https://github.com/bazelbuild/starlark/blob/master/spec.md)). Its syntax is like Python, but it's designed to be safe to run: the rules engine can run it without side effects (for example, touching the filesystem).



---


# Custom instructions with AGENTS.md

Codex reads `AGENTS.md` files before doing any work. By layering global guidance with project-specific overrides, you can start each task with consistent expectations, no matter which repository you open.

## How Codex discovers guidance

Codex builds an instruction chain when it starts (once per run; in the TUI this usually means once per launched session). Discovery follows this precedence order:

1. **Global scope:** In your Codex home directory (defaults to `~/.codex`, unless you set `CODEX_HOME`), Codex reads `AGENTS.override.md` if it exists. Otherwise, Codex reads `AGENTS.md`. Codex uses only the first non-empty file at this level.
2. **Project scope:** Starting at the project root (typically the Git root), Codex walks down to your current working directory. If Codex cannot find a project root, it only checks the current directory. In each directory along the path, it checks for `AGENTS.override.md`, then `AGENTS.md`, then any fallback names in `project_doc_fallback_filenames`. Codex includes at most one file per directory.
3. **Merge order:** Codex concatenates files from the root down, joining them with blank lines. Files closer to your current directory override earlier guidance because they appear later in the combined prompt.

Codex skips empty files and stops adding files once the combined size reaches the limit defined by `project_doc_max_bytes` (32 KiB by default). For details on these knobs, see [Project instructions discovery](https://developers.openai.com/codex/config-advanced#project-instructions-discovery). Raise the limit or split instructions across nested directories when you hit the cap.

## Create global guidance

Create persistent defaults in your Codex home directory so every repository inherits your working agreements.

1. Ensure the directory exists:

   ```bash
   mkdir -p ~/.codex
   ```

2. Create `~/.codex/AGENTS.md` with reusable preferences:

   ```md
   # ~/.codex/AGENTS.md

   ## Working agreements

   - Always run `npm test` after modifying JavaScript files.
   - Prefer `pnpm` when installing dependencies.
   - Ask for confirmation before adding new production dependencies.
   ```

3. Run Codex anywhere to confirm it loads the file:

   ```bash
   codex --ask-for-approval never "Summarize the current instructions."
   ```

   Expected: Codex quotes the items from `~/.codex/AGENTS.md` before proposing work.

Use `~/.codex/AGENTS.override.md` when you need a temporary global override without deleting the base file. Remove the override to restore the shared guidance.

## Layer project instructions

Repository-level files keep Codex aware of project norms while still inheriting your global defaults.

1. In your repository root, add an `AGENTS.md` that covers basic setup:

   ```md
   # AGENTS.md

   ## Repository expectations

   - Run `npm run lint` before opening a pull request.
   - Document public utilities in `docs/` when you change behavior.
   ```

2. Add overrides in nested directories when specific teams need different rules. For example, inside `services/payments/` create `AGENTS.override.md`:

   ```md
   # services/payments/AGENTS.override.md

   ## Payments service rules

   - Use `make test-payments` instead of `npm test`.
   - Never rotate API keys without notifying the security channel.
   ```

3. Start Codex from the payments directory:

   ```bash
   codex --cd services/payments --ask-for-approval never "List the instruction sources you loaded."
   ```

   Expected: Codex reports the global file first, the repository root `AGENTS.md` second, and the payments override last.

Codex stops searching once it reaches your current directory, so place overrides as close to specialized work as possible.

Here is a sample repository after you add a global file and a payments-specific override:

<FileTree
  class="mt-4"
  tree={[
    {
      name: "AGENTS.md",
      comment: "Repository expectations",
      highlight: true,
    },
    {
      name: "services/",
      open: true,
      children: [
        {
          name: "payments/",
          open: true,
          children: [
            {
              name: "AGENTS.md",
              comment: "Ignored because an override exists",
            },
            {
              name: "AGENTS.override.md",
              comment: "Payments service rules",
              highlight: true,
            },
            { name: "README.md" },
          ],
        },
        {
          name: "search/",
          children: [{ name: "AGENTS.md" }, { name: "…", placeholder: true }],
        },
      ],
    },
  ]}
/>

## Customize fallback filenames

If your repository already uses a different filename (for example `TEAM_GUIDE.md`), add it to the fallback list so Codex treats it like an instructions file.

1. Edit your Codex configuration:

   ```toml
   # ~/.codex/config.toml
   project_doc_fallback_filenames = ["TEAM_GUIDE.md", ".agents.md"]
   project_doc_max_bytes = 65536
   ```

2. Restart Codex or run a new command so the updated configuration loads.

Now Codex checks each directory in this order: `AGENTS.override.md`, `AGENTS.md`, `TEAM_GUIDE.md`, `.agents.md`. Filenames not on this list are ignored for instruction discovery. The larger byte limit allows more combined guidance before truncation.

With the fallback list in place, Codex treats the alternate files as instructions:

<FileTree
  class="mt-4"
  tree={[
    {
      name: "TEAM_GUIDE.md",
      comment: "Detected via fallback list",
      highlight: true,
    },
    {
      name: ".agents.md",
      comment: "Fallback file in root",
    },
    {
      name: "support/",
      open: true,
      children: [
        {
          name: "AGENTS.override.md",
          comment: "Overrides fallback guidance",
          highlight: true,
        },
        {
          name: "playbooks/",
          children: [{ name: "…", placeholder: true }],
        },
      ],
    },
  ]}
/>

Set the `CODEX_HOME` environment variable when you want a different profile, such as a project-specific automation user:

```bash
CODEX_HOME=$(pwd)/.codex codex exec "List active instruction sources"
```

Expected: The output lists files relative to the custom `.codex` directory.

## Verify your setup

- Run `codex --ask-for-approval never "Summarize the current instructions."` from a repository root. Codex should echo guidance from global and project files in precedence order.
- Use `codex --cd subdir --ask-for-approval never "Show which instruction files are active."` to confirm nested overrides replace broader rules.
- Check `~/.codex/log/codex-tui.log` (or the most recent `session-*.jsonl` file if you enabled session logging) after a session if you need to audit which instruction files Codex loaded.
- If instructions look stale, restart Codex in the target directory. Codex rebuilds the instruction chain on every run (and at the start of each TUI session), so there is no cache to clear manually.

## Troubleshoot discovery issues

- **Nothing loads:** Verify you are in the intended repository and that `codex status` reports the workspace root you expect. Ensure instruction files contain content; Codex ignores empty files.
- **Wrong guidance appears:** Look for an `AGENTS.override.md` higher in the directory tree or under your Codex home. Rename or remove the override to fall back to the regular file.
- **Codex ignores fallback names:** Confirm you listed the names in `project_doc_fallback_filenames` without typos, then restart Codex so the updated configuration takes effect.
- **Instructions truncated:** Raise `project_doc_max_bytes` or split large files across nested directories to keep critical guidance intact.
- **Profile confusion:** Run `echo $CODEX_HOME` before launching Codex. A non-default value points Codex at a different home directory than the one you edited.

## Next steps

- Visit the official [AGENTS.md](https://agents.md) website for more information.
- Review [Prompting Codex](https://developers.openai.com/codex/prompting) for conversational patterns that pair well with persistent guidance.


---


# Model Context Protocol

Model Context Protocol (MCP) connects models to tools and context. Use it to give Codex access to third-party documentation, or to let it interact with developer tools like your browser or Figma.

Codex supports MCP servers in both the CLI and the IDE extension.

## Supported MCP features

- **STDIO servers**: Servers that run as a local process (started by a command).
  - Environment variables
- **Streamable HTTP servers**: Servers that you access at an address.
  - Bearer token authentication
  - OAuth authentication (run `codex mcp login <server-name>` for servers that support OAuth)

## Connect Codex to an MCP server

Codex stores MCP configuration in `~/.codex/config.toml` alongside other Codex configuration settings.

The CLI and the IDE extension share this configuration. Once you configure your MCP servers, you can switch between the two Codex clients without redoing setup.

To configure MCP servers, choose one option:

1. **Use the CLI**: Run `codex mcp` to add and manage servers.
2. **Edit `config.toml`**: Update `~/.codex/config.toml` directly.

### Configure with the CLI

#### Add an MCP server

```bash
codex mcp add <server-name> --env VAR1=VALUE1 --env VAR2=VALUE2 -- <stdio server-command>
```

For example, to add Context7 (a free MCP server for developer documentation), you can run the following command:

```bash
codex mcp add context7 -- npx -y @upstash/context7-mcp
```

#### Other CLI commands

To see all available MCP commands, you can run `codex mcp --help`.

#### Terminal UI (TUI)

In the `codex` TUI, use `/mcp` to see your active MCP servers.

### Configure with config.toml

For more fine-grained control over MCP server options, edit `~/.codex/config.toml`. In the IDE extension, select **MCP settings** > **Open config.toml** from the gear menu.

Configure each MCP server with a `[mcp_servers.<server-name>]` table in the configuration file.

#### STDIO servers

- `command` (required): The command that starts the server.
- `args` (optional): Arguments to pass to the server.
- `env` (optional): Environment variables to set for the server.
- `env_vars` (optional): Environment variables to allow and forward.
- `cwd` (optional): Working directory to start the server from.

#### Streamable HTTP servers

- `url` (required): The server address.
- `bearer_token_env_var` (optional): Environment variable name for a bearer token to send in `Authorization`.
- `http_headers` (optional): Map of header names to static values.
- `env_http_headers` (optional): Map of header names to environment variable names (values pulled from the environment).

#### Other configuration options

- `startup_timeout_sec` (optional): Timeout (seconds) for the server to start. Default: `10`.
- `tool_timeout_sec` (optional): Timeout (seconds) for the server to run a tool. Default: `60`.
- `enabled` (optional): Set `false` to disable a server without deleting it.
- `enabled_tools` (optional): Tool allow list.
- `disabled_tools` (optional): Tool deny list (applied after `enabled_tools`).

If your OAuth provider requires a static callback URI, set the top-level `mcp_oauth_callback_port` in `config.toml`. If unset, Codex binds to an ephemeral port.

#### config.toml examples

```toml
[mcp_servers.context7]
command = "npx"
args = ["-y", "@upstash/context7-mcp"]

[mcp_servers.context7.env]
MY_ENV_VAR = "MY_ENV_VALUE"
```

```toml
[mcp_servers.figma]
url = "https://mcp.figma.com/mcp"
bearer_token_env_var = "FIGMA_OAUTH_TOKEN"
http_headers = { "X-Figma-Region" = "us-east-1" }
```

```toml
[mcp_servers.chrome_devtools]
url = "http://localhost:3000/mcp"
enabled_tools = ["open", "screenshot"]
disabled_tools = ["screenshot"] # applied after enabled_tools
startup_timeout_sec = 20
tool_timeout_sec = 45
enabled = true
```

## Examples of useful MCP servers

The list of MCP servers keeps growing. Here are a few common ones:

- [OpenAI Docs MCP](https://developers.openai.com/resources/docs-mcp): Search and read OpenAI developer docs.
- [Context7](https://github.com/upstash/context7): Connect to up-to-date developer documentation.
- Figma [Local](https://developers.figma.com/docs/figma-mcp-server/local-server-installation/) and [Remote](https://developers.figma.com/docs/figma-mcp-server/remote-server-installation/): Access your Figma designs.
- [Playwright](https://www.npmjs.com/package/@playwright/mcp): Control and inspect a browser using Playwright.
- [Chrome Developer Tools](https://github.com/ChromeDevTools/chrome-devtools-mcp/): Control and inspect Chrome.
- [Sentry](https://docs.sentry.io/product/sentry-mcp/#codex): Access Sentry logs.
- [GitHub](https://github.com/github/github-mcp-server): Manage GitHub beyond what `git` supports (for example, pull requests and issues).


---


# Agent Skills

Use agent skills to extend Codex with task-specific capabilities. A skill packages instructions, resources, and optional scripts so Codex can follow a workflow reliably. You can share skills across teams or with the community. Skills build on the [open agent skills standard](https://agentskills.io).

Skills are available in both the Codex CLI and IDE extensions.

## Agent skill definition

A skill captures a capability expressed through Markdown instructions in a `SKILL.md` file. A skill folder can also include scripts, resources, and assets that Codex uses to perform a specific task.

<FileTree
  class="mt-4"
  tree={[
    {
      name: "my-skill/",
      open: true,
      children: [
        {
          name: "SKILL.md",
          comment: "Required: instructions + metadata",
        },
        {
          name: "scripts/",
          comment: "Optional: executable code",
        },
        {
          name: "references/",
          comment: "Optional: documentation",
        },
        {
          name: "assets/",
          comment: "Optional: templates, resources",
        },
      ],
    },
  ]}
/>

Skills use **progressive disclosure** to manage context efficiently. At startup, Codex loads the name and description of each available skill. Codex can then activate and use a skill in two ways:

1. **Explicit invocation:** You include skills directly in your prompt. To select one, run the `/skills` slash command, or start typing `$` to mention a skill. Codex web and iOS don't support explicit invocation yet, but you can still ask Codex to use any skill checked into a repo.

<div class="not-prose my-2 mb-4 grid gap-4 lg:grid-cols-2">
  <div>
    <img src="https://developers.openai.com/images/codex/skills/skills-selector-cli-light.webp"
      alt=""
      class="block w-full lg:h-64 rounded-lg border border-default my-0 object-contain bg-[#F0F1F5] dark:hidden"
    />
    <img src="https://developers.openai.com/images/codex/skills/skills-selector-cli-dark.webp"
      alt=""
      class="hidden w-full lg:h-64 rounded-lg border border-default my-0 object-contain bg-[#1E1E2E] dark:block"
    />
  </div>
  <div>
    <img src="https://developers.openai.com/images/codex/skills/skills-selector-ide-light.webp"
      alt=""
      class="block w-full lg:h-64 rounded-lg border border-default my-0 object-contain bg-[#E8E9ED] dark:hidden"
    />
    <img src="https://developers.openai.com/images/codex/skills/skills-selector-ide-dark.webp"
      alt=""
      class="hidden w-full lg:h-64 rounded-lg border border-default my-0 object-contain bg-[#181824] dark:block"
    />
  </div>
</div>

2. **Implicit invocation:** Codex can decide to use an available skill when your task matches the skill's description.

In either method, Codex reads the full instructions of the invoked skills and any extra references checked into the skill.

## Where to save skills

Team Config defines both the locations and precedence for skills. Codex loads skills from these locations in order of precedence (high to low). When skill names collide, higher-precedence locations override lower-precedence ones.

| Skill Scope | Location                                                                                                                                           | Suggested use                                                                                                                                                                                              |
| :---------- | :------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `REPO`      | `$CWD/.codex/skills` <br /> Current working directory: where you launch Codex.                                                                     | If you're in a repository or code environment, teams can check in skills relevant to a working folder. For example, skills only relevant to a microservice or a module.                                    |
| `REPO`      | `$CWD/../.codex/skills` <br /> A folder above CWD when you launch Codex inside a Git repository.                                                   | If you're in a repository with nested folders, organizations can check in skills relevant to a shared area in a parent folder.                                                                             |
| `REPO`      | `$REPO_ROOT/.codex/skills` <br /> The topmost root folder when you launch Codex inside a Git repository.                                           | If you're in a repository with nested folders, organizations can check in skills relevant to everyone using the repository. These serve as root skills that any subfolder in the repository can overwrite. |
| `USER`      | `$CODEX_HOME/skills` <br /> <small>(macOS and Linux default: `~/.codex/skills`)</small> <br /> Any skills checked into the user's personal folder. | Use to curate skills relevant to a user that apply to any repository the user may work in.                                                                                                                 |
| `ADMIN`     | `/etc/codex/skills` <br /> Any skills checked into the machine or container in a shared, system location.                                          | Use for SDK scripts, automation, and for checking in default admin skills available to each user on the machine.                                                                                           |
| `SYSTEM`    | Bundled with Codex.                                                                                                                                | Useful skills relevant to a broad audience such as the skill-creator and plan skills. Available to everyone when they start Codex and can be overwritten by any layer above.                               |

Codex supports symlinked skill folders and follows the symlink target when scanning these locations.

## Enable or disable skills

Per-skill enablement in `~/.codex/config.toml` is experimental and may change as needed. Use `[[skills.config]]` entries to disable a skill without deleting it, then restart Codex:

```toml
[[skills.config]]
path = "/path/to/skill"
enabled = false
```

## Create a skill

To create a new skill, use the built-in `$skill-creator` skill in Codex. Describe what you want your skill to do, and Codex will start bootstrapping your skill.

If you also install `$create-plan` (experimental) with `$skill-installer install the create-plan skill from the .experimental folder`, Codex will create a plan for your skill before it writes files.

For a step-by-step guide, see [Create custom skills](https://developers.openai.com/codex/skills/create-skill).

You can also create a skill manually by creating a folder with a `SKILL.md` file inside a valid skill location. A `SKILL.md` must contain a `name` and `description` to help Codex select the skill:

```md
---
name: skill-name
description: Description that helps Codex select the skill
metadata:
  short-description: Optional user-facing description
---

Skill instructions for the Codex agent to follow when using this skill.
```

Codex skills build on the [agent skills specification](https://agentskills.io/specification). Check out the documentation to learn more.

## Install new skills

To install more than the built-in skills, you can download skills from a [curated set of skills on GitHub](https://github.com/openai/skills) using the `$skill-installer` skill:

```bash
$skill-installer install the linear skill from the .experimental folder
```

You can also prompt the installer to download skills from other repositories.

After installing a skill, restart Codex to pick up new skills.

## Skill examples

### Plan a new feature

`$create-plan` is an experimental skill that you can install with `$skill-installer` to have Codex research and create a plan to build a new feature or solve a complex problem:

```bash
$skill-installer install the create-plan skill from the .experimental folder
```

### Access Linear context for Codex tasks

```bash
$skill-installer install the linear skill from the .experimental folder
```

<div class="not-prose my-4">
  <video
    class="w-full rounded-lg border border-default"
    controls
    playsinline
    preload="metadata"
  >
    <source
      src="https://cdn.openai.com/codex/docs/linear-example.mp4"
      type="video/mp4"
    />
  </video>
</div>

### Have Codex access Notion for more context

```bash
$skill-installer notion-spec-to-implementation
```

<div class="not-prose my-4">
  <video
    class="w-full rounded-lg border border-default"
    controls
    playsinline
    preload="metadata"
  >
    <source
      src="https://cdn.openai.com/codex/docs/notion-spec-example.mp4"
      type="video/mp4"
    />
  </video>
</div>


---


# Create skills

[Skills](https://developers.openai.com/codex/skills) let teams capture institutional knowledge and turn it into reusable, shareable workflows. Skills help Codex behave consistently across users, repositories, and sessions, which is especially useful when you want standard conventions and checks applied automatically.

A **skill** is a small bundle consisting of a `name`, a `description` that explains what it does and when to use it, and an optional body of instructions. Codex injects only the skill's name, description, and file path into the runtime context. The instruction body is never injected unless the skill is explicitly invoked.

## Decide when to create a skill

Use skills when you want to share behavior across a team, enforce consistent workflows, or encode best practices once and reuse them everywhere.

Typical use cases include:

- Standardizing code review checklists and conventions
- Enforcing security or compliance checks
- Automating common analysis tasks
- Providing team-specific tooling that Codex can discover automatically

Avoid skills for one-off prompts or exploratory tasks, and keep skills focused rather than trying to model large multi-step systems.

## Create a skill

### Use the skill creator

Codex ships with a built-in skill to create new skills. Use this method to receive guidance and iterate on your skill.

Invoke the skill creator from within the Codex CLI or the Codex IDE extension:

```text
$skill-creator
```

Optional: add context about what you want the skill to do.

```text
$skill-creator

Create a skill that drafts a conventional commit message based on a short summary of changes.
```

The creator asks what the skill does, when Codex should trigger it automatically, and the run type (instruction-only or script-backed). Use instruction-only by default.

The output is a `SKILL.md` file with a name, description, and instructions. If needed, it can also scaffold script stubs (Python or a container).

### Create a skill manually

Use this method when you want full control or are working directly in an editor.

1. Choose a location (repo-scoped or user-scoped).

   ```shell
   # User-scoped skill (macOS/Linux default)
   mkdir -p ~/.codex/skills/<skill-name>

   # Repo-scoped skill (checked into your repository)
   mkdir -p .codex/skills/<skill-name>
   ```

2. Create `SKILL.md`.

   ```md
   ---
   name: <skill-name>
   description: <what it does and when to use it>
   ---

   <instructions, references, or examples>
   ```

3. Restart Codex to load the skill.

## Understand the skill format

Skills use YAML front matter plus an optional body. Required fields are `name` (non-empty, at most 100 characters, single line) and `description` (non-empty, at most 500 characters, single line). Codex ignores extra keys. The body can contain any Markdown, stays on disk, and isn't injected into the runtime context unless explicitly invoked.

Along with inline instructions, skill directories often include:

- Scripts (for example, Python files) to perform deterministic processing, validation, or external tool calls
- Templates and schemas such as report templates, JSON/YAML schemas, or configuration defaults
- Reference data like lookup tables, prompts, or canned examples
- Documentation that explains assumptions, inputs, or expected outputs

<FileTree
  class="mt-4"
  tree={[
    {
      name: "my-skill/",
      open: true,
      children: [
        {
          name: "SKILL.md",
          comment: "Required: instructions + metadata",
        },
        {
          name: "scripts/",
          comment: "Optional: executable code",
        },
        {
          name: "references/",
          comment: "Optional: documentation",
        },
        {
          name: "assets/",
          comment: "Optional: templates, resources",
        },
      ],
    },
  ]}
/>

The skill's instructions reference these resources, but they remain on disk, keeping the runtime context small and predictable.

For real-world patterns and examples, see [agentskills.io](https://agentskills.io) and check out the skills catalog at [github.com/openai/skills](https://github.com/openai/skills).

## Choose where to save skills

Codex loads skills from these locations (repo, user, admin, and system scopes). Choose a location based on who should get the skill:

- Save skills in your repository's `.codex/skills/` when they should travel with the codebase.
- Save skills in your user skills directory when they should apply across all repositories on your machine.
- Use admin/system locations only in managed environments (for example, when loading skills on shared machines).

For the full list of supported locations and precedence, see the "Where to save skills" section on the [Skills overview](https://developers.openai.com/codex/skills#where-to-save-skills).

## See an example skill

```md
---
name: draft-commit-message
description: Draft a conventional commit message when the user asks for help writing a commit message.
metadata:
  short-description: Draft an informative commit message.
---

Draft a conventional commit message that matches the change summary provided by the user.

Requirements:

- Use the Conventional Commits format: `type(scope): summary`
- Use the imperative mood in the summary (for example, "Add", "Fix", "Refactor")
- Keep the summary under 72 characters
- If there are breaking changes, include a `BREAKING CHANGE:` footer
```

Example prompt that triggers this skill:

```text
Help me write a commit message for these changes: I renamed `SkillCreator` to `SkillsCreator` and updated the sidebar.
```

Check out more example skills and ideas in the [github.com/openai/skills](https://github.com/openai/skills) repository.

## Follow best practices

- Be explicit about triggers. The `description` tells Codex when to trigger a skill.
- Keep skills small. Prefer narrow, modular skills over large ones.
- Prefer instructions over scripts. Use scripts only when you need determinism or external data.
- Assume no context. Write instructions as if Codex knows nothing beyond the input.
- Avoid ambiguity. Use imperative, step-by-step language.
- Test triggers. Verify your example prompts activate the skill as expected.

## Troubleshoot skills

### Skill doesn’t appear

If a skill doesn’t show up in Codex, make sure you enabled skills and restarted Codex. Confirm the file name is exactly `SKILL.md` and that it lives under a supported path such as `~/.codex/skills`.

If you’ve disabled a skill in `~/.codex/config.toml`, remove or flip the matching `[[skills.config]]` entry and restart Codex.

If you use symlinked directories, confirm the symlink target exists and is readable. Codex also skips skills with malformed YAML or `name`/`description` fields that exceed the length limits.

### Skill doesn’t trigger

If a skill loads but doesn’t run automatically, the most common issue is an unclear trigger. Make sure the `description` explicitly states when to use the skill, and test with prompts that match that description.

If two or more skills overlap in intent, narrow the description so Codex can select the correct one.

### Startup validation errors

If Codex reports validation errors at startup, fix the listed issues in `SKILL.md`. Most often, this is a multi-line or over-length `name` or `description`. Restart Codex to reload skills.


---


# Non-interactive mode

Non-interactive mode lets you run Codex from scripts (for example, continuous integration (CI) jobs) without opening the interactive TUI.
You invoke it with `codex exec`.

For flag-level details, see [`codex exec`](https://developers.openai.com/codex/cli/reference#codex-exec).

## When to use `codex exec`

Use `codex exec` when you want Codex to:

- Run as part of a pipeline (CI, pre-merge checks, scheduled jobs).
- Produce output you can pipe into other tools (for example, to generate release notes or summaries).
- Run with explicit, pre-set sandbox and approval settings.

## Basic usage

Pass a task prompt as a single argument:

```bash
codex exec "summarize the repository structure and list the top 5 risky areas"
```

While `codex exec` runs, Codex streams progress to `stderr` and prints only the final agent message to `stdout`. This makes it straightforward to redirect or pipe the final result:

```bash
codex exec "generate release notes for the last 10 commits" | tee release-notes.md
```

## Permissions and safety

By default, `codex exec` runs in a read-only sandbox. In automation, set the least permissions needed for the workflow:

- Allow edits: `codex exec --full-auto "<task>"`
- Allow broader access: `codex exec --sandbox danger-full-access "<task>"`

Use `danger-full-access` only in a controlled environment (for example, an isolated CI runner or container).

## Make output machine-readable

To consume Codex output in scripts, use JSON Lines output:

```bash
codex exec --json "summarize the repo structure" | jq
```

When you enable `--json`, `stdout` becomes a JSON Lines (JSONL) stream so you can capture every event Codex emits while it's running. Event types include `thread.started`, `turn.started`, `turn.completed`, `turn.failed`, `item.*`, and `error`.

Item types include agent messages, reasoning, command executions, file changes, MCP tool calls, web searches, and plan updates.

Sample JSON stream (each line is a JSON object):

```jsonl
{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a53"}
{"type":"turn.started"}
{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"bash -lc ls","status":"in_progress"}}
{"type":"item.completed","item":{"id":"item_3","type":"agent_message","text":"Repo contains docs, sdk, and examples directories."}}
{"type":"turn.completed","usage":{"input_tokens":24763,"cached_input_tokens":24448,"output_tokens":122}}
```

If you only need the final message, write it to a file with `-o <path>`/`--output-last-message <path>`. This writes the final message to the file and still prints it to `stdout` (see [`codex exec`](https://developers.openai.com/codex/cli/reference#codex-exec) for details).

## Create structured outputs with a schema

If you need structured data for downstream steps, use `--output-schema` to request a final response that conforms to a JSON Schema.
This is useful for automated workflows that need stable fields (for example, job summaries, risk reports, or release metadata).

`schema.json`

```json
{
  "type": "object",
  "properties": {
    "project_name": { "type": "string" },
    "programming_languages": {
      "type": "array",
      "items": { "type": "string" }
    }
  },
  "required": ["project_name", "programming_languages"],
  "additionalProperties": false
}
```

Run Codex with the schema and write the final JSON response to disk:

```bash
codex exec "Extract project metadata" \
  --output-schema ./schema.json \
  -o ./project-metadata.json
```

Example final output (stdout):

```json
{
  "project_name": "Codex CLI",
  "programming_languages": ["Rust", "TypeScript", "Shell"]
}
```

## Authenticate in CI

`codex exec` reuses saved CLI authentication by default. In CI, it's common to provide credentials explicitly:

- Set `CODEX_API_KEY` as a secret environment variable for the job.
- Keep prompts and tool output in mind: they can include sensitive code or data.

To use a different API key for a single run, set `CODEX_API_KEY` inline:

```bash
CODEX_API_KEY=<api-key> codex exec --json "triage open bug reports"
```

`CODEX_API_KEY` is only supported in `codex exec`.

## Resume a non-interactive session

If you need to continue a previous run (for example, a two-stage pipeline), use the `resume` subcommand:

```bash
codex exec "review the change for race conditions"
codex exec resume --last "fix the race conditions you found"
```

You can also target a specific session ID with `codex exec resume <SESSION_ID>`.

## Git repository required

Codex requires commands to run inside a Git repository to prevent destructive changes. Override this check with `codex exec --skip-git-repo-check` if you're sure the environment is safe.

## Common automation patterns

### Example: Autofix CI failures in GitHub Actions

You can use `codex exec` to automatically propose fixes when a CI workflow fails. The typical pattern is:

1. Trigger a follow-up workflow when your main CI workflow completes with an error.
2. Check out the failing commit SHA.
3. Install dependencies and run Codex with a narrow prompt and minimal permissions.
4. Re-run the test command.
5. Open a pull request with the resulting patch.

#### Minimal workflow using the Codex CLI

The example below shows the core steps. Adjust the install and test commands to match your stack.

```yaml
name: Codex auto-fix on CI failure

on:
  workflow_run:
    workflows: ["CI"]
    types: [completed]

permissions:
  contents: write
  pull-requests: write

jobs:
  auto-fix:
    if: ${{ github.event.workflow_run.conclusion == 'failure' }}
    runs-on: ubuntu-latest
    env:
      OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
      FAILED_HEAD_SHA: ${{ github.event.workflow_run.head_sha }}
      FAILED_HEAD_BRANCH: ${{ github.event.workflow_run.head_branch }}
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ env.FAILED_HEAD_SHA }}
          fetch-depth: 0

      - uses: actions/setup-node@v4
        with:
          node-version: "20"

      - name: Install dependencies
        run: |
          if [ -f package-lock.json ]; then npm ci; else npm i; fi

      - name: Install Codex
        run: npm i -g @openai/codex

      - name: Authenticate Codex
        run: codex login --api-key "$OPENAI_API_KEY"

      - name: Run Codex
        run: |
          codex exec --full-auto --sandbox workspace-write \
            "Read the repository, run the test suite, identify the minimal change needed to make all tests pass, implement only that change, and stop. Do not refactor unrelated files."

      - name: Verify tests
        run: npm test --silent

      - name: Create pull request
        if: success()
        uses: peter-evans/create-pull-request@v6
        with:
          branch: codex/auto-fix-${{ github.event.workflow_run.run_id }}
          base: ${{ env.FAILED_HEAD_BRANCH }}
          title: "Auto-fix failing CI via Codex"
```

#### Alternative: Use the Codex GitHub Action

If you want to avoid installing the CLI yourself, you can run `codex exec` through the [Codex GitHub Action](https://developers.openai.com/codex/github-action) and pass the prompt as an input.


---


# Use Codex with the Agents SDK

# Running Codex as an MCP server

You can run Codex as an MCP server and connect it from other MCP clients (for example, an agent built with the [OpenAI Agents SDK](https://openai.github.io/openai-agents-js/guides/mcp/)).

To start Codex as an MCP server, you can use the following command:

```bash
codex mcp-server
```

You can launch a Codex MCP server with the [Model Context Protocol Inspector](https://modelcontextprotocol.io/legacy/tools/inspector):

```bash
npx @modelcontextprotocol/inspector codex mcp-server
```

Send a `tools/list` request to see two tools:

**`codex`**: Run a Codex session. Accepts configuration parameters that match the Codex `Config` struct. The `codex` tool takes these properties:

| Property                | Type      | Description                                                                                                  |
| ----------------------- | --------- | ------------------------------------------------------------------------------------------------------------ |
| **`prompt`** (required) | `string`  | The initial user prompt to start the Codex conversation.                                                     |
| `approval-policy`       | `string`  | Approval policy for shell commands generated by the model: `untrusted`, `on-request`, `on-failure`, `never`. |
| `base-instructions`     | `string`  | The set of instructions to use instead of the default ones.                                                  |
| `config`                | `object`  | Individual configuration settings that override what's in `$CODEX_HOME/config.toml`.                         |
| `cwd`                   | `string`  | Working directory for the session. If relative, resolved against the server process's current directory.     |
| `include-plan-tool`     | `boolean` | Whether to include the plan tool in the conversation.                                                        |
| `model`                 | `string`  | Optional override for the model name (for example, `o3`, `o4-mini`).                                         |
| `profile`               | `string`  | Configuration profile from `config.toml` to specify default options.                                         |
| `sandbox`               | `string`  | Sandbox mode: `read-only`, `workspace-write`, or `danger-full-access`.                                       |

**`codex-reply`**: Continue a Codex session by providing the thread ID and prompt. The `codex-reply` tool takes these properties:

| Property                      | Type   | Description                                               |
| ----------------------------- | ------ | --------------------------------------------------------- |
| **`prompt`** (required)       | string | The next user prompt to continue the Codex conversation.  |
| **`threadId`** (required)     | string | The ID of the thread to continue.                         |
| `conversationId` (deprecated) | string | Deprecated alias for `threadId` (kept for compatibility). |

Use the `threadId` from `structuredContent.threadId` in the `tools/call` response. Approval elicitations (exec/patch) also include `threadId` in their `params` payload.

Example response payload:

```json
{
  "structuredContent": {
    "threadId": "019bbb20-bff6-7130-83aa-bf45ab33250e",
    "content": "`ls -lah` (or `ls -alh`) — long listing, includes dotfiles, human-readable sizes."
  },
  "content": [
    {
      "type": "text",
      "text": "`ls -lah` (or `ls -alh`) — long listing, includes dotfiles, human-readable sizes."
    }
  ]
}
```

Note modern MCP clients generally report only `"structuredContent"` as the result of a tool call, if present, though the Codex MCP server also returns `"content"` for the benefit of older MCP clients.

# Creating multi-agent workflows

Codex CLI can do far more than run ad-hoc tasks. By exposing the CLI as a [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server and orchestrating it with the OpenAI Agents SDK, you can create deterministic, auditable workflows that scale from a single agent to a complete software delivery pipeline.

This guide walks through the same workflow showcased in the [OpenAI Cookbook](https://github.com/openai/openai-cookbook/blob/main/examples/codex/codex_mcp_agents_sdk/building_consistent_workflows_codex_cli_agents_sdk.ipynb). You will:

- launch Codex CLI as a long-running MCP server,
- build a focused single-agent workflow that produces a playable browser game, and
- orchestrate a multi-agent team with hand-offs, guardrails, and full traces you can review afterwards.

Before starting, make sure you have:

- [Codex CLI](https://developers.openai.com/codex/cli) installed locally so `npx codex` can run.
- Python 3.10+ with `pip`.
- Node.js 18+ (required for `npx`).
- An OpenAI API key stored locally. You can create or manage keys in the [OpenAI dashboard](https://platform.openai.com/account/api-keys).

Create a working directory for the guide and add your API key to a `.env` file:

```bash
mkdir codex-workflows
cd codex-workflows
printf "OPENAI_API_KEY=sk-..." > .env
```

## Install dependencies

The Agents SDK handles orchestration across Codex, hand-offs, and traces. Install the latest SDK packages:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade openai openai-agents python-dotenv
```

<DocsTip>
  Activating a virtual environment keeps the SDK dependencies isolated from the
  rest of your system.
</DocsTip>

## Initialize Codex CLI as an MCP server

Start by turning Codex CLI into an MCP server that the Agents SDK can call. The server exposes two tools—`codex()` to start a conversation and `codex-reply()` to continue one—and keeps Codex alive across multiple agent turns.

Create a file called `codex_mcp.py` and add the following:

```python
import asyncio

from agents import Agent, Runner
from agents.mcp import MCPServerStdio


async def main() -> None:
    async with MCPServerStdio(
        name="Codex CLI",
        params={
            "command": "npx",
            "args": ["-y", "codex", "mcp-server"],
        },
        client_session_timeout_seconds=360000,
    ) as codex_mcp_server:
        print("Codex MCP server started.")
        # More logic coming in the next sections.
        return


if __name__ == "__main__":
    asyncio.run(main())
```

Run the script once to verify that Codex launches successfully:

```bash
python codex_mcp.py
```

The script exits after printing `Codex MCP server started.`. In the next sections you will reuse the same MCP server inside richer workflows.

## Build a single-agent workflow

Let’s start with a scoped example that uses Codex MCP to ship a small browser game. The workflow relies on two agents:

1. **Game Designer** – writes a brief for the game.
2. **Game Developer** – implements the game by calling Codex MCP.

Update `codex_mcp.py` with the following code. It keeps the MCP server setup from above and adds both agents.

```python
import asyncio
import os

from dotenv import load_dotenv

from agents import Agent, Runner, set_default_openai_api
from agents.mcp import MCPServerStdio

load_dotenv(override=True)
set_default_openai_api(os.getenv("OPENAI_API_KEY"))


async def main() -> None:
    async with MCPServerStdio(
        name="Codex CLI",
        params={
            "command": "npx",
            "args": ["-y", "codex", "mcp-server"],
        },
        client_session_timeout_seconds=360000,
    ) as codex_mcp_server:
        developer_agent = Agent(
            name="Game Developer",
            instructions=(
                "You are an expert in building simple games using basic html + css + javascript with no dependencies. "
                "Save your work in a file called index.html in the current directory. "
                "Always call codex with \"approval-policy\": \"never\" and \"sandbox\": \"workspace-write\"."
            ),
            mcp_servers=[codex_mcp_server],
        )

        designer_agent = Agent(
            name="Game Designer",
            instructions=(
                "You are an indie game connoisseur. Come up with an idea for a single page html + css + javascript game that a developer could build in about 50 lines of code. "
                "Format your request as a 3 sentence design brief for a game developer and call the Game Developer coder with your idea."
            ),
            model="gpt-5",
            handoffs=[developer_agent],
        )

        await Runner.run(designer_agent, "Implement a fun new game!")


if __name__ == "__main__":
    asyncio.run(main())
```

Execute the script:

```bash
python codex_mcp.py
```

Codex will read the designer’s brief, create an `index.html` file, and write the full game to disk. Open the generated file in a browser to play the result. Every run produces a different design with unique gameplay twists and polish.

## Expand to a multi-agent workflow

Now turn the single-agent setup into an orchestrated, traceable workflow. The system adds:

- **Project Manager** – creates shared requirements, coordinates hand-offs, and enforces guardrails.
- **Designer**, **Frontend Developer**, **Backend Developer**, and **Tester** – each with scoped instructions and output folders.

Create a new file called `multi_agent_workflow.py`:

```python
import asyncio
import os

from dotenv import load_dotenv

from agents import (
    Agent,
    ModelSettings,
    Runner,
    WebSearchTool,
    set_default_openai_api,
)
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX
from agents.mcp import MCPServerStdio
from openai.types.shared import Reasoning

load_dotenv(override=True)
set_default_openai_api(os.getenv("OPENAI_API_KEY"))


async def main() -> None:
    async with MCPServerStdio(
        name="Codex CLI",
        params={"command": "npx", "args": ["-y", "codex", "mcp"]},
        client_session_timeout_seconds=360000,
    ) as codex_mcp_server:
        designer_agent = Agent(
            name="Designer",
            instructions=(
                f"""{RECOMMENDED_PROMPT_PREFIX}"""
                "You are the Designer.\n"
                "Your only source of truth is AGENT_TASKS.md and REQUIREMENTS.md from the Project Manager.\n"
                "Do not assume anything that is not written there.\n\n"
                "You may use the internet for additional guidance or research."
                "Deliverables (write to /design):\n"
                "- design_spec.md – a single page describing the UI/UX layout, main screens, and key visual notes as requested in AGENT_TASKS.md.\n"
                "- wireframe.md – a simple text or ASCII wireframe if specified.\n\n"
                "Keep the output short and implementation-friendly.\n"
                "When complete, handoff to the Project Manager with transfer_to_project_manager."
                "When creating files, call Codex MCP with {\"approval-policy\":\"never\",\"sandbox\":\"workspace-write\"}."
            ),
            model="gpt-5",
            tools=[WebSearchTool()],
            mcp_servers=[codex_mcp_server],
        )

        frontend_developer_agent = Agent(
            name="Frontend Developer",
            instructions=(
                f"""{RECOMMENDED_PROMPT_PREFIX}"""
                "You are the Frontend Developer.\n"
                "Read AGENT_TASKS.md and design_spec.md. Implement exactly what is described there.\n\n"
                "Deliverables (write to /frontend):\n"
                "- index.html – main page structure\n"
                "- styles.css or inline styles if specified\n"
                "- main.js or game.js if specified\n\n"
                "Follow the Designer’s DOM structure and any integration points given by the Project Manager.\n"
                "Do not add features or branding beyond the provided documents.\n\n"
                "When complete, handoff to the Project Manager with transfer_to_project_manager_agent."
                "When creating files, call Codex MCP with {\"approval-policy\":\"never\",\"sandbox\":\"workspace-write\"}."
            ),
            model="gpt-5",
            mcp_servers=[codex_mcp_server],
        )

        backend_developer_agent = Agent(
            name="Backend Developer",
            instructions=(
                f"""{RECOMMENDED_PROMPT_PREFIX}"""
                "You are the Backend Developer.\n"
                "Read AGENT_TASKS.md and REQUIREMENTS.md. Implement the backend endpoints described there.\n\n"
                "Deliverables (write to /backend):\n"
                "- package.json – include a start script if requested\n"
                "- server.js – implement the API endpoints and logic exactly as specified\n\n"
                "Keep the code as simple and readable as possible. No external database.\n\n"
                "When complete, handoff to the Project Manager with transfer_to_project_manager_agent."
                "When creating files, call Codex MCP with {\"approval-policy\":\"never\",\"sandbox\":\"workspace-write\"}."
            ),
            model="gpt-5",
            mcp_servers=[codex_mcp_server],
        )

        tester_agent = Agent(
            name="Tester",
            instructions=(
                f"""{RECOMMENDED_PROMPT_PREFIX}"""
                "You are the Tester.\n"
                "Read AGENT_TASKS.md and TEST.md. Verify that the outputs of the other roles meet the acceptance criteria.\n\n"
                "Deliverables (write to /tests):\n"
                "- TEST_PLAN.md – bullet list of manual checks or automated steps as requested\n"
                "- test.sh or a simple automated script if specified\n\n"
                "Keep it minimal and easy to run.\n\n"
                "When complete, handoff to the Project Manager with transfer_to_project_manager."
                "When creating files, call Codex MCP with {\"approval-policy\":\"never\",\"sandbox\":\"workspace-write\"}."
            ),
            model="gpt-5",
            mcp_servers=[codex_mcp_server],
        )

        project_manager_agent = Agent(
            name="Project Manager",
            instructions=(
                f"""{RECOMMENDED_PROMPT_PREFIX}"""
                """
                You are the Project Manager.

                Objective:
                Convert the input task list into three project-root files the team will execute against.

                Deliverables (write in project root):
                - REQUIREMENTS.md: concise summary of product goals, target users, key features, and constraints.
                - TEST.md: tasks with [Owner] tags (Designer, Frontend, Backend, Tester) and clear acceptance criteria.
                - AGENT_TASKS.md: one section per role containing:
                  - Project name
                  - Required deliverables (exact file names and purpose)
                  - Key technical notes and constraints

                Process:
                - Resolve ambiguities with minimal, reasonable assumptions. Be specific so each role can act without guessing.
                - Create files using Codex MCP with {"approval-policy":"never","sandbox":"workspace-write"}.
                - Do not create folders. Only create REQUIREMENTS.md, TEST.md, AGENT_TASKS.md.

                Handoffs (gated by required files):
                1) After the three files above are created, hand off to the Designer with transfer_to_designer_agent and include REQUIREMENTS.md and AGENT_TASKS.md.
                2) Wait for the Designer to produce /design/design_spec.md. Verify that file exists before proceeding.
                3) When design_spec.md exists, hand off in parallel to both:
                   - Frontend Developer with transfer_to_frontend_developer_agent (provide design_spec.md, REQUIREMENTS.md, AGENT_TASKS.md).
                   - Backend Developer with transfer_to_backend_developer_agent (provide REQUIREMENTS.md, AGENT_TASKS.md).
                4) Wait for Frontend to produce /frontend/index.html and Backend to produce /backend/server.js. Verify both files exist.
                5) When both exist, hand off to the Tester with transfer_to_tester_agent and provide all prior artifacts and outputs.
                6) Do not advance to the next handoff until the required files for that step are present. If something is missing, request the owning agent to supply it and re-check.

                PM Responsibilities:
                - Coordinate all roles, track file completion, and enforce the above gating checks.
                - Do NOT respond with status updates. Just handoff to the next agent until the project is complete.
                """
            ),
            model="gpt-5",
            model_settings=ModelSettings(
                reasoning=Reasoning(effort="medium"),
            ),
            handoffs=[designer_agent, frontend_developer_agent, backend_developer_agent, tester_agent],
            mcp_servers=[codex_mcp_server],
        )

        designer_agent.handoffs = [project_manager_agent]
        frontend_developer_agent.handoffs = [project_manager_agent]
        backend_developer_agent.handoffs = [project_manager_agent]
        tester_agent.handoffs = [project_manager_agent]

        task_list = """
Goal: Build a tiny browser game to showcase a multi-agent workflow.

High-level requirements:
- Single-screen game called "Bug Busters".
- Player clicks a moving bug to earn points.
- Game ends after 20 seconds and shows final score.
- Optional: submit score to a simple backend and display a top-10 leaderboard.

Roles:
- Designer: create a one-page UI/UX spec and basic wireframe.
- Frontend Developer: implement the page and game logic.
- Backend Developer: implement a minimal API (GET /health, GET/POST /scores).
- Tester: write a quick test plan and a simple script to verify core routes.

Constraints:
- No external database—memory storage is fine.
- Keep everything readable for beginners; no frameworks required.
- All outputs should be small files saved in clearly named folders.
"""

        result = await Runner.run(project_manager_agent, task_list, max_turns=30)
        print(result.final_output)


if __name__ == "__main__":
    asyncio.run(main())
```

Run the script and watch the generated files:

```bash
python multi_agent_workflow.py
ls -R
```

The project manager agent writes `REQUIREMENTS.md`, `TEST.md`, and `AGENT_TASKS.md`, then coordinates hand-offs across the designer, frontend, backend, and tester agents. Each agent writes scoped artifacts in its own folder before handing control back to the project manager.

## Trace the workflow

Codex automatically records traces that capture every prompt, tool call, and hand-off. After the multi-agent run completes, open the [Traces dashboard](https://platform.openai.com/trace) to inspect the execution timeline.

The high-level trace highlights how the project manager verifies hand-offs before moving forward. Click into individual steps to see prompts, Codex MCP calls, files written, and execution durations. These details make it easy to audit every hand-off and understand how the workflow evolved turn by turn.
These traces make it easy to debug workflow hiccups, audit agent behavior, and measure performance over time without requiring any additional instrumentation.


---


# Authentication

## OpenAI authentication

Codex supports two ways to sign in when using OpenAI models:

- Sign in with ChatGPT for subscription access
- Sign in with an API key for usage-based access

Codex cloud requires signing in with ChatGPT. The Codex CLI and IDE extension support both sign-in methods.

### Sign in with ChatGPT

When you sign in with ChatGPT from the Codex CLI or IDE extension, Codex opens a browser window for you to complete the login flow. After you sign in, the browser returns an access token to the CLI or IDE extension.

### Sign in with an API key

You can also sign in to the Codex CLI or IDE extension with an API key. Get your API key from the [OpenAI dashboard](https://platform.openai.com/api-keys).

OpenAI bills API key usage through your OpenAI Platform account at standard API rates. See the [API pricing page](https://openai.com/api/pricing/).

## Secure your Codex cloud account

Codex cloud interacts directly with your codebase, so it needs stronger security than many other ChatGPT features. Enable multi-factor authentication (MFA).

If you use a social login provider (Google, Microsoft, Apple), you aren't required to enable MFA on your ChatGPT account, but you can set it up with your social login provider.

For setup instructions, see:

- [Google](https://support.google.com/accounts/answer/185839)
- [Microsoft](https://support.microsoft.com/en-us/topic/what-is-multifactor-authentication-e5e39437-121c-be60-d123-eda06bddf661)
- [Apple](https://support.apple.com/en-us/102660)

If you access ChatGPT through single sign-on (SSO), your organization's SSO administrator should enforce MFA for all users.

If you log in using an email and password, you must set up MFA on your account before accessing Codex cloud.

If your account supports more than one login method and one of them is email and password, you must set up MFA before accessing Codex, even if you sign in another way.

## Login caching

When you sign in to the Codex CLI or IDE extension using either ChatGPT or an API key, Codex caches your login details and reuses them the next time you start the CLI or extension. The CLI and extension share the same cached login details. If you log out from either one, you'll need to sign in again the next time you start the CLI or extension.

Codex caches login details locally in a plaintext file at `~/.codex/auth.json` or in your OS-specific credential store.

## Credential storage

Use `cli_auth_credentials_store` to control where the Codex CLI stores cached credentials:

```toml
# file | keyring | auto
cli_auth_credentials_store = "keyring"
```

- `file` stores credentials in `auth.json` under `CODEX_HOME` (defaults to `~/.codex`).
- `keyring` stores credentials in your operating system credential store.
- `auto` uses the OS credential store when available, otherwise falls back to `auth.json`.

<DocsTip>
  If you use file-based storage, treat `~/.codex/auth.json` like a password: it
  contains access tokens. Don't commit it, paste it into tickets, or share it in
  chat.
</DocsTip>

## Enforce a login method or workspace

In managed environments, admins may restrict how users are allowed to authenticate:

```toml
# Only allow ChatGPT login or only allow API key login.
forced_login_method = "chatgpt" # or "api"

# When using ChatGPT login, restrict users to a specific workspace.
forced_chatgpt_workspace_id = "00000000-0000-0000-0000-000000000000"
```

If the active credentials don't match the configured restrictions, Codex logs the user out and exits.

These settings are commonly applied via managed configuration rather than per-user setup. See [Managed configuration](https://developers.openai.com/codex/security#managed-configuration).

## Login on headless devices

If you are signing in to ChatGPT with the Codex CLI, there are some situations where the browser-based login UI may not work:

- You're running the CLI in a remote or headless environment.
- Your local networking configuration blocks the localhost callback Codex uses to return the OAuth token to the CLI after you sign in.

In these situations, prefer device code authentication (beta). In the interactive login UI, choose **Sign in with Device Code**, or run `codex login --device-auth` directly. If device code authentication doesn't work in your environment, use one of the fallback methods.

### Preferred: Device code authentication (beta)

1. Enable device code login in your ChatGPT security settings (personal account) or ChatGPT workspace permissions (workspace admin).
2. In the terminal where you're running Codex, choose one of these options:
   - In the interactive login UI, select **Sign in with Device Code**.
   - Run `codex login --device-auth`.
3. Open the link in your browser, sign in, then enter the one-time code.

If device code login isn't enabled by the server, Codex falls back to the standard browser-based login flow.

### Fallback: Authenticate locally and copy your auth cache

If you can complete the login flow on a machine with a browser, you can copy your cached credentials to the headless machine.

1. On a machine where you can use the browser-based login flow, run `codex login`.
2. Confirm the login cache exists at `~/.codex/auth.json`.
3. Copy `~/.codex/auth.json` to `~/.codex/auth.json` on the headless machine.

Treat `~/.codex/auth.json` like a password: it contains access tokens. Don't commit it, paste it into tickets, or share it in chat.

If your OS stores credentials in a credential store instead of `~/.codex/auth.json`, this method may not apply. See
[Credential storage](#credential-storage) for how to configure file-based storage.

Copy to a remote machine over SSH:

```shell
ssh user@remote 'mkdir -p ~/.codex'
scp ~/.codex/auth.json user@remote:~/.codex/auth.json
```

Or use a one-liner that avoids `scp`:

```shell
ssh user@remote 'mkdir -p ~/.codex && cat > ~/.codex/auth.json' < ~/.codex/auth.json
```

Copy into a Docker container:

```shell
# Replace MY_CONTAINER with the name or ID of your container.
CONTAINER_HOME=$(docker exec MY_CONTAINER printenv HOME)
docker exec MY_CONTAINER mkdir -p "$CONTAINER_HOME/.codex"
docker cp ~/.codex/auth.json MY_CONTAINER:"$CONTAINER_HOME/.codex/auth.json"
```

### Fallback: Forward the localhost callback over SSH

If you can forward ports between your local machine and the remote host, you can use the standard browser-based flow by tunneling Codex's local callback server (default `localhost:1455`).

1. From your local machine, start port forwarding:

```shell
ssh -L 1455:localhost:1455 user@remote
```

2. In that SSH session, run `codex login` and follow the printed address on your local machine.

## Alternative model providers

When you define a [custom model provider](https://developers.openai.com/codex/config-advanced#custom-model-providers) in your configuration file, you can choose one of these authentication methods:

- **OpenAI authentication**: Set `requires_openai_auth = true` to use OpenAI authentication. You can then sign in with ChatGPT or an API key. This is useful when you access OpenAI models through an LLM proxy server. When `requires_openai_auth = true`, Codex ignores `env_key`.
- **Environment variable authentication**: Set `env_key = "<ENV_VARIABLE_NAME>"` to use a provider-specific API key from the local environment variable named `<ENV_VARIABLE_NAME>`.
- **No authentication**: If you don't set `requires_openai_auth` (or set it to `false`) and you don't set `env_key`, Codex assumes the provider doesn't require authentication. This is useful for local models.


---


# Security

Codex helps protect your code and data and reduces the risk of misuse.

By default, the agent runs with network access turned off. Locally, Codex uses an OS-enforced sandbox that limits what it can touch (typically to the current workspace), plus an approval policy that controls when it must stop and ask you before acting.

## Sandbox and approvals

Codex security controls come from two layers that work together:

- **Sandbox mode**: What Codex can do technically (for example, where it can write and whether it can reach the network) when it executes model-generated commands.
- **Approval policy**: When Codex must ask you before it executes an action (for example, leaving the sandbox, using the network, or running commands outside a trusted set).

Codex uses different sandbox modes depending on where you run it:

- **Codex cloud**: Runs in isolated OpenAI-managed containers, preventing access to your host system or unrelated data. You can expand access intentionally (for example, to install dependencies or allow specific domains) when needed. Network access is always enabled during the setup phase, which runs before the agent has access to your code.
- **Codex CLI / IDE extension**: OS-level mechanisms enforce sandbox policies. Defaults include no network access and write permissions limited to the active workspace. You can configure the sandbox, approval policy, and network settings based on your risk tolerance.

In the `Auto` preset (for example, `--full-auto`), Codex can read files, make edits, and run commands in the working directory automatically.

Codex asks for approval to edit files outside the workspace or to run commands that require network access. If you want to chat or plan without making changes, switch to `read-only` mode with the `/approvals` command.

## Network access

For Codex cloud, see [agent internet access](https://developers.openai.com/codex/cloud/internet-access) to enable full internet access or a domain allow list.

For the Codex CLI or IDE extension, the default `workspace-write` sandbox mode keeps network access turned off unless you enable it in your configuration:

```toml
[sandbox_workspace_write]
network_access = true
```

You can also enable the [web search tool](https://platform.openai.com/docs/guides/tools-web-search) without allowing full network access by passing the `--search` flag or toggling the feature in `config.toml`:

```toml
[features]
web_search_request = true
```

Use caution when enabling network access or web search in Codex. Prompt injection can cause the agent to fetch and follow untrusted instructions.

## Defaults and recommendations

- On launch, Codex detects whether the folder is version-controlled and recommends:
  - Version-controlled folders: `Auto` (workspace write + on-request approvals)
  - Non-version-controlled folders: `read-only`
- Depending on your setup, Codex may also start in `read-only` until you explicitly trust the working directory (for example, via an onboarding prompt or `/approvals`).
- The workspace includes the current directory and temporary directories like `/tmp`. Use the `/status` command to see which directories are in the workspace.
- To accept the defaults, run `codex`.
- You can set these explicitly:
  - `codex --sandbox workspace-write --ask-for-approval on-request`
  - `codex --sandbox read-only --ask-for-approval on-request`

### Run without approval prompts

You can disable approval prompts with `--ask-for-approval never` or `-a never` (shorthand).

This option works with all `--sandbox` modes, so you still control Codex's level of autonomy. Codex makes a best effort within the constraints you set.

If you need Codex to read files, make edits, and run commands with network access without approval prompts, use `--sandbox danger-full-access` (or the `--dangerously-bypass-approvals-and-sandbox` flag). Use caution before doing so.

### Common sandbox and approval combinations

| Intent                                                            | Flags                                                          | Effect                                                                                                                                           |
| ----------------------------------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Auto (preset)                                                     | _no flags needed_ or `--full-auto`                             | Codex can read files, make edits, and run commands in the workspace. Codex requires approval to edit outside the workspace or to access network. |
| Safe read-only browsing                                           | `--sandbox read-only --ask-for-approval on-request`            | Codex can read files and answer questions. Codex requires approval to make edits, run commands, or access network.                               |
| Read-only non-interactive (CI)                                    | `--sandbox read-only --ask-for-approval never`                 | Codex can only read files; never asks for approval.                                                                                              |
| Automatically edit but ask for approval to run untrusted commands | `--sandbox workspace-write --ask-for-approval untrusted`       | Codex can read and edit files but asks for approval before running untrusted commands.                                                           |
| Dangerous full access                                             | `--dangerously-bypass-approvals-and-sandbox` (alias: `--yolo`) | No sandbox; no approvals _(not recommended)_                                                                                                     |

`--full-auto` is a convenience alias for `--sandbox workspace-write --ask-for-approval on-request`.

#### Configuration in `config.toml`

```toml
# Always ask for approval mode
approval_policy = "untrusted"
sandbox_mode    = "read-only"

# Optional: Allow network in workspace-write mode
[sandbox_workspace_write]
network_access = true
```

You can also save presets as profiles, then select them with `codex --profile <name>`:

```toml
[profiles.full_auto]
approval_policy = "on-request"
sandbox_mode    = "workspace-write"

[profiles.readonly_quiet]
approval_policy = "never"
sandbox_mode    = "read-only"
```

### Test the sandbox locally

To see what happens when a command runs under the Codex sandbox, use these Codex CLI commands:

```bash
# macOS
codex sandbox macos [--full-auto] [--log-denials] [COMMAND]...
# Linux
codex sandbox linux [--full-auto] [COMMAND]...
```

The `sandbox` command is also available as `codex debug`, and the platform helpers have aliases (for example `codex sandbox seatbelt` and `codex sandbox landlock`).

## OS-level sandbox

Codex enforces the sandbox differently depending on your OS:

- **macOS** uses Seatbelt policies and runs commands using `sandbox-exec` with a profile (`-p`) that corresponds to the `--sandbox` mode you selected.
- **Linux** uses a combination of `Landlock` and `seccomp` to enforce the sandbox configuration.
- **Windows** uses the Linux sandbox implementation when running in [Windows Subsystem for Linux (WSL)](https://developers.openai.com/codex/windows#windows-subsystem-for-linux). When running natively on Windows, you can enable an [experimental sandbox](https://developers.openai.com/codex/windows#windows-experimental-sandbox) implementation.

If you use the Codex IDE extension on Windows, it supports WSL directly. Set the following in your VS Code settings to keep the agent inside WSL whenever it's available:

```json
{
  "chatgpt.runCodexInWindowsSubsystemForLinux": true
}
```

This ensures the IDE extension inherits Linux sandbox semantics for commands, approvals, and filesystem access even when the host OS is Windows. Learn more in the [Windows setup guide](https://developers.openai.com/codex/windows).

The native Windows sandbox is experimental and has important limitations. For example, it cannot prevent writes in directories where the `Everyone` SID already has write permissions (for example, world-writable folders). See the [Windows setup guide](https://developers.openai.com/codex/windows#windows-experimental-sandbox) for details and mitigations.

When you run Linux in a containerized environment such as Docker, the sandbox may not work if the host or container configuration doesn't support the required `Landlock` and `seccomp` features.

In that case, configure your Docker container to provide the isolation you need, then run `codex` with `--sandbox danger-full-access` (or the `--dangerously-bypass-approvals-and-sandbox` flag) inside the container.

## Version control

Codex works best with a version control workflow:

- Work on a feature branch and keep `git status` clean before delegating. This keeps Codex patches easier to isolate and revert.
- Prefer patch-based workflows (for example, `git diff`/`git apply`) over editing tracked files directly. Commit frequently so you can roll back in small increments.
- Treat Codex suggestions like any other PR: run targeted verification, review diffs, and document decisions in commit messages for auditing.

## Monitoring and telemetry

Codex supports opt-in monitoring via OpenTelemetry (OTEL) to help teams audit usage, investigate issues, and meet compliance requirements without weakening local security defaults. Telemetry is off by default and must be explicitly enabled in your configuration.

### Overview

- Codex turns off OTEL export by default to keep local runs self-contained.
- When enabled, Codex emits structured log events covering conversations, API requests, streamed responses, user prompts (redacted by default), tool approval decisions, and tool results.
- Codex tags exported events with `service.name` (originator), CLI version, and an environment label to separate dev/staging/prod traffic.

### Enable OTEL (opt-in)

Add an `[otel]` block to your Codex configuration (typically `~/.codex/config.toml`), choosing an exporter and whether to log prompt text.

```toml
[otel]
environment = "staging"   # dev | staging | prod
exporter = "none"          # none | otlp-http | otlp-grpc
log_user_prompt = false     # redact prompt text unless policy allows
```

- `exporter = "none"` leaves instrumentation active but doesn't send data anywhere.
- To send events to your own collector, pick one of:

```toml
[otel]
exporter = { otlp-http = {
  endpoint = "https://otel.example.com/v1/logs",
  protocol = "binary",
  headers = { "x-otlp-api-key" = "${OTLP_TOKEN}" }
}}
```

```toml
[otel]
exporter = { otlp-grpc = {
  endpoint = "https://otel.example.com:4317",
  headers = { "x-otlp-meta" = "abc123" }
}}
```

Codex batches events and flushes them on shutdown. Codex exports only telemetry produced by its OTEL module.

### Event categories

Representative event types include:

- `codex.conversation_starts` (model, reasoning settings, sandbox/approval policy)
- `codex.api_request` and `codex.sse_event` (durations, status, token counts)
- `codex.user_prompt` (length; content redacted unless explicitly enabled)
- `codex.tool_decision` (approved/denied, source: configuration vs. user)
- `codex.tool_result` (duration, success, output snippet)

For the full event catalog and configuration reference, see the [Codex configuration documentation on GitHub](https://github.com/openai/codex/blob/main/docs/config.md#otel).

### Security and privacy guidance

- Keep `log_user_prompt = false` unless policy explicitly permits storing prompt contents. Prompts can include source code and sensitive data.
- Route telemetry only to collectors you control; apply retention limits and access controls aligned with your compliance requirements.
- Treat tool arguments and outputs as sensitive. Favor redaction at the collector or SIEM when possible.
- Review local data retention settings (for example, `history.persistence` / `history.max_bytes`) if you don't want Codex to save session transcripts under `CODEX_HOME`. See [Advanced Config](https://developers.openai.com/codex/config-advanced#history-persistence) and [Configuration Reference](https://developers.openai.com/codex/config-reference).
- If you run the CLI with network access turned off, OTEL export can't reach your collector. To export, either allow network access in `workspace-write` mode for the OTEL endpoint or export from Codex cloud with the collector domain on your allow list.
- Review events periodically for approval/sandbox changes and unexpected tool executions.

OTEL is optional and designed to complement, not replace, the sandbox and approval protections described above.

## Managed configuration

Enterprise admins can control local Codex behavior in two ways:

- **Requirements**: admin-enforced constraints that users cannot override.
- **Managed defaults**: starting values applied when Codex launches. Users can still change settings during a session; Codex reapplies managed defaults the next time it starts.

### Admin-enforced requirements (requirements.toml)

Requirements constrain security-sensitive settings (approval policy, sandbox mode, and optionally which MCP servers can be enabled). If a user tries to select a disallowed approval policy or sandbox mode (via `config.toml`, CLI flags, profiles, or in-session UI), Codex rejects it. If an `mcp_servers` allowlist is configured, an MCP server is enabled only when both its name and identity match an allowlisted entry; otherwise it is disabled.

#### Locations

- Linux/macOS (Unix): `/etc/codex/requirements.toml`
- macOS MDM: preference domain `com.openai.codex`, key `requirements_toml_base64`

For backwards compatibility, Codex also interprets legacy `managed_config.toml` fields `approval_policy` and `sandbox_mode` as requirements (allowing only that single value).

#### Example requirements.toml

This example blocks `--ask-for-approval never` and `--sandbox danger-full-access` (including `--yolo`):

```toml
allowed_approval_policies = ["untrusted", "on-request", "on-failure"]
allowed_sandbox_modes = ["read-only", "workspace-write"]
```

To restrict which MCP servers can be enabled, add an `mcp_servers` allowlist. For stdio servers, match on `command`; for streamable HTTP servers, match on `url`:

```toml
[mcp_servers.docs]
identity = { command = "codex-mcp" }

[mcp_servers.remote]
identity = { url = "https://example.com/mcp" }
```

If `mcp_servers` is present but empty, Codex disables all MCP servers.

### Managed defaults (managed_config.toml)

Managed defaults merge on top of a user's local `config.toml` and take precedence over any CLI `--config` overrides, setting the starting values when Codex launches. Users can still change those settings during a session; Codex reapplies managed defaults the next time it starts.

Make sure your managed defaults comply with your requirements; a disallowed value will be rejected.

#### Precedence and layering

Codex assembles the effective configuration in this order (top overrides bottom):

- Managed preferences (macOS MDM; highest precedence)
- `managed_config.toml` (system/managed file)
- `config.toml` (user's base configuration)

CLI `--config key=value` overrides apply to the base, but managed layers override them. This means each run starts from the managed defaults even if you provide local flags.

#### Locations

- Linux/macOS (Unix): `/etc/codex/managed_config.toml`
- Windows/non-Unix: `~/.codex/managed_config.toml`

If the file is missing, Codex skips the managed layer.

#### macOS managed preferences (MDM)

On macOS, admins can push a device profile that provides base64-encoded TOML payloads at:

- Preference domain: `com.openai.codex`
- Keys:
  - `config_toml_base64` (managed defaults)
  - `requirements_toml_base64` (requirements)

Codex parses these "managed preferences" payloads as TOML and applies them with the highest precedence.

### MDM setup workflow

Codex honors standard macOS MDM payloads, so you can distribute settings with tooling like `Jamf Pro`, `Fleet`, or `Kandji`. A lightweight deployment looks like:

1. Build the managed payload TOML and encode it with `base64` (no wrapping).
2. Drop the string into your MDM profile under the `com.openai.codex` domain at `config_toml_base64` (managed defaults) or `requirements_toml_base64` (requirements).
3. Push the profile, then ask users to restart Codex or rerun `codex config show --effective` to confirm the managed values are active.
4. When revoking or changing policy, update the managed payload; the CLI reads the refreshed preference the next time it launches.

Avoid embedding secrets or high-churn dynamic values in the payload. Treat the managed TOML like any other MDM setting under change control.

### Example managed_config.toml

```toml
# Set conservative defaults
approval_policy = "on-request"
sandbox_mode    = "workspace-write"

[sandbox_workspace_write]
network_access = false             # keep network disabled unless explicitly allowed

[otel]
environment = "prod"
exporter = "otlp-http"            # point at your collector
log_user_prompt = false            # keep prompts redacted
# exporter details live under exporter tables; see Monitoring and telemetry above
```

### Recommended guardrails

- Prefer `workspace-write` with approvals for most users; reserve full access for controlled containers.
- Keep `network_access = false` unless your security review allows a collector or domains required by your workflows.
- Use managed configuration to pin OTEL settings (exporter, environment), but keep `log_user_prompt = false` unless your policy explicitly allows storing prompt contents.
- Periodically audit diffs between local `config.toml` and managed policy to catch drift; managed layers should win over local flags and files.


---
