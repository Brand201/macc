# Welcome to Gemini CLI documentation

This documentation provides a comprehensive guide to installing, using, and
developing Gemini CLI, a tool that lets you interact with Gemini models through
a command-line interface.

## Gemini CLI overview

Gemini CLI brings the capabilities of Gemini models to your terminal in an
interactive Read-Eval-Print Loop (REPL) environment. Gemini CLI consists of a
client-side application (`packages/cli`) that communicates with a local server
(`packages/core`), which in turn manages requests to the Gemini API and its AI
models. Gemini CLI also contains a variety of tools for tasks such as performing
file system operations, running shells, and web fetching, which are managed by
`packages/core`.

## Navigating the documentation

This documentation is organized into the following sections:

### Overview

- **[Architecture overview](#gemini-cli-architecture-overview):** Understand the high-level
  design of Gemini CLI, including its components and how they interact.
- **[Contribution guide](https://github.com/google-gemini/gemini-cli/blob/main/CONTRIBUTING.md):** Information for contributors and
  developers, including setup, building, testing, and coding conventions.

### Get started

- **[Gemini CLI quickstart](#get-started-with-gemini-cli):** Let's get started with
  Gemini CLI.
- **[Gemini 3 Pro on Gemini CLI](#gemini-3-pro-and-gemini-3-flash-on-gemini-cli):** Learn how to
  enable and use Gemini 3.
- **[Authentication](#gemini-cli-authentication-setup):** Authenticate to Gemini
  CLI.
- **[Configuration](#gemini-cli-configuration):** Learn how to configure
  the CLI.
- **[Installation](#gemini-cli-installation--execution--and-deployment):** Install and run Gemini CLI.
- **[Examples](#gemini-cli-examples):** Example usage of Gemini CLI.

### CLI

- **[Introduction: Gemini CLI](#gemini-cli):** Overview of the command-line
  interface.
- **[Commands](#cli-commands):** Description of available CLI commands.
- **[Checkpointing](#checkpointing):** Documentation for the
  checkpointing feature.
- **[Custom commands](#custom-commands):** Create your own commands and
  shortcuts for frequently used prompts.
- **[Enterprise](#gemini-cli-for-the-enterprise):** Gemini CLI for enterprise.
- **[Headless mode](#headless-mode):** Use Gemini CLI programmatically for
  scripting and automation.
- **[Keyboard shortcuts](#gemini-cli-keyboard-shortcuts):** A reference for all
  keyboard shortcuts to improve your workflow.
- **[Model selection](#gemini-cli-model-selection):** Select the model used to process your
  commands with `/model`.
- **[Sandbox](#sandboxing-in-the-gemini-cli):** Isolate tool execution in a secure,
  containerized environment.
- **[Agent Skills](#agent-skills):** (Experimental) Extend the CLI with
  specialized expertise and procedural workflows.
- **[Settings](#gemini-cli-settings):** Configure various aspects of the CLI's
  behavior and appearance with `/settings`.
- **[Telemetry](#observability-with-opentelemetry):** Overview of telemetry in the CLI.
- **[Themes](#themes):** Themes for Gemini CLI.
- **[Token caching](#token-caching-and-cost-optimization):** Token caching and optimization.
- **[Trusted Folders](#trusted-folders):** An overview of the Trusted
  Folders security feature.
- **[Tutorials](#tutorials):** Tutorials for Gemini CLI.
- **[Uninstall](#uninstalling-the-cli):** Methods for uninstalling the Gemini CLI.

### Core

- **[Introduction: Gemini CLI core](#system-prompt-override):** Information about Gemini
  CLI core.
- **[Memport](#gemini-cli-core):** Using the Memory Import Processor.
- **[Tools API](#gemini-cli-core:-tools-api):** Information on how the core manages and
  exposes tools.
- **[System Prompt Override](#memory-import-processor):** Replace built-in system
  instructions using `GEMINI_SYSTEM_MD`.

- **[Policy Engine](#policy-engine):** Use the Policy Engine for
  fine-grained control over tool execution.

### Tools

- **[Introduction: Gemini CLI tools](#gemini-cli-tools):** Information about
  Gemini CLI's tools.
- **[File system tools](#gemini-cli-file-system-tools):** Documentation for the
  `read_file` and `write_file` tools.
- **[Shell tool](#shell-tool):** Documentation for the `run_shell_command`
  tool.
- **[Web fetch tool](#web-fetch-tool):** Documentation for the `web_fetch`
  tool.
- **[Web search tool](#web-search-tool):** Documentation for the
  `google_web_search` tool.
- **[Memory tool](#memory-tool):** Documentation for the `save_memory`
  tool.
- **[Todo tool](#todo-tool):** Documentation for the `write_todos` tool.
- **[MCP servers](#mcp-servers-with-the-gemini-cli):** Using MCP servers with Gemini CLI.

### Extensions

- **[Introduction: Extensions](#gemini-cli-extensions):** How to extend the CLI
  with new functionality.
- **[Writing extensions](#getting-started-with-gemini-cli-extensions):** Learn how to
  build your own extension.
- **[Extension releasing](#extensions-reference):** How to release Gemini
  CLI extensions.

### Hooks

- **[Hooks](#gemini-cli-hooks):** Intercept and customize Gemini CLI behavior at
  key lifecycle points.
- **[Writing Hooks](#writing-hooks-for-gemini-cli):** Learn how to create your first
  hook with a comprehensive example.
- **[Best Practices](#hooks-reference):** Security, performance, and
  debugging guidelines for hooks.

### IDE integration

- **[Introduction to IDE integration](#ide-integration):** Connect the
  CLI to your editor.
- **[IDE companion extension spec](#gemini-cli-companion-plugin--interface-specification):**
  Spec for building IDE companion extensions.

### Development

- **[NPM](#package-overview):** Details on how the project's packages are structured.
- **[Releases](#gemini-cli-releases):** Information on the project's releases and
  deployment cadence.
- **[Changelog](#changelogs):** Highlights and notable changes to
  Gemini CLI.
- **[Integration tests](#integration-tests):** Information about the
  integration testing framework used in this project.
- **[Issue and PR automation](#automation-and-triage-processes):** A detailed
  overview of the automated processes we use to manage and triage issues and
  pull requests.

We hope this documentation helps you make the most of Gemini CLI!


---


# Gemini CLI Architecture Overview

This document provides a high-level overview of the Gemini CLI's architecture.

## Core components

The Gemini CLI is primarily composed of two main packages, along with a suite of
tools that can be used by the system in the course of handling command-line
input:

1.  **CLI package (`packages/cli`):**
    - **Purpose:** This contains the user-facing portion of the Gemini CLI, such
      as handling the initial user input, presenting the final output, and
      managing the overall user experience.
    - **Key functions contained in the package:**
      - [Input processing](/docs/cli/commands)
      - History management
      - Display rendering
      - [Theme and UI customization](/docs/cli/themes)
      - [CLI configuration settings](/docs/get-started/configuration)

2.  **Core package (`packages/core`):**
    - **Purpose:** This acts as the backend for the Gemini CLI. It receives
      requests sent from `packages/cli`, orchestrates interactions with the
      Gemini API, and manages the execution of available tools.
    - **Key functions contained in the package:**
      - API client for communicating with the Google Gemini API
      - Prompt construction and management
      - Tool registration and execution logic
      - State management for conversations or sessions
      - Server-side configuration

3.  **Tools (`packages/core/src/tools/`):**
    - **Purpose:** These are individual modules that extend the capabilities of
      the Gemini model, allowing it to interact with the local environment
      (e.g., file system, shell commands, web fetching).
    - **Interaction:** `packages/core` invokes these tools based on requests
      from the Gemini model.

## Interaction flow

A typical interaction with the Gemini CLI follows this flow:

1.  **User input:** The user types a prompt or command into the terminal, which
    is managed by `packages/cli`.
2.  **Request to core:** `packages/cli` sends the user's input to
    `packages/core`.
3.  **Request processed:** The core package:
    - Constructs an appropriate prompt for the Gemini API, possibly including
      conversation history and available tool definitions.
    - Sends the prompt to the Gemini API.
4.  **Gemini API response:** The Gemini API processes the prompt and returns a
    response. This response might be a direct answer or a request to use one of
    the available tools.
5.  **Tool execution (if applicable):**
    - When the Gemini API requests a tool, the core package prepares to execute
      it.
    - If the requested tool can modify the file system or execute shell
      commands, the user is first given details of the tool and its arguments,
      and the user must approve the execution.
    - Read-only operations, such as reading files, might not require explicit
      user confirmation to proceed.
    - Once confirmed, or if confirmation is not required, the core package
      executes the relevant action within the relevant tool, and the result is
      sent back to the Gemini API by the core package.
    - The Gemini API processes the tool result and generates a final response.
6.  **Response to CLI:** The core package sends the final response back to the
    CLI package.
7.  **Display to user:** The CLI package formats and displays the response to
    the user in the terminal.

## Key design principles

- **Modularity:** Separating the CLI (frontend) from the Core (backend) allows
  for independent development and potential future extensions (e.g., different
  frontends for the same backend).
- **Extensibility:** The tool system is designed to be extensible, allowing new
  capabilities to be added.
- **User experience:** The CLI focuses on providing a rich and interactive
  terminal experience.
  

---


# How to contribute

We would love to accept your patches and contributions to this project. This
document includes:

- **[Before you begin](#before-you-begin):** Essential steps to take before
  becoming a Gemini CLI contributor.
- **[Code contribution process](#code-contribution-process):** How to contribute
  code to Gemini CLI.
- **[Development setup and workflow](#development-setup-and-workflow):** How to
  set up your development environment and workflow.
- **[Documentation contribution process](#documentation-contribution-process):**
  How to contribute documentation to Gemini CLI.

We're looking forward to seeing your contributions!

## Before you begin

### Sign our Contributor License Agreement

Contributions to this project must be accompanied by a
[Contributor License Agreement](https://cla.developers.google.com/about) (CLA).
You (or your employer) retain the copyright to your contribution; this simply
gives us permission to use and redistribute your contributions as part of the
project.

If you or your current employer have already signed the Google CLA (even if it
was for a different project), you probably don't need to do it again.

Visit <https://cla.developers.google.com/> to see your current agreements or to
sign a new one.

### Review our Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/).

## Code contribution process

### Get started

The process for contributing code is as follows:

1.  **Find an issue** that you want to work on. If an issue is tagged as
    `🔒Maintainers only`, this means it is reserved for project maintainers. We
    will not accept pull requests related to these issues. In the near future,
    we will explicitly mark issues looking for contributions using the
    `help  wanted` label. If you believe an issue is a good candidate for
    community contribution, please leave a comment on the issue. A maintainer
    will review it and apply the `help-wanted` label if appropriate. Only
    maintainers should attempt to add the `help-wanted` label to an issue.
2.  **Fork the repository** and create a new branch.
3.  **Make your changes** in the `packages/` directory.
4.  **Ensure all checks pass** by running `npm run preflight`.
5.  **Open a pull request** with your changes.

### Code reviews

All submissions, including submissions by project members, require review. We
use [GitHub pull requests](https://docs.github.com/articles/about-pull-requests)
for this purpose.

If your pull request involves changes to `packages/cli` (the frontend), we
recommend running our automated frontend review tool. **Note: This tool is
currently experimental.** It helps detect common React anti-patterns, testing
issues, and other frontend-specific best practices that are easy to miss.

To run the review tool, enter the following command from within Gemini CLI:

```text
/review-frontend <PR_NUMBER>
```

Replace `<PR_NUMBER>` with your pull request number. Authors are encouraged to
run this on their own PRs for self-review, and reviewers should use it to
augment their manual review process.

### Self assigning issues

To assign an issue to yourself, simply add a comment with the text `/assign`.
The comment must contain only that text and nothing else. This command will
assign the issue to you, provided it is not already assigned.

Please note that you can have a maximum of 3 issues assigned to you at any given
time.

### Pull request guidelines

To help us review and merge your PRs quickly, please follow these guidelines.
PRs that do not meet these standards may be closed.

#### 1. Link to an existing issue

All PRs should be linked to an existing issue in our tracker. This ensures that
every change has been discussed and is aligned with the project's goals before
any code is written.

- **For bug fixes:** The PR should be linked to the bug report issue.
- **For features:** The PR should be linked to the feature request or proposal
  issue that has been approved by a maintainer.

If an issue for your change doesn't exist, we will automatically close your PR
along with a comment reminding you to associate the PR with an issue. The ideal
workflow starts with an issue that has been reviewed and approved by a
maintainer. Please **open the issue first** and wait for feedback before you
start coding.

#### 2. Keep it small and focused

We favor small, atomic PRs that address a single issue or add a single,
self-contained feature.

- **Do:** Create a PR that fixes one specific bug or adds one specific feature.
- **Don't:** Bundle multiple unrelated changes (e.g., a bug fix, a new feature,
  and a refactor) into a single PR.

Large changes should be broken down into a series of smaller, logical PRs that
can be reviewed and merged independently.

#### 3. Use draft PRs for work in progress

If you'd like to get early feedback on your work, please use GitHub's **Draft
Pull Request** feature. This signals to the maintainers that the PR is not yet
ready for a formal review but is open for discussion and initial feedback.

#### 4. Ensure all checks pass

Before submitting your PR, ensure that all automated checks are passing by
running `npm run preflight`. This command runs all tests, linting, and other
style checks.

#### 5. Update documentation

If your PR introduces a user-facing change (e.g., a new command, a modified
flag, or a change in behavior), you must also update the relevant documentation
in the `/docs` directory.

See more about writing documentation:
[Documentation contribution process](#documentation-contribution-process).

#### 6. Write clear commit messages and a good PR description

Your PR should have a clear, descriptive title and a detailed description of the
changes. Follow the [Conventional Commits](https://www.conventionalcommits.org/)
standard for your commit messages.

- **Good PR title:** `feat(cli): Add --json flag to 'config get' command`
- **Bad PR title:** `Made some changes`

In the PR description, explain the "why" behind your changes and link to the
relevant issue (e.g., `Fixes #123`).

### Forking

If you are forking the repository you will be able to run the Build, Test and
Integration test workflows. However in order to make the integration tests run
you'll need to add a
[GitHub Repository Secret](https://docs.github.com/en/actions/security-for-github-actions/security-guides/using-secrets-in-github-actions#creating-secrets-for-a-repository)
with a value of `GEMINI_API_KEY` and set that to a valid API key that you have
available. Your key and secret are private to your repo; no one without access
can see your key and you cannot see any secrets related to this repo.

Additionally you will need to click on the `Actions` tab and enable workflows
for your repository, you'll find it's the large blue button in the center of the
screen.

### Development setup and workflow

This section guides contributors on how to build, modify, and understand the
development setup of this project.

### Setting up the development environment

**Prerequisites:**

1.  **Node.js**:
    - **Development:** Please use Node.js `~20.19.0`. This specific version is
      required due to an upstream development dependency issue. You can use a
      tool like [nvm](https://github.com/nvm-sh/nvm) to manage Node.js versions.
    - **Production:** For running the CLI in a production environment, any
      version of Node.js `>=20` is acceptable.
2.  **Git**

### Build process

To clone the repository:

```bash
git clone https://github.com/google-gemini/gemini-cli.git # Or your fork's URL
cd gemini-cli
```

To install dependencies defined in `package.json` as well as root dependencies:

```bash
npm install
```

To build the entire project (all packages):

```bash
npm run build
```

This command typically compiles TypeScript to JavaScript, bundles assets, and
prepares the packages for execution. Refer to `scripts/build.js` and
`package.json` scripts for more details on what happens during the build.

### Enabling sandboxing

[Sandboxing](#sandboxing) is highly recommended and requires, at a minimum,
setting `GEMINI_SANDBOX=true` in your `~/.env` and ensuring a sandboxing
provider (e.g. `macOS Seatbelt`, `docker`, or `podman`) is available. See
[Sandboxing](#sandboxing) for details.

To build both the `gemini` CLI utility and the sandbox container, run
`build:all` from the root directory:

```bash
npm run build:all
```

To skip building the sandbox container, you can use `npm run build` instead.

### Running the CLI

To start the Gemini CLI from the source code (after building), run the following
command from the root directory:

```bash
npm start
```

If you'd like to run the source build outside of the gemini-cli folder, you can
utilize `npm link path/to/gemini-cli/packages/cli` (see:
[docs](https://docs.npmjs.com/cli/v9/commands/npm-link)) or
`alias gemini="node path/to/gemini-cli/packages/cli"` to run with `gemini`

### Running tests

This project contains two types of tests: unit tests and integration tests.

#### Unit tests

To execute the unit test suite for the project:

```bash
npm run test
```

This will run tests located in the `packages/core` and `packages/cli`
directories. Ensure tests pass before submitting any changes. For a more
comprehensive check, it is recommended to run `npm run preflight`.

#### Integration tests

The integration tests are designed to validate the end-to-end functionality of
the Gemini CLI. They are not run as part of the default `npm run test` command.

To run the integration tests, use the following command:

```bash
npm run test:e2e
```

For more detailed information on the integration testing framework, please see
the [Integration Tests documentation](/docs/integration-tests.md).

### Linting and preflight checks

To ensure code quality and formatting consistency, run the preflight check:

```bash
npm run preflight
```

This command will run ESLint, Prettier, all tests, and other checks as defined
in the project's `package.json`.

_ProTip_

after cloning create a git precommit hook file to ensure your commits are always
clean.

```bash
echo "
# Run npm build and check for errors
if ! npm run preflight; then
  echo "npm build failed. Commit aborted."
  exit 1
fi
" > .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit
```

#### Formatting

To separately format the code in this project by running the following command
from the root directory:

```bash
npm run format
```

This command uses Prettier to format the code according to the project's style
guidelines.

#### Linting

To separately lint the code in this project, run the following command from the
root directory:

```bash
npm run lint
```

### Coding conventions

- Please adhere to the coding style, patterns, and conventions used throughout
  the existing codebase.
- Consult
  [GEMINI.md](https://github.com/google-gemini/gemini-cli/blob/main/GEMINI.md)
  (typically found in the project root) for specific instructions related to
  AI-assisted development, including conventions for React, comments, and Git
  usage.
- **Imports:** Pay special attention to import paths. The project uses ESLint to
  enforce restrictions on relative imports between packages.

### Project structure

- `packages/`: Contains the individual sub-packages of the project.
  - `a2a-server`: A2A server implementation for the Gemini CLI. (Experimental)
  - `cli/`: The command-line interface.
  - `core/`: The core backend logic for the Gemini CLI.
  - `test-utils` Utilities for creating and cleaning temporary file systems for
    testing.
  - `vscode-ide-companion/`: The Gemini CLI Companion extension pairs with
    Gemini CLI.
- `docs/`: Contains all project documentation.
- `scripts/`: Utility scripts for building, testing, and development tasks.

For more detailed architecture, see `docs/architecture.md`.

### Debugging

#### VS Code

0.  Run the CLI to interactively debug in VS Code with `F5`
1.  Start the CLI in debug mode from the root directory:
    ```bash
    npm run debug
    ```
    This command runs `node --inspect-brk dist/gemini.js` within the
    `packages/cli` directory, pausing execution until a debugger attaches. You
    can then open `chrome://inspect` in your Chrome browser to connect to the
    debugger.
2.  In VS Code, use the "Attach" launch configuration (found in
    `.vscode/launch.json`).

Alternatively, you can use the "Launch Program" configuration in VS Code if you
prefer to launch the currently open file directly, but 'F5' is generally
recommended.

To hit a breakpoint inside the sandbox container run:

```bash
DEBUG=1 gemini
```

**Note:** If you have `DEBUG=true` in a project's `.env` file, it won't affect
gemini-cli due to automatic exclusion. Use `.gemini/.env` files for gemini-cli
specific debug settings.

### React DevTools

To debug the CLI's React-based UI, you can use React DevTools. Ink, the library
used for the CLI's interface, is compatible with React DevTools version 4.x.

1.  **Start the Gemini CLI in development mode:**

    ```bash
    DEV=true npm start
    ```

2.  **Install and run React DevTools version 4.28.5 (or the latest compatible
    4.x version):**

    You can either install it globally:

    ```bash
    npm install -g react-devtools@4.28.5
    react-devtools
    ```

    Or run it directly using npx:

    ```bash
    npx react-devtools@4.28.5
    ```

    Your running CLI application should then connect to React DevTools.
    ![](/docs/assets/connected_devtools.png)

### Sandboxing

#### macOS Seatbelt

On macOS, `gemini` uses Seatbelt (`sandbox-exec`) under a `permissive-open`
profile (see `packages/cli/src/utils/sandbox-macos-permissive-open.sb`) that
restricts writes to the project folder but otherwise allows all other operations
and outbound network traffic ("open") by default. You can switch to a
`restrictive-closed` profile (see
`packages/cli/src/utils/sandbox-macos-restrictive-closed.sb`) that declines all
operations and outbound network traffic ("closed") by default by setting
`SEATBELT_PROFILE=restrictive-closed` in your environment or `.env` file.
Available built-in profiles are `{permissive,restrictive}-{open,closed,proxied}`
(see below for proxied networking). You can also switch to a custom profile
`SEATBELT_PROFILE=<profile>` if you also create a file
`.gemini/sandbox-macos-<profile>.sb` under your project settings directory
`.gemini`.

#### Container-based sandboxing (all platforms)

For stronger container-based sandboxing on macOS or other platforms, you can set
`GEMINI_SANDBOX=true|docker|podman|<command>` in your environment or `.env`
file. The specified command (or if `true` then either `docker` or `podman`) must
be installed on the host machine. Once enabled, `npm run build:all` will build a
minimal container ("sandbox") image and `npm start` will launch inside a fresh
instance of that container. The first build can take 20-30s (mostly due to
downloading of the base image) but after that both build and start overhead
should be minimal. Default builds (`npm run build`) will not rebuild the
sandbox.

Container-based sandboxing mounts the project directory (and system temp
directory) with read-write access and is started/stopped/removed automatically
as you start/stop Gemini CLI. Files created within the sandbox should be
automatically mapped to your user/group on host machine. You can easily specify
additional mounts, ports, or environment variables by setting
`SANDBOX_{MOUNTS,PORTS,ENV}` as needed. You can also fully customize the sandbox
for your projects by creating the files `.gemini/sandbox.Dockerfile` and/or
`.gemini/sandbox.bashrc` under your project settings directory (`.gemini`) and
running `gemini` with `BUILD_SANDBOX=1` to trigger building of your custom
sandbox.

#### Proxied networking

All sandboxing methods, including macOS Seatbelt using `*-proxied` profiles,
support restricting outbound network traffic through a custom proxy server that
can be specified as `GEMINI_SANDBOX_PROXY_COMMAND=<command>`, where `<command>`
must start a proxy server that listens on `:::8877` for relevant requests. See
`docs/examples/proxy-script.md` for a minimal proxy that only allows `HTTPS`
connections to `example.com:443` (e.g. `curl https://example.com`) and declines
all other requests. The proxy is started and stopped automatically alongside the
sandbox.

### Manual publish

We publish an artifact for each commit to our internal registry. But if you need
to manually cut a local build, then run the following commands:

```
npm run clean
npm install
npm run auth
npm run prerelease:dev
npm publish --workspaces
```

## Documentation contribution process

Our documentation must be kept up-to-date with our code contributions. We want
our documentation to be clear, concise, and helpful to our users. We value:

- **Clarity:** Use simple and direct language. Avoid jargon where possible.
- **Accuracy:** Ensure all information is correct and up-to-date.
- **Completeness:** Cover all aspects of a feature or topic.
- **Examples:** Provide practical examples to help users understand how to use
  Gemini CLI.

### Getting started

The process for contributing to the documentation is similar to contributing
code.

1. **Fork the repository** and create a new branch.
2. **Make your changes** in the `/docs` directory.
3. **Preview your changes locally** in Markdown rendering.
4. **Lint and format your changes.** Our preflight check includes linting and
   formatting for documentation files.
   ```bash
   npm run preflight
   ```
5. **Open a pull request** with your changes.

### Documentation structure

Our documentation is organized using [sidebar.json](/docs/sidebar.json) as the
table of contents. When adding new documentation:

1. Create your markdown file **in the appropriate directory** under `/docs`.
2. Add an entry to `sidebar.json` in the relevant section.
3. Ensure all internal links use relative paths and point to existing files.

### Style guide

We follow the
[Google Developer Documentation Style Guide](https://developers.google.com/style).
Please refer to it for guidance on writing style, tone, and formatting.

#### Key style points

- Use sentence case for headings.
- Write in second person ("you") when addressing the reader.
- Use present tense.
- Keep paragraphs short and focused.
- Use code blocks with appropriate language tags for syntax highlighting.
- Include practical examples whenever possible.

### Linting and formatting

We use `prettier` to enforce a consistent style across our documentation. The
`npm run preflight` command will check for any linting issues.

You can also run the linter and formatter separately:

- `npm run lint` - Check for linting issues
- `npm run format` - Auto-format markdown files
- `npm run lint:fix` - Auto-fix linting issues where possible

Please make sure your contributions are free of linting errors before submitting
a pull request.

### Before you submit

Before submitting your documentation pull request, please:

1. Run `npm run preflight` to ensure all checks pass.
2. Review your changes for clarity and accuracy.
3. Check that all links work correctly.
4. Ensure any code examples are tested and functional.
5. Sign the
   [Contributor License Agreement (CLA)](https://cla.developers.google.com/) if
   you haven't already.

### Need help?

If you have questions about contributing documentation:

- Check our [FAQ](/docs/faq.md).
- Review existing documentation for examples.
- Open [an issue](https://github.com/google-gemini/gemini-cli/issues) to discuss
  your proposed changes.
- Reach out to the maintainers.

We appreciate your contributions to making Gemini CLI documentation better!


---


# Get started with Gemini CLI

Welcome to Gemini CLI! This guide will help you install, configure, and start
using the Gemini CLI to enhance your workflow right from your terminal.

## Quickstart: Install, authenticate, configure, and use Gemini CLI

Gemini CLI brings the power of advanced language models directly to your command
line interface. As an AI-powered assistant, Gemini CLI can help you with a
variety of tasks, from understanding and generating code to reviewing and
editing documents.

## Install

The standard method to install and run Gemini CLI uses `npm`:

```bash
npm install -g @google/gemini-cli
```

Once Gemini CLI is installed, run Gemini CLI from your command line:

```bash
gemini
```

For more installation options, see [Gemini CLI Installation](/docs/get-started/installation).

## Authenticate

To begin using Gemini CLI, you must authenticate with a Google service. In most
cases, you can log in with your existing Google account:

1. Run Gemini CLI after installation:

   ```bash
   gemini
   ```

2. When asked "How would you like to authenticate for this project?" select **1.
   Login with Google**.

3. Select your Google account.

4. Click on **Sign in**.

Certain account types may require you to configure a Google Cloud project. For
more information, including other authentication methods, see
[Gemini CLI Authentication Setup](/docs/get-started/authentication).

## Configure

Gemini CLI offers several ways to configure its behavior, including environment
variables, command-line arguments, and settings files.

To explore your configuration options, see
[Gemini CLI Configuration](/docs/get-started/configuration).

## Use

Once installed and authenticated, you can start using Gemini CLI by issuing
commands and prompts in your terminal. Ask it to generate code, explain files,
and more.

To explore the power of Gemini CLI, see [Gemini CLI examples](/docs/get-started/examples).

## What's next?

- Find out more about [Gemini CLI's tools](/docs/tools).
- Review [Gemini CLI's commands](/docs/cli/commands).
- Learn how to [get started with Gemini 3](/docs/get-started/gemini-3).


---


# Gemini 3 Pro and Gemini 3 Flash on Gemini CLI

Gemini 3 Pro and Gemini 3 Flash are available on Gemini CLI for all users!

## How to get started with Gemini 3 on Gemini CLI

Get started by upgrading Gemini CLI to the latest version:

```bash
npm install -g @google/gemini-cli@latest
```

After you’ve confirmed your version is 0.21.1 or later:

1. Use the `/settings` command in Gemini CLI.
2. Toggle **Preview Features** to `true`.
3. Run `/model` and select **Auto (Gemini 3)**.

For more information, see [Gemini CLI model selection](/docs/cli/model).

### Usage limits and fallback

Gemini CLI will tell you when you reach your Gemini 3 Pro daily usage limit.
When you encounter that limit, you’ll be given the option to switch to Gemini
2.5 Pro, upgrade for higher limits, or stop. You’ll also be told when your usage
limit resets and Gemini 3 Pro can be used again.

Similarly, when you reach your daily usage limit for Gemini 2.5 Pro, you’ll see
a message prompting fallback to Gemini 2.5 Flash.

### Capacity errors

There may be times when the Gemini 3 Pro model is overloaded. When that happens,
Gemini CLI will ask you to decide whether you want to keep trying Gemini 3 Pro
or fallback to Gemini 2.5 Pro.

> **Note:** The **Keep trying** option uses exponential backoff, in which Gemini
> CLI waits longer between each retry, when the system is busy. If the retry
> doesn't happen immediately, please wait a few minutes for the request to
> process.

### Model selection and routing types

When using Gemini CLI, you may want to control how your requests are routed
between models. By default, Gemini CLI uses **Auto** routing.

When using Gemini 3 Pro, you may want to use Auto routing or Pro routing to
manage your usage limits:

- **Auto routing:** Auto routing first determines whether a prompt involves a
  complex or simple operation. For simple prompts, it will automatically use
  Gemini 2.5 Flash. For complex prompts, if Gemini 3 Pro is enabled, it will use
  Gemini 3 Pro; otherwise, it will use Gemini 2.5 Pro.
- **Pro routing:** If you want to ensure your task is processed by the most
  capable model, use `/model` and select **Pro**. Gemini CLI will prioritize the
  most capable model available, including Gemini 3 Pro if it has been enabled.

To learn more about selecting a model and routing, refer to
[Gemini CLI Model Selection](/docs/cli/model).

## How to enable Gemini 3 with Gemini CLI on Gemini Code Assist

If you're using Gemini Code Assist Standard or Gemini Code Assist Enterprise,
enabling Gemini 3 Pro on Gemini CLI requires configuring your release channels.
Using Gemini 3 Pro will require two steps: administrative enablement and user
enablement.

To learn more about these settings, refer to
[Configure Gemini Code Assist release channels](https://developers.google.com/gemini-code-assist/docs/configure-release-channels).

### Administrator instructions

An administrator with **Google Cloud Settings Admin** permissions must follow
these directions:

- Navigate to the Google Cloud Project you're using with Gemini CLI for Code
  Assist.
- Go to **Admin for Gemini** > **Settings**.
- Under **Release channels for Gemini Code Assist in local IDEs** select
  **Preview**.
- Click **Save changes**.

### User instructions

Wait for two to three minutes after your administrator has enabled **Preview**,
then:

- Open Gemini CLI.
- Use the `/settings` command.
- Set **Preview Features** to `true`.

Restart Gemini CLI and you should have access to Gemini 3.

## Need help?

If you need help, we recommend searching for an existing
[GitHub issue](https://github.com/google-gemini/gemini-cli/issues). If you
cannot find a GitHub issue that matches your concern, you can
[create a new issue](https://github.com/google-gemini/gemini-cli/issues/new/choose).
For comments and feedback, consider opening a
[GitHub discussion](https://github.com/google-gemini/gemini-cli/discussions).


---


# Gemini CLI authentication setup

To use Gemini CLI, you'll need to authenticate with Google. This guide helps you
quickly find the best way to sign in based on your account type and how you're
using the CLI.

For most users, we recommend starting Gemini CLI and logging in with your
personal Google account.

## Choose your authentication method <a id="auth-methods"></a>

Select the authentication method that matches your situation in the table below:

| User Type / Scenario                                                   | Recommended Authentication Method                                | Google Cloud Project Required                               |
| :--------------------------------------------------------------------- | :--------------------------------------------------------------- | :---------------------------------------------------------- |
| Individual Google accounts                                             | [Login with Google](#login-google)                               | No, with exceptions                                         |
| Organization users with a company, school, or Google Workspace account | [Login with Google](#login-google)                               | [Yes](#set-gcp)                                             |
| AI Studio user with a Gemini API key                                   | [Use Gemini API Key](#gemini-api)                                | No                                                          |
| Google Cloud Vertex AI user                                            | [Vertex AI](#vertex-ai)                                          | [Yes](#set-gcp)                                             |
| [Headless mode](#headless)                                             | [Use Gemini API Key](#gemini-api) or<br> [Vertex AI](#vertex-ai) | No (for Gemini API Key)<br> [Yes](#set-gcp) (for Vertex AI) |

### What is my Google account type?

- **Individual Google accounts:** Includes all
  [free tier accounts](/docs/quota-and-pricing#free-usage) such as Gemini Code
  Assist for individuals, as well as paid subscriptions for
  [Google AI Pro and Ultra](https://gemini.google/subscriptions/).

- **Organization accounts:** Accounts using paid licenses through an
  organization such as a company, school, or
  [Google Workspace](https://workspace.google.com/). Includes
  [Google AI Ultra for Business](https://support.google.com/a/answer/16345165)
  subscriptions.

## (Recommended) Login with Google <a id="login-google"></a>

If you run Gemini CLI on your local machine, the simplest authentication method
is logging in with your Google account. This method requires a web browser on a
machine that can communicate with the terminal running Gemini CLI (e.g., your
local machine).

> **Important:** If you are a **Google AI Pro** or **Google AI Ultra**
> subscriber, use the Google account associated with your subscription.

To authenticate and use Gemini CLI:

1. Start the CLI:

   ```bash
   gemini
   ```

2. Select **Login with Google**. Gemini CLI opens a login prompt using your web
   browser. Follow the on-screen instructions. Your credentials will be cached
   locally for future sessions.

### Do I need to set my Google Cloud project?

Most individual Google accounts (free and paid) don't require a Google Cloud
project for authentication. However, you'll need to set a Google Cloud project
when you meet at least one of the following conditions:

- You are using a company, school, or Google Workspace account.
- You are using a Gemini Code Assist license from the Google Developer Program.
- You are using a license from a Gemini Code Assist subscription.

For instructions, see [Set your Google Cloud Project](#set-gcp).

## Use Gemini API key <a id="gemini-api"></a>

If you don't want to authenticate using your Google account, you can use an API
key from Google AI Studio.

To authenticate and use Gemini CLI with a Gemini API key:

1. Obtain your API key from
   [Google AI Studio](https://aistudio.google.com/app/apikey).

2. Set the `GEMINI_API_KEY` environment variable to your key. For example:

   ```bash
   # Replace YOUR_GEMINI_API_KEY with the key from AI Studio
   export GEMINI_API_KEY="YOUR_GEMINI_API_KEY"
   ```

   To make this setting persistent, see
   [Persisting Environment Variables](#persisting-vars).

3. Start the CLI:

   ```bash
   gemini
   ```

4. Select **Use Gemini API key**.

> **Warning:** Treat API keys, especially for services like Gemini, as sensitive
> credentials. Protect them to prevent unauthorized access and potential misuse
> of the service under your account.

## Use Vertex AI <a id="vertex-ai"></a>

To use Gemini CLI with Google Cloud's Vertex AI platform, choose from the
following authentication options:

- A. Application Default Credentials (ADC) using `gcloud`.
- B. Service account JSON key.
- C. Google Cloud API key.

Regardless of your authentication method for Vertex AI, you'll need to set
`GOOGLE_CLOUD_PROJECT` to your Google Cloud project ID with the Vertex AI API
enabled, and `GOOGLE_CLOUD_LOCATION` to the location of your Vertex AI resources
or the location where you want to run your jobs.

For example:

```bash
# Replace with your project ID and desired location (e.g., us-central1)
export GOOGLE_CLOUD_PROJECT="YOUR_PROJECT_ID"
export GOOGLE_CLOUD_LOCATION="YOUR_PROJECT_LOCATION"
```

To make any Vertex AI environment variable settings persistent, see
[Persisting Environment Variables](#persisting-vars).

#### A. Vertex AI - application default credentials (ADC) using `gcloud`

Consider this authentication method if you have Google Cloud CLI installed.

> **Note:** If you have previously set `GOOGLE_API_KEY` or `GEMINI_API_KEY`, you
> must unset them to use ADC:
>
> ```bash
> unset GOOGLE_API_KEY GEMINI_API_KEY
> ```

1. Verify you have a Google Cloud project and Vertex AI API is enabled.

2. Log in to Google Cloud:

   ```bash
   gcloud auth application-default login
   ```

3. [Configure your Google Cloud Project](#set-gcp).

4. Start the CLI:

   ```bash
   gemini
   ```

5. Select **Vertex AI**.

#### B. Vertex AI - service account JSON key

Consider this method of authentication in non-interactive environments, CI/CD
pipelines, or if your organization restricts user-based ADC or API key creation.

> **Note:** If you have previously set `GOOGLE_API_KEY` or `GEMINI_API_KEY`, you
> must unset them:
>
> ```bash
> unset GOOGLE_API_KEY GEMINI_API_KEY
> ```

1.  [Create a service account and key](https://cloud.google.com/iam/docs/keys-create-delete)
    and download the provided JSON file. Assign the "Vertex AI User" role to the
    service account.

2.  Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the JSON
    file's absolute path. For example:

    ```bash
    # Replace /path/to/your/keyfile.json with the actual path
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keyfile.json"
    ```

3.  [Configure your Google Cloud Project](#set-gcp).

4.  Start the CLI:

    ```bash
    gemini
    ```

5.  Select **Vertex AI**.
    > **Warning:** Protect your service account key file as it gives access to
    > your resources.

#### C. Vertex AI - Google Cloud API key

1.  Obtain a Google Cloud API key:
    [Get an API Key](https://cloud.google.com/vertex-ai/generative-ai/docs/start/api-keys?usertype=newuser).

2.  Set the `GOOGLE_API_KEY` environment variable:

    ```bash
    # Replace YOUR_GOOGLE_API_KEY with your Vertex AI API key
    export GOOGLE_API_KEY="YOUR_GOOGLE_API_KEY"
    ```

    > **Note:** If you see errors like
    > `"API keys are not supported by this API..."`, your organization might
    > restrict API key usage for this service. Try the other Vertex AI
    > authentication methods instead.

3.  [Configure your Google Cloud Project](#set-gcp).

4.  Start the CLI:

    ```bash
    gemini
    ```

5.  Select **Vertex AI**.

## Set your Google Cloud project <a id="set-gcp"></a>

> **Important:** Most individual Google accounts (free and paid) don't require a
> Google Cloud project for authentication.

When you sign in using your Google account, you may need to configure a Google
Cloud project for Gemini CLI to use. This applies when you meet at least one of
the following conditions:

- You are using a Company, School, or Google Workspace account.
- You are using a Gemini Code Assist license from the Google Developer Program.
- You are using a license from a Gemini Code Assist subscription.

To configure Gemini CLI to use a Google Cloud project, do the following:

1.  [Find your Google Cloud Project ID](https://support.google.com/googleapi/answer/7014113).

2.  [Enable the Gemini for Cloud API](https://cloud.google.com/gemini/docs/discover/set-up-gemini#enable-api).

3.  [Configure necessary IAM access permissions](https://cloud.google.com/gemini/docs/discover/set-up-gemini#grant-iam).

4.  Configure your environment variables. Set either the `GOOGLE_CLOUD_PROJECT`
    or `GOOGLE_CLOUD_PROJECT_ID` variable to the project ID to use with Gemini
    CLI. Gemini CLI checks for `GOOGLE_CLOUD_PROJECT` first, then falls back to
    `GOOGLE_CLOUD_PROJECT_ID`.

    For example, to set the `GOOGLE_CLOUD_PROJECT_ID` variable:

    ```bash
    # Replace YOUR_PROJECT_ID with your actual Google Cloud project ID
    export GOOGLE_CLOUD_PROJECT="YOUR_PROJECT_ID"
    ```

    To make this setting persistent, see
    [Persisting Environment Variables](#persisting-vars).

## Persisting environment variables <a id="persisting-vars"></a>

To avoid setting environment variables for every terminal session, you can
persist them with the following methods:

1.  **Add your environment variables to your shell configuration file:** Append
    the `export ...` commands to your shell's startup file (e.g., `~/.bashrc`,
    `~/.zshrc`, or `~/.profile`) and reload your shell (e.g.,
    `source ~/.bashrc`).

    ```bash
    # Example for .bashrc
    echo 'export GOOGLE_CLOUD_PROJECT="YOUR_PROJECT_ID"' >> ~/.bashrc
    source ~/.bashrc
    ```

    > **Warning:** Be aware that when you export API keys or service account
    > paths in your shell configuration file, any process launched from that
    > shell can read them.

2.  **Use a `.env` file:** Create a `.gemini/.env` file in your project
    directory or home directory. Gemini CLI automatically loads variables from
    the first `.env` file it finds, searching up from the current directory,
    then in `~/.gemini/.env` or `~/.env`. `.gemini/.env` is recommended.

    Example for user-wide settings:

    ```bash
    mkdir -p ~/.gemini
    cat >> ~/.gemini/.env <<'EOF'
    GOOGLE_CLOUD_PROJECT="your-project-id"
    # Add other variables like GEMINI_API_KEY as needed
    EOF
    ```

Variables are loaded from the first file found, not merged.

## Running in Google Cloud environments <a id="cloud-env"></a>

When running Gemini CLI within certain Google Cloud environments, authentication
is automatic.

In a Google Cloud Shell environment, Gemini CLI typically authenticates
automatically using your Cloud Shell credentials. In Compute Engine
environments, Gemini CLI automatically uses Application Default Credentials
(ADC) from the environment's metadata server.

If automatic authentication fails, use one of the interactive methods described
on this page.

## Running in headless mode <a id="headless"></a>

[Headless mode](/docs/cli/headless) will use your existing authentication method,
if an existing authentication credential is cached.

If you have not already logged in with an authentication credential, you must
configure authentication using environment variables:

- [Use Gemini API Key](#gemini-api)
- [Vertex AI](#vertex-ai)

## What's next?

Your authentication method affects your quotas, pricing, Terms of Service, and
privacy notices. Review the following pages to learn more:

- [Gemini CLI: Quotas and Pricing](/docs/quota-and-pricing).
- [Gemini CLI: Terms of Service and Privacy Notice](/docs/tos-privacy).


---


# Gemini CLI configuration

> **Note on configuration format, 9/17/25:** The format of the `settings.json`
> file has been updated to a new, more organized structure.
>
> - The new format will be supported in the stable release starting
>   **[09/10/25]**.
> - Automatic migration from the old format to the new format will begin on
>   **[09/17/25]**.
>
> For details on the previous format, please see the
> [v1 Configuration documentation](/docs/get-started/configuration-v1).

Gemini CLI offers several ways to configure its behavior, including environment
variables, command-line arguments, and settings files. This document outlines
the different configuration methods and available settings.

## Configuration layers

Configuration is applied in the following order of precedence (lower numbers are
overridden by higher numbers):

1.  **Default values:** Hardcoded defaults within the application.
2.  **System defaults file:** System-wide default settings that can be
    overridden by other settings files.
3.  **User settings file:** Global settings for the current user.
4.  **Project settings file:** Project-specific settings.
5.  **System settings file:** System-wide settings that override all other
    settings files.
6.  **Environment variables:** System-wide or session-specific variables,
    potentially loaded from `.env` files.
7.  **Command-line arguments:** Values passed when launching the CLI.

## Settings files

Gemini CLI uses JSON settings files for persistent configuration. There are four
locations for these files:

> **Tip:** JSON-aware editors can use autocomplete and validation by pointing to
> the generated schema at `schemas/settings.schema.json` in this repository.
> When working outside the repo, reference the hosted schema at
> `https://raw.githubusercontent.com/google-gemini/gemini-cli/main/schemas/settings.schema.json`.

- **System defaults file:**
  - **Location:** `/etc/gemini-cli/system-defaults.json` (Linux),
    `C:\ProgramData\gemini-cli\system-defaults.json` (Windows) or
    `/Library/Application Support/GeminiCli/system-defaults.json` (macOS). The
    path can be overridden using the `GEMINI_CLI_SYSTEM_DEFAULTS_PATH`
    environment variable.
  - **Scope:** Provides a base layer of system-wide default settings. These
    settings have the lowest precedence and are intended to be overridden by
    user, project, or system override settings.
- **User settings file:**
  - **Location:** `~/.gemini/settings.json` (where `~` is your home directory).
  - **Scope:** Applies to all Gemini CLI sessions for the current user. User
    settings override system defaults.
- **Project settings file:**
  - **Location:** `.gemini/settings.json` within your project's root directory.
  - **Scope:** Applies only when running Gemini CLI from that specific project.
    Project settings override user settings and system defaults.
- **System settings file:**
  - **Location:** `/etc/gemini-cli/settings.json` (Linux),
    `C:\ProgramData\gemini-cli\settings.json` (Windows) or
    `/Library/Application Support/GeminiCli/settings.json` (macOS). The path can
    be overridden using the `GEMINI_CLI_SYSTEM_SETTINGS_PATH` environment
    variable.
  - **Scope:** Applies to all Gemini CLI sessions on the system, for all users.
    System settings act as overrides, taking precedence over all other settings
    files. May be useful for system administrators at enterprises to have
    controls over users' Gemini CLI setups.

**Note on environment variables in settings:** String values within your
`settings.json` and `gemini-extension.json` files can reference environment
variables using either `$VAR_NAME` or `${VAR_NAME}` syntax. These variables will
be automatically resolved when the settings are loaded. For example, if you have
an environment variable `MY_API_TOKEN`, you could use it in `settings.json` like
this: `"apiKey": "$MY_API_TOKEN"`. Additionally, each extension can have its own
`.env` file in its directory, which will be loaded automatically.

> **Note for Enterprise Users:** For guidance on deploying and managing Gemini
> CLI in a corporate environment, please see the
> [Enterprise Configuration](/docs/cli/enterprise) documentation.

### The `.gemini` directory in your project

In addition to a project settings file, a project's `.gemini` directory can
contain other project-specific files related to Gemini CLI's operation, such as:

- [Custom sandbox profiles](#sandboxing) (e.g.,
  `.gemini/sandbox-macos-custom.sb`, `.gemini/sandbox.Dockerfile`).

### Available settings in `settings.json`

Settings are organized into categories. All settings should be placed within
their corresponding top-level category object in your `settings.json` file.

<!-- SETTINGS-AUTOGEN:START -->

#### `general`

- **`general.previewFeatures`** (boolean):
  - **Description:** Enable preview features (e.g., preview models).
  - **Default:** `false`

- **`general.preferredEditor`** (string):
  - **Description:** The preferred editor to open files in.
  - **Default:** `undefined`

- **`general.vimMode`** (boolean):
  - **Description:** Enable Vim keybindings
  - **Default:** `false`

- **`general.enableAutoUpdate`** (boolean):
  - **Description:** Enable automatic updates.
  - **Default:** `true`

- **`general.enableAutoUpdateNotification`** (boolean):
  - **Description:** Enable update notification prompts.
  - **Default:** `true`

- **`general.checkpointing.enabled`** (boolean):
  - **Description:** Enable session checkpointing for recovery
  - **Default:** `false`
  - **Requires restart:** Yes

- **`general.enablePromptCompletion`** (boolean):
  - **Description:** Enable AI-powered prompt completion suggestions while
    typing.
  - **Default:** `false`
  - **Requires restart:** Yes

- **`general.retryFetchErrors`** (boolean):
  - **Description:** Retry on "exception TypeError: fetch failed sending
    request" errors.
  - **Default:** `false`

- **`general.debugKeystrokeLogging`** (boolean):
  - **Description:** Enable debug logging of keystrokes to the console.
  - **Default:** `false`

- **`general.sessionRetention.enabled`** (boolean):
  - **Description:** Enable automatic session cleanup
  - **Default:** `false`

- **`general.sessionRetention.maxAge`** (string):
  - **Description:** Maximum age of sessions to keep (e.g., "30d", "7d", "24h",
    "1w")
  - **Default:** `undefined`

- **`general.sessionRetention.maxCount`** (number):
  - **Description:** Alternative: Maximum number of sessions to keep (most
    recent)
  - **Default:** `undefined`

- **`general.sessionRetention.minRetention`** (string):
  - **Description:** Minimum retention period (safety limit, defaults to "1d")
  - **Default:** `"1d"`

#### `output`

- **`output.format`** (enum):
  - **Description:** The format of the CLI output. Can be `text` or `json`.
  - **Default:** `"text"`
  - **Values:** `"text"`, `"json"`

#### `ui`

- **`ui.theme`** (string):
  - **Description:** The color theme for the UI. See the CLI themes guide for
    available options.
  - **Default:** `undefined`

- **`ui.customThemes`** (object):
  - **Description:** Custom theme definitions.
  - **Default:** `{}`

- **`ui.hideWindowTitle`** (boolean):
  - **Description:** Hide the window title bar
  - **Default:** `false`
  - **Requires restart:** Yes

- **`ui.showStatusInTitle`** (boolean):
  - **Description:** Show Gemini CLI model thoughts in the terminal window title
    during the working phase
  - **Default:** `false`

- **`ui.dynamicWindowTitle`** (boolean):
  - **Description:** Update the terminal window title with current status icons
    (Ready: ◇, Action Required: ✋, Working: ✦)
  - **Default:** `true`

- **`ui.showHomeDirectoryWarning`** (boolean):
  - **Description:** Show a warning when running Gemini CLI in the home
    directory.
  - **Default:** `true`
  - **Requires restart:** Yes

- **`ui.hideTips`** (boolean):
  - **Description:** Hide helpful tips in the UI
  - **Default:** `false`

- **`ui.hideBanner`** (boolean):
  - **Description:** Hide the application banner
  - **Default:** `false`

- **`ui.hideContextSummary`** (boolean):
  - **Description:** Hide the context summary (GEMINI.md, MCP servers) above the
    input.
  - **Default:** `false`

- **`ui.footer.hideCWD`** (boolean):
  - **Description:** Hide the current working directory path in the footer.
  - **Default:** `false`

- **`ui.footer.hideSandboxStatus`** (boolean):
  - **Description:** Hide the sandbox status indicator in the footer.
  - **Default:** `false`

- **`ui.footer.hideModelInfo`** (boolean):
  - **Description:** Hide the model name and context usage in the footer.
  - **Default:** `false`

- **`ui.footer.hideContextPercentage`** (boolean):
  - **Description:** Hides the context window remaining percentage.
  - **Default:** `true`

- **`ui.hideFooter`** (boolean):
  - **Description:** Hide the footer from the UI
  - **Default:** `false`

- **`ui.showMemoryUsage`** (boolean):
  - **Description:** Display memory usage information in the UI
  - **Default:** `false`

- **`ui.showLineNumbers`** (boolean):
  - **Description:** Show line numbers in the chat.
  - **Default:** `true`

- **`ui.showCitations`** (boolean):
  - **Description:** Show citations for generated text in the chat.
  - **Default:** `false`

- **`ui.showModelInfoInChat`** (boolean):
  - **Description:** Show the model name in the chat for each model turn.
  - **Default:** `false`

- **`ui.useFullWidth`** (boolean):
  - **Description:** Use the entire width of the terminal for output.
  - **Default:** `true`

- **`ui.useAlternateBuffer`** (boolean):
  - **Description:** Use an alternate screen buffer for the UI, preserving shell
    history.
  - **Default:** `false`
  - **Requires restart:** Yes

- **`ui.incrementalRendering`** (boolean):
  - **Description:** Enable incremental rendering for the UI. This option will
    reduce flickering but may cause rendering artifacts. Only supported when
    useAlternateBuffer is enabled.
  - **Default:** `true`
  - **Requires restart:** Yes

- **`ui.customWittyPhrases`** (array):
  - **Description:** Custom witty phrases to display during loading. When
    provided, the CLI cycles through these instead of the defaults.
  - **Default:** `[]`

- **`ui.accessibility.enableLoadingPhrases`** (boolean):
  - **Description:** Enable loading phrases during operations.
  - **Default:** `true`
  - **Requires restart:** Yes

- **`ui.accessibility.screenReader`** (boolean):
  - **Description:** Render output in plain-text to be more screen reader
    accessible
  - **Default:** `false`
  - **Requires restart:** Yes

#### `ide`

- **`ide.enabled`** (boolean):
  - **Description:** Enable IDE integration mode.
  - **Default:** `false`
  - **Requires restart:** Yes

- **`ide.hasSeenNudge`** (boolean):
  - **Description:** Whether the user has seen the IDE integration nudge.
  - **Default:** `false`

#### `privacy`

- **`privacy.usageStatisticsEnabled`** (boolean):
  - **Description:** Enable collection of usage statistics
  - **Default:** `true`
  - **Requires restart:** Yes

#### `model`

- **`model.name`** (string):
  - **Description:** The Gemini model to use for conversations.
  - **Default:** `undefined`

- **`model.maxSessionTurns`** (number):
  - **Description:** Maximum number of user/model/tool turns to keep in a
    session. -1 means unlimited.
  - **Default:** `-1`

- **`model.summarizeToolOutput`** (object):
  - **Description:** Enables or disables summarization of tool output. Configure
    per-tool token budgets (for example {"run_shell_command": {"tokenBudget":
    2000}}). Currently only the run_shell_command tool supports summarization.
  - **Default:** `undefined`

- **`model.compressionThreshold`** (number):
  - **Description:** The fraction of context usage at which to trigger context
    compression (e.g. 0.2, 0.3).
  - **Default:** `0.5`
  - **Requires restart:** Yes

- **`model.skipNextSpeakerCheck`** (boolean):
  - **Description:** Skip the next speaker check.
  - **Default:** `true`

#### `modelConfigs`

- **`modelConfigs.aliases`** (object):
  - **Description:** Named presets for model configs. Can be used in place of a
    model name and can inherit from other aliases using an `extends` property.
  - **Default:**

    ```json
    {
      "base": {
        "modelConfig": {
          "generateContentConfig": {
            "temperature": 0,
            "topP": 1
          }
        }
      },
      "chat-base": {
        "extends": "base",
        "modelConfig": {
          "generateContentConfig": {
            "thinkingConfig": {
              "includeThoughts": true
            },
            "temperature": 1,
            "topP": 0.95,
            "topK": 64
          }
        }
      },
      "chat-base-2.5": {
        "extends": "chat-base",
        "modelConfig": {
          "generateContentConfig": {
            "thinkingConfig": {
              "thinkingBudget": 8192
            }
          }
        }
      },
      "chat-base-3": {
        "extends": "chat-base",
        "modelConfig": {
          "generateContentConfig": {
            "thinkingConfig": {
              "thinkingLevel": "HIGH"
            }
          }
        }
      },
      "gemini-3-pro-preview": {
        "extends": "chat-base-3",
        "modelConfig": {
          "model": "gemini-3-pro-preview"
        }
      },
      "gemini-3-flash-preview": {
        "extends": "chat-base-3",
        "modelConfig": {
          "model": "gemini-3-flash-preview"
        }
      },
      "gemini-2.5-pro": {
        "extends": "chat-base-2.5",
        "modelConfig": {
          "model": "gemini-2.5-pro"
        }
      },
      "gemini-2.5-flash": {
        "extends": "chat-base-2.5",
        "modelConfig": {
          "model": "gemini-2.5-flash"
        }
      },
      "gemini-2.5-flash-lite": {
        "extends": "chat-base-2.5",
        "modelConfig": {
          "model": "gemini-2.5-flash-lite"
        }
      },
      "gemini-2.5-flash-base": {
        "extends": "base",
        "modelConfig": {
          "model": "gemini-2.5-flash"
        }
      },
      "classifier": {
        "extends": "base",
        "modelConfig": {
          "model": "gemini-2.5-flash-lite",
          "generateContentConfig": {
            "maxOutputTokens": 1024,
            "thinkingConfig": {
              "thinkingBudget": 512
            }
          }
        }
      },
      "prompt-completion": {
        "extends": "base",
        "modelConfig": {
          "model": "gemini-2.5-flash-lite",
          "generateContentConfig": {
            "temperature": 0.3,
            "maxOutputTokens": 16000,
            "thinkingConfig": {
              "thinkingBudget": 0
            }
          }
        }
      },
      "edit-corrector": {
        "extends": "base",
        "modelConfig": {
          "model": "gemini-2.5-flash-lite",
          "generateContentConfig": {
            "thinkingConfig": {
              "thinkingBudget": 0
            }
          }
        }
      },
      "summarizer-default": {
        "extends": "base",
        "modelConfig": {
          "model": "gemini-2.5-flash-lite",
          "generateContentConfig": {
            "maxOutputTokens": 2000
          }
        }
      },
      "summarizer-shell": {
        "extends": "base",
        "modelConfig": {
          "model": "gemini-2.5-flash-lite",
          "generateContentConfig": {
            "maxOutputTokens": 2000
          }
        }
      },
      "web-search": {
        "extends": "gemini-2.5-flash-base",
        "modelConfig": {
          "generateContentConfig": {
            "tools": [
              {
                "googleSearch": {}
              }
            ]
          }
        }
      },
      "web-fetch": {
        "extends": "gemini-2.5-flash-base",
        "modelConfig": {
          "generateContentConfig": {
            "tools": [
              {
                "urlContext": {}
              }
            ]
          }
        }
      },
      "web-fetch-fallback": {
        "extends": "gemini-2.5-flash-base",
        "modelConfig": {}
      },
      "loop-detection": {
        "extends": "gemini-2.5-flash-base",
        "modelConfig": {}
      },
      "loop-detection-double-check": {
        "extends": "base",
        "modelConfig": {
          "model": "gemini-2.5-pro"
        }
      },
      "llm-edit-fixer": {
        "extends": "gemini-2.5-flash-base",
        "modelConfig": {}
      },
      "next-speaker-checker": {
        "extends": "gemini-2.5-flash-base",
        "modelConfig": {}
      },
      "chat-compression-3-pro": {
        "modelConfig": {
          "model": "gemini-3-pro-preview"
        }
      },
      "chat-compression-3-flash": {
        "modelConfig": {
          "model": "gemini-3-flash-preview"
        }
      },
      "chat-compression-2.5-pro": {
        "modelConfig": {
          "model": "gemini-2.5-pro"
        }
      },
      "chat-compression-2.5-flash": {
        "modelConfig": {
          "model": "gemini-2.5-flash"
        }
      },
      "chat-compression-2.5-flash-lite": {
        "modelConfig": {
          "model": "gemini-2.5-flash-lite"
        }
      },
      "chat-compression-default": {
        "modelConfig": {
          "model": "gemini-2.5-pro"
        }
      }
    }
    ```

- **`modelConfigs.customAliases`** (object):
  - **Description:** Custom named presets for model configs. These are merged
    with (and override) the built-in aliases.
  - **Default:** `{}`

- **`modelConfigs.customOverrides`** (array):
  - **Description:** Custom model config overrides. These are merged with (and
    added to) the built-in overrides.
  - **Default:** `[]`

- **`modelConfigs.overrides`** (array):
  - **Description:** Apply specific configuration overrides based on matches,
    with a primary key of model (or alias). The most specific match will be
    used.
  - **Default:** `[]`

#### `agents`

- **`agents.overrides`** (object):
  - **Description:** Override settings for specific agents, e.g. to disable the
    agent, set a custom model config, or run config.
  - **Default:** `{}`
  - **Requires restart:** Yes

#### `context`

- **`context.fileName`** (string | string[]):
  - **Description:** The name of the context file or files to load into memory.
    Accepts either a single string or an array of strings.
  - **Default:** `undefined`

- **`context.importFormat`** (string):
  - **Description:** The format to use when importing memory.
  - **Default:** `undefined`

- **`context.discoveryMaxDirs`** (number):
  - **Description:** Maximum number of directories to search for memory.
  - **Default:** `200`

- **`context.includeDirectories`** (array):
  - **Description:** Additional directories to include in the workspace context.
    Missing directories will be skipped with a warning.
  - **Default:** `[]`

- **`context.loadMemoryFromIncludeDirectories`** (boolean):
  - **Description:** Controls how /memory refresh loads GEMINI.md files. When
    true, include directories are scanned; when false, only the current
    directory is used.
  - **Default:** `false`

- **`context.fileFiltering.respectGitIgnore`** (boolean):
  - **Description:** Respect .gitignore files when searching.
  - **Default:** `true`
  - **Requires restart:** Yes

- **`context.fileFiltering.respectGeminiIgnore`** (boolean):
  - **Description:** Respect .geminiignore files when searching.
  - **Default:** `true`
  - **Requires restart:** Yes

- **`context.fileFiltering.enableRecursiveFileSearch`** (boolean):
  - **Description:** Enable recursive file search functionality when completing
    @ references in the prompt.
  - **Default:** `true`
  - **Requires restart:** Yes

- **`context.fileFiltering.enableFuzzySearch`** (boolean):
  - **Description:** Enable fuzzy search when searching for files.
  - **Default:** `true`
  - **Requires restart:** Yes

#### `tools`

- **`tools.sandbox`** (boolean | string):
  - **Description:** Sandbox execution environment. Set to a boolean to enable
    or disable the sandbox, or provide a string path to a sandbox profile.
  - **Default:** `undefined`
  - **Requires restart:** Yes

- **`tools.shell.enableInteractiveShell`** (boolean):
  - **Description:** Use node-pty for an interactive shell experience. Fallback
    to child_process still applies.
  - **Default:** `true`
  - **Requires restart:** Yes

- **`tools.shell.pager`** (string):
  - **Description:** The pager command to use for shell output. Defaults to
    `cat`.
  - **Default:** `"cat"`

- **`tools.shell.showColor`** (boolean):
  - **Description:** Show color in shell output.
  - **Default:** `false`

- **`tools.shell.inactivityTimeout`** (number):
  - **Description:** The maximum time in seconds allowed without output from the
    shell command. Defaults to 5 minutes.
  - **Default:** `300`

- **`tools.shell.enableShellOutputEfficiency`** (boolean):
  - **Description:** Enable shell output efficiency optimizations for better
    performance.
  - **Default:** `true`

- **`tools.autoAccept`** (boolean):
  - **Description:** Automatically accept and execute tool calls that are
    considered safe (e.g., read-only operations).
  - **Default:** `false`

- **`tools.approvalMode`** (enum):
  - **Description:** The default approval mode for tool execution. 'default'
    prompts for approval, 'auto_edit' auto-approves edit tools, and 'plan' is
    read-only mode. 'yolo' is not supported yet.
  - **Default:** `"default"`
  - **Values:** `"default"`, `"auto_edit"`, `"plan"`

- **`tools.core`** (array):
  - **Description:** Restrict the set of built-in tools with an allowlist. Match
    semantics mirror tools.allowed; see the built-in tools documentation for
    available names.
  - **Default:** `undefined`
  - **Requires restart:** Yes

- **`tools.allowed`** (array):
  - **Description:** Tool names that bypass the confirmation dialog. Useful for
    trusted commands (for example ["run_shell_command(git)",
    "run_shell_command(npm test)"]). See shell tool command restrictions for
    matching details.
  - **Default:** `undefined`
  - **Requires restart:** Yes

- **`tools.exclude`** (array):
  - **Description:** Tool names to exclude from discovery.
  - **Default:** `undefined`
  - **Requires restart:** Yes

- **`tools.discoveryCommand`** (string):
  - **Description:** Command to run for tool discovery.
  - **Default:** `undefined`
  - **Requires restart:** Yes

- **`tools.callCommand`** (string):
  - **Description:** Defines a custom shell command for invoking discovered
    tools. The command must take the tool name as the first argument, read JSON
    arguments from stdin, and emit JSON results on stdout.
  - **Default:** `undefined`
  - **Requires restart:** Yes

- **`tools.useRipgrep`** (boolean):
  - **Description:** Use ripgrep for file content search instead of the fallback
    implementation. Provides faster search performance.
  - **Default:** `true`

- **`tools.enableToolOutputTruncation`** (boolean):
  - **Description:** Enable truncation of large tool outputs.
  - **Default:** `true`
  - **Requires restart:** Yes

- **`tools.truncateToolOutputThreshold`** (number):
  - **Description:** Truncate tool output if it is larger than this many
    characters. Set to -1 to disable.
  - **Default:** `4000000`
  - **Requires restart:** Yes

- **`tools.truncateToolOutputLines`** (number):
  - **Description:** The number of lines to keep when truncating tool output.
  - **Default:** `1000`
  - **Requires restart:** Yes

- **`tools.disableLLMCorrection`** (boolean):
  - **Description:** Disable LLM-based error correction for edit tools. When
    enabled, tools will fail immediately if exact string matches are not found,
    instead of attempting to self-correct.
  - **Default:** `true`
  - **Requires restart:** Yes

- **`tools.enableHooks`** (boolean):
  - **Description:** Enables the hooks system experiment. When disabled, the
    hooks system is completely deactivated regardless of other settings.
  - **Default:** `true`
  - **Requires restart:** Yes

#### `mcp`

- **`mcp.serverCommand`** (string):
  - **Description:** Command to start an MCP server.
  - **Default:** `undefined`
  - **Requires restart:** Yes

- **`mcp.allowed`** (array):
  - **Description:** A list of MCP servers to allow.
  - **Default:** `undefined`
  - **Requires restart:** Yes

- **`mcp.excluded`** (array):
  - **Description:** A list of MCP servers to exclude.
  - **Default:** `undefined`
  - **Requires restart:** Yes

#### `useWriteTodos`

- **`useWriteTodos`** (boolean):
  - **Description:** Enable the write_todos tool.
  - **Default:** `true`

#### `security`

- **`security.disableYoloMode`** (boolean):
  - **Description:** Disable YOLO mode, even if enabled by a flag.
  - **Default:** `false`
  - **Requires restart:** Yes

- **`security.enablePermanentToolApproval`** (boolean):
  - **Description:** Enable the "Allow for all future sessions" option in tool
    confirmation dialogs.
  - **Default:** `false`

- **`security.blockGitExtensions`** (boolean):
  - **Description:** Blocks installing and loading extensions from Git.
  - **Default:** `false`
  - **Requires restart:** Yes

- **`security.folderTrust.enabled`** (boolean):
  - **Description:** Setting to track whether Folder trust is enabled.
  - **Default:** `false`
  - **Requires restart:** Yes

- **`security.environmentVariableRedaction.allowed`** (array):
  - **Description:** Environment variables to always allow (bypass redaction).
  - **Default:** `[]`
  - **Requires restart:** Yes

- **`security.environmentVariableRedaction.blocked`** (array):
  - **Description:** Environment variables to always redact.
  - **Default:** `[]`
  - **Requires restart:** Yes

- **`security.environmentVariableRedaction.enabled`** (boolean):
  - **Description:** Enable redaction of environment variables that may contain
    secrets.
  - **Default:** `false`
  - **Requires restart:** Yes

- **`security.auth.selectedType`** (string):
  - **Description:** The currently selected authentication type.
  - **Default:** `undefined`
  - **Requires restart:** Yes

- **`security.auth.enforcedType`** (string):
  - **Description:** The required auth type. If this does not match the selected
    auth type, the user will be prompted to re-authenticate.
  - **Default:** `undefined`
  - **Requires restart:** Yes

- **`security.auth.useExternal`** (boolean):
  - **Description:** Whether to use an external authentication flow.
  - **Default:** `undefined`
  - **Requires restart:** Yes

#### `advanced`

- **`advanced.autoConfigureMemory`** (boolean):
  - **Description:** Automatically configure Node.js memory limits
  - **Default:** `false`
  - **Requires restart:** Yes

- **`advanced.dnsResolutionOrder`** (string):
  - **Description:** The DNS resolution order.
  - **Default:** `undefined`
  - **Requires restart:** Yes

- **`advanced.excludedEnvVars`** (array):
  - **Description:** Environment variables to exclude from project context.
  - **Default:**

    ```json
    ["DEBUG", "DEBUG_MODE"]
    ```

- **`advanced.bugCommand`** (object):
  - **Description:** Configuration for the bug report command.
  - **Default:** `undefined`

#### `experimental`

- **`experimental.enableAgents`** (boolean):
  - **Description:** Enable local and remote subagents. Warning: Experimental
    feature, uses YOLO mode for subagents
  - **Default:** `false`
  - **Requires restart:** Yes

- **`experimental.extensionManagement`** (boolean):
  - **Description:** Enable extension management features.
  - **Default:** `true`
  - **Requires restart:** Yes

- **`experimental.extensionConfig`** (boolean):
  - **Description:** Enable requesting and fetching of extension settings.
  - **Default:** `false`
  - **Requires restart:** Yes

- **`experimental.enableEventDrivenScheduler`** (boolean):
  - **Description:** Enables event-driven scheduler within the CLI session.
  - **Default:** `true`
  - **Requires restart:** Yes

- **`experimental.extensionReloading`** (boolean):
  - **Description:** Enables extension loading/unloading within the CLI session.
  - **Default:** `false`
  - **Requires restart:** Yes

- **`experimental.jitContext`** (boolean):
  - **Description:** Enable Just-In-Time (JIT) context loading.
  - **Default:** `false`
  - **Requires restart:** Yes

- **`experimental.skills`** (boolean):
  - **Description:** Enable Agent Skills (experimental).
  - **Default:** `false`
  - **Requires restart:** Yes

- **`experimental.useOSC52Paste`** (boolean):
  - **Description:** Use OSC 52 sequence for pasting instead of clipboardy
    (useful for remote sessions).
  - **Default:** `false`

- **`experimental.plan`** (boolean):
  - **Description:** Enable planning features (Plan Mode and tools).
  - **Default:** `false`
  - **Requires restart:** Yes

#### `skills`

- **`skills.disabled`** (array):
  - **Description:** List of disabled skills.
  - **Default:** `[]`
  - **Requires restart:** Yes

#### `hooksConfig`

- **`hooksConfig.enabled`** (boolean):
  - **Description:** Canonical toggle for the hooks system. When disabled, no
    hooks will be executed.
  - **Default:** `true`

- **`hooksConfig.disabled`** (array):
  - **Description:** List of hook names (commands) that should be disabled.
    Hooks in this list will not execute even if configured.
  - **Default:** `[]`

- **`hooksConfig.notifications`** (boolean):
  - **Description:** Show visual indicators when hooks are executing.
  - **Default:** `true`

#### `hooks`

- **`hooks.BeforeTool`** (array):
  - **Description:** Hooks that execute before tool execution. Can intercept,
    validate, or modify tool calls.
  - **Default:** `[]`

- **`hooks.AfterTool`** (array):
  - **Description:** Hooks that execute after tool execution. Can process
    results, log outputs, or trigger follow-up actions.
  - **Default:** `[]`

- **`hooks.BeforeAgent`** (array):
  - **Description:** Hooks that execute before agent loop starts. Can set up
    context or initialize resources.
  - **Default:** `[]`

- **`hooks.AfterAgent`** (array):
  - **Description:** Hooks that execute after agent loop completes. Can perform
    cleanup or summarize results.
  - **Default:** `[]`

- **`hooks.Notification`** (array):
  - **Description:** Hooks that execute on notification events (errors,
    warnings, info). Can log or alert on specific conditions.
  - **Default:** `[]`

- **`hooks.SessionStart`** (array):
  - **Description:** Hooks that execute when a session starts. Can initialize
    session-specific resources or state.
  - **Default:** `[]`

- **`hooks.SessionEnd`** (array):
  - **Description:** Hooks that execute when a session ends. Can perform cleanup
    or persist session data.
  - **Default:** `[]`

- **`hooks.PreCompress`** (array):
  - **Description:** Hooks that execute before chat history compression. Can
    back up or analyze conversation before compression.
  - **Default:** `[]`

- **`hooks.BeforeModel`** (array):
  - **Description:** Hooks that execute before LLM requests. Can modify prompts,
    inject context, or control model parameters.
  - **Default:** `[]`

- **`hooks.AfterModel`** (array):
  - **Description:** Hooks that execute after LLM responses. Can process
    outputs, extract information, or log interactions.
  - **Default:** `[]`

- **`hooks.BeforeToolSelection`** (array):
  - **Description:** Hooks that execute before tool selection. Can filter or
    prioritize available tools dynamically.
  - **Default:** `[]`

#### `admin`

- **`admin.secureModeEnabled`** (boolean):
  - **Description:** If true, disallows yolo mode from being used.
  - **Default:** `false`

- **`admin.extensions.enabled`** (boolean):
  - **Description:** If false, disallows extensions from being installed or
    used.
  - **Default:** `true`

- **`admin.mcp.enabled`** (boolean):
  - **Description:** If false, disallows MCP servers from being used.
  - **Default:** `true`

- **`admin.skills.enabled`** (boolean):
  - **Description:** If false, disallows agent skills from being used.
  - **Default:** `true`
  <!-- SETTINGS-AUTOGEN:END -->

#### `mcpServers`

Configures connections to one or more Model-Context Protocol (MCP) servers for
discovering and using custom tools. Gemini CLI attempts to connect to each
configured MCP server to discover available tools. If multiple MCP servers
expose a tool with the same name, the tool names will be prefixed with the
server alias you defined in the configuration (e.g.,
`serverAlias__actualToolName`) to avoid conflicts. Note that the system might
strip certain schema properties from MCP tool definitions for compatibility. At
least one of `command`, `url`, or `httpUrl` must be provided. If multiple are
specified, the order of precedence is `httpUrl`, then `url`, then `command`.

- **`mcpServers.<SERVER_NAME>`** (object): The server parameters for the named
  server.
  - `command` (string, optional): The command to execute to start the MCP server
    via standard I/O.
  - `args` (array of strings, optional): Arguments to pass to the command.
  - `env` (object, optional): Environment variables to set for the server
    process.
  - `cwd` (string, optional): The working directory in which to start the
    server.
  - `url` (string, optional): The URL of an MCP server that uses Server-Sent
    Events (SSE) for communication.
  - `httpUrl` (string, optional): The URL of an MCP server that uses streamable
    HTTP for communication.
  - `headers` (object, optional): A map of HTTP headers to send with requests to
    `url` or `httpUrl`.
  - `timeout` (number, optional): Timeout in milliseconds for requests to this
    MCP server.
  - `trust` (boolean, optional): Trust this server and bypass all tool call
    confirmations.
  - `description` (string, optional): A brief description of the server, which
    may be used for display purposes.
  - `includeTools` (array of strings, optional): List of tool names to include
    from this MCP server. When specified, only the tools listed here will be
    available from this server (allowlist behavior). If not specified, all tools
    from the server are enabled by default.
  - `excludeTools` (array of strings, optional): List of tool names to exclude
    from this MCP server. Tools listed here will not be available to the model,
    even if they are exposed by the server. **Note:** `excludeTools` takes
    precedence over `includeTools` - if a tool is in both lists, it will be
    excluded.

#### `telemetry`

Configures logging and metrics collection for Gemini CLI. For more information,
see [Telemetry](/docs/cli/telemetry).

- **Properties:**
  - **`enabled`** (boolean): Whether or not telemetry is enabled.
  - **`target`** (string): The destination for collected telemetry. Supported
    values are `local` and `gcp`.
  - **`otlpEndpoint`** (string): The endpoint for the OTLP Exporter.
  - **`otlpProtocol`** (string): The protocol for the OTLP Exporter (`grpc` or
    `http`).
  - **`logPrompts`** (boolean): Whether or not to include the content of user
    prompts in the logs.
  - **`outfile`** (string): The file to write telemetry to when `target` is
    `local`.
  - **`useCollector`** (boolean): Whether to use an external OTLP collector.

### Example `settings.json`

Here is an example of a `settings.json` file with the nested structure, new as
of v0.3.0:

```json
{
  "general": {
    "vimMode": true,
    "preferredEditor": "code",
    "sessionRetention": {
      "enabled": true,
      "maxAge": "30d",
      "maxCount": 100
    }
  },
  "ui": {
    "theme": "GitHub",
    "hideBanner": true,
    "hideTips": false,
    "customWittyPhrases": [
      "You forget a thousand things every day. Make sure this is one of ’em",
      "Connecting to AGI"
    ]
  },
  "tools": {
    "sandbox": "docker",
    "discoveryCommand": "bin/get_tools",
    "callCommand": "bin/call_tool",
    "exclude": ["write_file"]
  },
  "mcpServers": {
    "mainServer": {
      "command": "bin/mcp_server.py"
    },
    "anotherServer": {
      "command": "node",
      "args": ["mcp_server.js", "--verbose"]
    }
  },
  "telemetry": {
    "enabled": true,
    "target": "local",
    "otlpEndpoint": "http://localhost:4317",
    "logPrompts": true
  },
  "privacy": {
    "usageStatisticsEnabled": true
  },
  "model": {
    "name": "gemini-1.5-pro-latest",
    "maxSessionTurns": 10,
    "summarizeToolOutput": {
      "run_shell_command": {
        "tokenBudget": 100
      }
    }
  },
  "context": {
    "fileName": ["CONTEXT.md", "GEMINI.md"],
    "includeDirectories": ["path/to/dir1", "~/path/to/dir2", "../path/to/dir3"],
    "loadFromIncludeDirectories": true,
    "fileFiltering": {
      "respectGitIgnore": false
    }
  },
  "advanced": {
    "excludedEnvVars": ["DEBUG", "DEBUG_MODE", "NODE_ENV"]
  }
}
```

## Shell history

The CLI keeps a history of shell commands you run. To avoid conflicts between
different projects, this history is stored in a project-specific directory
within your user's home folder.

- **Location:** `~/.gemini/tmp/<project_hash>/shell_history`
  - `<project_hash>` is a unique identifier generated from your project's root
    path.
  - The history is stored in a file named `shell_history`.

## Environment variables and `.env` files

Environment variables are a common way to configure applications, especially for
sensitive information like API keys or for settings that might change between
environments. For authentication setup, see the
[Authentication documentation](/docs/get-started/authentication) which covers all available
authentication methods.

The CLI automatically loads environment variables from an `.env` file. The
loading order is:

1.  `.env` file in the current working directory.
2.  If not found, it searches upwards in parent directories until it finds an
    `.env` file or reaches the project root (identified by a `.git` folder) or
    the home directory.
3.  If still not found, it looks for `~/.env` (in the user's home directory).

**Environment variable exclusion:** Some environment variables (like `DEBUG` and
`DEBUG_MODE`) are automatically excluded from being loaded from project `.env`
files to prevent interference with gemini-cli behavior. Variables from
`.gemini/.env` files are never excluded. You can customize this behavior using
the `advanced.excludedEnvVars` setting in your `settings.json` file.

- **`GEMINI_API_KEY`**:
  - Your API key for the Gemini API.
  - One of several available [authentication methods](/docs/get-started/authentication).
  - Set this in your shell profile (e.g., `~/.bashrc`, `~/.zshrc`) or an `.env`
    file.
- **`GEMINI_MODEL`**:
  - Specifies the default Gemini model to use.
  - Overrides the hardcoded default
  - Example: `export GEMINI_MODEL="gemini-3-flash-preview"`
- **`GOOGLE_API_KEY`**:
  - Your Google Cloud API key.
  - Required for using Vertex AI in express mode.
  - Ensure you have the necessary permissions.
  - Example: `export GOOGLE_API_KEY="YOUR_GOOGLE_API_KEY"`.
- **`GOOGLE_CLOUD_PROJECT`**:
  - Your Google Cloud Project ID.
  - Required for using Code Assist or Vertex AI.
  - If using Vertex AI, ensure you have the necessary permissions in this
    project.
  - **Cloud Shell note:** When running in a Cloud Shell environment, this
    variable defaults to a special project allocated for Cloud Shell users. If
    you have `GOOGLE_CLOUD_PROJECT` set in your global environment in Cloud
    Shell, it will be overridden by this default. To use a different project in
    Cloud Shell, you must define `GOOGLE_CLOUD_PROJECT` in a `.env` file.
  - Example: `export GOOGLE_CLOUD_PROJECT="YOUR_PROJECT_ID"`.
- **`GOOGLE_APPLICATION_CREDENTIALS`** (string):
  - **Description:** The path to your Google Application Credentials JSON file.
  - **Example:**
    `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"`
- **`OTLP_GOOGLE_CLOUD_PROJECT`**:
  - Your Google Cloud Project ID for Telemetry in Google Cloud
  - Example: `export OTLP_GOOGLE_CLOUD_PROJECT="YOUR_PROJECT_ID"`.
- **`GEMINI_TELEMETRY_ENABLED`**:
  - Set to `true` or `1` to enable telemetry. Any other value is treated as
    disabling it.
  - Overrides the `telemetry.enabled` setting.
- **`GEMINI_TELEMETRY_TARGET`**:
  - Sets the telemetry target (`local` or `gcp`).
  - Overrides the `telemetry.target` setting.
- **`GEMINI_TELEMETRY_OTLP_ENDPOINT`**:
  - Sets the OTLP endpoint for telemetry.
  - Overrides the `telemetry.otlpEndpoint` setting.
- **`GEMINI_TELEMETRY_OTLP_PROTOCOL`**:
  - Sets the OTLP protocol (`grpc` or `http`).
  - Overrides the `telemetry.otlpProtocol` setting.
- **`GEMINI_TELEMETRY_LOG_PROMPTS`**:
  - Set to `true` or `1` to enable or disable logging of user prompts. Any other
    value is treated as disabling it.
  - Overrides the `telemetry.logPrompts` setting.
- **`GEMINI_TELEMETRY_OUTFILE`**:
  - Sets the file path to write telemetry to when the target is `local`.
  - Overrides the `telemetry.outfile` setting.
- **`GEMINI_TELEMETRY_USE_COLLECTOR`**:
  - Set to `true` or `1` to enable or disable using an external OTLP collector.
    Any other value is treated as disabling it.
  - Overrides the `telemetry.useCollector` setting.
- **`GOOGLE_CLOUD_LOCATION`**:
  - Your Google Cloud Project Location (e.g., us-central1).
  - Required for using Vertex AI in non-express mode.
  - Example: `export GOOGLE_CLOUD_LOCATION="YOUR_PROJECT_LOCATION"`.
- **`GEMINI_SANDBOX`**:
  - Alternative to the `sandbox` setting in `settings.json`.
  - Accepts `true`, `false`, `docker`, `podman`, or a custom command string.
- **`GEMINI_SYSTEM_MD`**:
  - Replaces the built‑in system prompt with content from a Markdown file.
  - `true`/`1`: Use project default path `./.gemini/system.md`.
  - Any other string: Treat as a path (relative/absolute supported, `~`
    expands).
  - `false`/`0` or unset: Use the built‑in prompt. See
    [System Prompt Override](/docs/cli/system-prompt).
- **`GEMINI_WRITE_SYSTEM_MD`**:
  - Writes the current built‑in system prompt to a file for review.
  - `true`/`1`: Write to `./.gemini/system.md`. Otherwise treat the value as a
    path.
  - Run the CLI once with this set to generate the file.
- **`SEATBELT_PROFILE`** (macOS specific):
  - Switches the Seatbelt (`sandbox-exec`) profile on macOS.
  - `permissive-open`: (Default) Restricts writes to the project folder (and a
    few other folders, see
    `packages/cli/src/utils/sandbox-macos-permissive-open.sb`) but allows other
    operations.
  - `strict`: Uses a strict profile that declines operations by default.
  - `<profile_name>`: Uses a custom profile. To define a custom profile, create
    a file named `sandbox-macos-<profile_name>.sb` in your project's `.gemini/`
    directory (e.g., `my-project/.gemini/sandbox-macos-custom.sb`).
- **`DEBUG` or `DEBUG_MODE`** (often used by underlying libraries or the CLI
  itself):
  - Set to `true` or `1` to enable verbose debug logging, which can be helpful
    for troubleshooting.
  - **Note:** These variables are automatically excluded from project `.env`
    files by default to prevent interference with gemini-cli behavior. Use
    `.gemini/.env` files if you need to set these for gemini-cli specifically.
- **`NO_COLOR`**:
  - Set to any value to disable all color output in the CLI.
- **`CLI_TITLE`**:
  - Set to a string to customize the title of the CLI.
- **`CODE_ASSIST_ENDPOINT`**:
  - Specifies the endpoint for the code assist server.
  - This is useful for development and testing.

### Environment variable redaction

To prevent accidental leakage of sensitive information, Gemini CLI automatically
redacts potential secrets from environment variables when executing tools (such
as shell commands). This "best effort" redaction applies to variables inherited
from the system or loaded from `.env` files.

**Default Redaction Rules:**

- **By Name:** Variables are redacted if their names contain sensitive terms
  like `TOKEN`, `SECRET`, `PASSWORD`, `KEY`, `AUTH`, `CREDENTIAL`, `PRIVATE`, or
  `CERT`.
- **By Value:** Variables are redacted if their values match known secret
  patterns, such as:
  - Private keys (RSA, OpenSSH, PGP, etc.)
  - Certificates
  - URLs containing credentials
  - API keys and tokens (GitHub, Google, AWS, Stripe, Slack, etc.)
- **Specific Blocklist:** Certain variables like `CLIENT_ID`, `DB_URI`,
  `DATABASE_URL`, and `CONNECTION_STRING` are always redacted by default.

**Allowlist (Never Redacted):**

- Common system variables (e.g., `PATH`, `HOME`, `USER`, `SHELL`, `TERM`,
  `LANG`).
- Variables starting with `GEMINI_CLI_`.
- GitHub Action specific variables.

**Configuration:**

You can customize this behavior in your `settings.json` file:

- **`security.allowedEnvironmentVariables`**: A list of variable names to
  _never_ redact, even if they match sensitive patterns.
- **`security.blockedEnvironmentVariables`**: A list of variable names to
  _always_ redact, even if they don't match sensitive patterns.

```json
{
  "security": {
    "allowedEnvironmentVariables": ["MY_PUBLIC_KEY", "NOT_A_SECRET_TOKEN"],
    "blockedEnvironmentVariables": ["INTERNAL_IP_ADDRESS"]
  }
}
```

## Command-line arguments

Arguments passed directly when running the CLI can override other configurations
for that specific session.

- **`--model <model_name>`** (**`-m <model_name>`**):
  - Specifies the Gemini model to use for this session.
  - Example: `npm start -- --model gemini-3-pro-preview`
- **`--prompt <your_prompt>`** (**`-p <your_prompt>`**):
  - Used to pass a prompt directly to the command. This invokes Gemini CLI in a
    non-interactive mode.
  - For scripting examples, use the `--output-format json` flag to get
    structured output.
- **`--prompt-interactive <your_prompt>`** (**`-i <your_prompt>`**):
  - Starts an interactive session with the provided prompt as the initial input.
  - The prompt is processed within the interactive session, not before it.
  - Cannot be used when piping input from stdin.
  - Example: `gemini -i "explain this code"`
- **`--output-format <format>`**:
  - **Description:** Specifies the format of the CLI output for non-interactive
    mode.
  - **Values:**
    - `text`: (Default) The standard human-readable output.
    - `json`: A machine-readable JSON output.
    - `stream-json`: A streaming JSON output that emits real-time events.
  - **Note:** For structured output and scripting, use the
    `--output-format json` or `--output-format stream-json` flag.
- **`--sandbox`** (**`-s`**):
  - Enables sandbox mode for this session.
- **`--debug`** (**`-d`**):
  - Enables debug mode for this session, providing more verbose output. Open the
    debug console with F12 to see the additional logging.

- **`--help`** (or **`-h`**):
  - Displays help information about command-line arguments.
- **`--yolo`**:
  - Enables YOLO mode, which automatically approves all tool calls.
- **`--approval-mode <mode>`**:
  - Sets the approval mode for tool calls. Available modes:
    - `default`: Prompt for approval on each tool call (default behavior)
    - `auto_edit`: Automatically approve edit tools (replace, write_file) while
      prompting for others
    - `yolo`: Automatically approve all tool calls (equivalent to `--yolo`)
    - `plan`: Read-only mode for tool calls (requires experimental planning to
      be enabled).
      > **Note:** This mode is currently under development and not yet fully
      > functional.
  - Cannot be used together with `--yolo`. Use `--approval-mode=yolo` instead of
    `--yolo` for the new unified approach.
  - Example: `gemini --approval-mode auto_edit`
- **`--allowed-tools <tool1,tool2,...>`**:
  - A comma-separated list of tool names that will bypass the confirmation
    dialog.
  - Example: `gemini --allowed-tools "ShellTool(git status)"`
- **`--extensions <extension_name ...>`** (**`-e <extension_name ...>`**):
  - Specifies a list of extensions to use for the session. If not provided, all
    available extensions are used.
  - Use the special term `gemini -e none` to disable all extensions.
  - Example: `gemini -e my-extension -e my-other-extension`
- **`--list-extensions`** (**`-l`**):
  - Lists all available extensions and exits.
- **`--resume [session_id]`** (**`-r [session_id]`**):
  - Resume a previous chat session. Use "latest" for the most recent session,
    provide a session index number, or provide a full session UUID.
  - If no session_id is provided, defaults to "latest".
  - Example: `gemini --resume 5` or `gemini --resume latest` or
    `gemini --resume a1b2c3d4-e5f6-7890-abcd-ef1234567890` or `gemini --resume`
  - See [Session Management](/docs/cli/session-management) for more details.
- **`--list-sessions`**:
  - List all available chat sessions for the current project and exit.
  - Shows session indices, dates, message counts, and preview of first user
    message.
  - Example: `gemini --list-sessions`
- **`--delete-session <identifier>`**:
  - Delete a specific chat session by its index number or full session UUID.
  - Use `--list-sessions` first to see available sessions, their indices, and
    UUIDs.
  - Example: `gemini --delete-session 3` or
    `gemini --delete-session a1b2c3d4-e5f6-7890-abcd-ef1234567890`
- **`--include-directories <dir1,dir2,...>`**:
  - Includes additional directories in the workspace for multi-directory
    support.
  - Can be specified multiple times or as comma-separated values.
  - 5 directories can be added at maximum.
  - Example: `--include-directories /path/to/project1,/path/to/project2` or
    `--include-directories /path/to/project1 --include-directories /path/to/project2`
- **`--screen-reader`**:
  - Enables screen reader mode, which adjusts the TUI for better compatibility
    with screen readers.
- **`--version`**:
  - Displays the version of the CLI.
- **`--experimental-acp`**:
  - Starts the agent in ACP mode.
- **`--allowed-mcp-server-names`**:
  - Allowed MCP server names.
- **`--fake-responses`**:
  - Path to a file with fake model responses for testing.
- **`--record-responses`**:
  - Path to a file to record model responses for testing.

## Context files (hierarchical instructional context)

While not strictly configuration for the CLI's _behavior_, context files
(defaulting to `GEMINI.md` but configurable via the `context.fileName` setting)
are crucial for configuring the _instructional context_ (also referred to as
"memory") provided to the Gemini model. This powerful feature allows you to give
project-specific instructions, coding style guides, or any relevant background
information to the AI, making its responses more tailored and accurate to your
needs. The CLI includes UI elements, such as an indicator in the footer showing
the number of loaded context files, to keep you informed about the active
context.

- **Purpose:** These Markdown files contain instructions, guidelines, or context
  that you want the Gemini model to be aware of during your interactions. The
  system is designed to manage this instructional context hierarchically.

### Example context file content (e.g., `GEMINI.md`)

Here's a conceptual example of what a context file at the root of a TypeScript
project might contain:

```markdown
# Project: My Awesome TypeScript Library

## General Instructions:

- When generating new TypeScript code, please follow the existing coding style.
- Ensure all new functions and classes have JSDoc comments.
- Prefer functional programming paradigms where appropriate.
- All code should be compatible with TypeScript 5.0 and Node.js 20+.

## Coding Style:

- Use 2 spaces for indentation.
- Interface names should be prefixed with `I` (e.g., `IUserService`).
- Private class members should be prefixed with an underscore (`_`).
- Always use strict equality (`===` and `!==`).

## Specific Component: `src/api/client.ts`

- This file handles all outbound API requests.
- When adding new API call functions, ensure they include robust error handling
  and logging.
- Use the existing `fetchWithRetry` utility for all GET requests.

## Regarding Dependencies:

- Avoid introducing new external dependencies unless absolutely necessary.
- If a new dependency is required, please state the reason.
```

This example demonstrates how you can provide general project context, specific
coding conventions, and even notes about particular files or components. The
more relevant and precise your context files are, the better the AI can assist
you. Project-specific context files are highly encouraged to establish
conventions and context.

- **Hierarchical loading and precedence:** The CLI implements a sophisticated
  hierarchical memory system by loading context files (e.g., `GEMINI.md`) from
  several locations. Content from files lower in this list (more specific)
  typically overrides or supplements content from files higher up (more
  general). The exact concatenation order and final context can be inspected
  using the `/memory show` command. The typical loading order is:
  1.  **Global context file:**
      - Location: `~/.gemini/<configured-context-filename>` (e.g.,
        `~/.gemini/GEMINI.md` in your user home directory).
      - Scope: Provides default instructions for all your projects.
  2.  **Project root and ancestors context files:**
      - Location: The CLI searches for the configured context file in the
        current working directory and then in each parent directory up to either
        the project root (identified by a `.git` folder) or your home directory.
      - Scope: Provides context relevant to the entire project or a significant
        portion of it.
  3.  **Sub-directory context files (contextual/local):**
      - Location: The CLI also scans for the configured context file in
        subdirectories _below_ the current working directory (respecting common
        ignore patterns like `node_modules`, `.git`, etc.). The breadth of this
        search is limited to 200 directories by default, but can be configured
        with the `context.discoveryMaxDirs` setting in your `settings.json`
        file.
      - Scope: Allows for highly specific instructions relevant to a particular
        component, module, or subsection of your project.
- **Concatenation and UI indication:** The contents of all found context files
  are concatenated (with separators indicating their origin and path) and
  provided as part of the system prompt to the Gemini model. The CLI footer
  displays the count of loaded context files, giving you a quick visual cue
  about the active instructional context.
- **Importing content:** You can modularize your context files by importing
  other Markdown files using the `@path/to/file.md` syntax. For more details,
  see the [Memory Import Processor documentation](/docs/core/memport).
- **Commands for memory management:**
  - Use `/memory refresh` to force a re-scan and reload of all context files
    from all configured locations. This updates the AI's instructional context.
  - Use `/memory show` to display the combined instructional context currently
    loaded, allowing you to verify the hierarchy and content being used by the
    AI.
  - See the [Commands documentation](/docs/cli/commands#memory) for full details
    on the `/memory` command and its sub-commands (`show` and `refresh`).

By understanding and utilizing these configuration layers and the hierarchical
nature of context files, you can effectively manage the AI's memory and tailor
the Gemini CLI's responses to your specific needs and projects.

## Sandboxing

The Gemini CLI can execute potentially unsafe operations (like shell commands
and file modifications) within a sandboxed environment to protect your system.

Sandboxing is disabled by default, but you can enable it in a few ways:

- Using `--sandbox` or `-s` flag.
- Setting `GEMINI_SANDBOX` environment variable.
- Sandbox is enabled when using `--yolo` or `--approval-mode=yolo` by default.

By default, it uses a pre-built `gemini-cli-sandbox` Docker image.

For project-specific sandboxing needs, you can create a custom Dockerfile at
`.gemini/sandbox.Dockerfile` in your project's root directory. This Dockerfile
can be based on the base sandbox image:

```dockerfile
FROM gemini-cli-sandbox

# Add your custom dependencies or configurations here
# For example:
# RUN apt-get update && apt-get install -y some-package
# COPY ./my-config /app/my-config
```

When `.gemini/sandbox.Dockerfile` exists, you can use `BUILD_SANDBOX`
environment variable when running Gemini CLI to automatically build the custom
sandbox image:

```bash
BUILD_SANDBOX=1 gemini -s
```

## Usage statistics

To help us improve the Gemini CLI, we collect anonymized usage statistics. This
data helps us understand how the CLI is used, identify common issues, and
prioritize new features.

**What we collect:**

- **Tool calls:** We log the names of the tools that are called, whether they
  succeed or fail, and how long they take to execute. We do not collect the
  arguments passed to the tools or any data returned by them.
- **API requests:** We log the Gemini model used for each request, the duration
  of the request, and whether it was successful. We do not collect the content
  of the prompts or responses.
- **Session information:** We collect information about the configuration of the
  CLI, such as the enabled tools and the approval mode.

**What we DON'T collect:**

- **Personally identifiable information (PII):** We do not collect any personal
  information, such as your name, email address, or API keys.
- **Prompt and response content:** We do not log the content of your prompts or
  the responses from the Gemini model.
- **File content:** We do not log the content of any files that are read or
  written by the CLI.

**How to opt out:**

You can opt out of usage statistics collection at any time by setting the
`usageStatisticsEnabled` property to `false` under the `privacy` category in
your `settings.json` file:

```json
{
  "privacy": {
    "usageStatisticsEnabled": false
  }
}
```


---


# Gemini CLI installation, execution, and deployment

Install and run Gemini CLI. This document provides an overview of Gemini CLI's
installation methods and deployment architecture.

## How to install and/or run Gemini CLI

There are several ways to run Gemini CLI. The recommended option depends on how
you intend to use Gemini CLI.

- As a standard installation. This is the most straightforward method of using
  Gemini CLI.
- In a sandbox. This method offers increased security and isolation.
- From the source. This is recommended for contributors to the project.

### 1. Standard installation (recommended for standard users)

This is the recommended way for end-users to install Gemini CLI. It involves
downloading the Gemini CLI package from the NPM registry.

- **Global install:**

  ```bash
  npm install -g @google/gemini-cli
  ```

  Then, run the CLI from anywhere:

  ```bash
  gemini
  ```

- **NPX execution:**

  ```bash
  # Execute the latest version from NPM without a global install
  npx @google/gemini-cli
  ```

### 2. Run in a sandbox (Docker/Podman)

For security and isolation, Gemini CLI can be run inside a container. This is
the default way that the CLI executes tools that might have side effects.

- **Directly from the registry:** You can run the published sandbox image
  directly. This is useful for environments where you only have Docker and want
  to run the CLI.
  ```bash
  # Run the published sandbox image
  docker run --rm -it us-docker.pkg.dev/gemini-code-dev/gemini-cli/sandbox:0.1.1
  ```
- **Using the `--sandbox` flag:** If you have Gemini CLI installed locally
  (using the standard installation described above), you can instruct it to run
  inside the sandbox container.
  ```bash
  gemini --sandbox -y -p "your prompt here"
  ```

### 3. Run from source (recommended for Gemini CLI contributors)

Contributors to the project will want to run the CLI directly from the source
code.

- **Development mode:** This method provides hot-reloading and is useful for
  active development.
  ```bash
  # From the root of the repository
  npm run start
  ```
- **Production-like mode (linked package):** This method simulates a global
  installation by linking your local package. It's useful for testing a local
  build in a production workflow.

  ```bash
  # Link the local cli package to your global node_modules
  npm link packages/cli

  # Now you can run your local version using the `gemini` command
  gemini
  ```

---

### 4. Running the latest Gemini CLI commit from GitHub

You can run the most recently committed version of Gemini CLI directly from the
GitHub repository. This is useful for testing features still in development.

```bash
# Execute the CLI directly from the main branch on GitHub
npx https://github.com/google-gemini/gemini-cli
```

## Deployment architecture

The execution methods described above are made possible by the following
architectural components and processes:

**NPM packages**

Gemini CLI project is a monorepo that publishes two core packages to the NPM
registry:

- `@google/gemini-cli-core`: The backend, handling logic and tool execution.
- `@google/gemini-cli`: The user-facing frontend.

These packages are used when performing the standard installation and when
running Gemini CLI from the source.

**Build and packaging processes**

There are two distinct build processes used, depending on the distribution
channel:

- **NPM publication:** For publishing to the NPM registry, the TypeScript source
  code in `@google/gemini-cli-core` and `@google/gemini-cli` is transpiled into
  standard JavaScript using the TypeScript Compiler (`tsc`). The resulting
  `dist/` directory is what gets published in the NPM package. This is a
  standard approach for TypeScript libraries.

- **GitHub `npx` execution:** When running the latest version of Gemini CLI
  directly from GitHub, a different process is triggered by the `prepare` script
  in `package.json`. This script uses `esbuild` to bundle the entire application
  and its dependencies into a single, self-contained JavaScript file. This
  bundle is created on-the-fly on the user's machine and is not checked into the
  repository.

**Docker sandbox image**

The Docker-based execution method is supported by the `gemini-cli-sandbox`
container image. This image is published to a container registry and contains a
pre-installed, global version of Gemini CLI.

## Release process

The release process is automated through GitHub Actions. The release workflow
performs the following actions:

1.  Build the NPM packages using `tsc`.
2.  Publish the NPM packages to the artifact registry.
3.  Create GitHub releases with bundled assets.


---


# Gemini CLI examples

Not sure where to get started with Gemini CLI? This document covers examples on
how to use Gemini CLI for a variety of tasks.

**Note:** Results are examples intended to showcase potential use cases. Your
results may vary.

## Rename your photographs based on content

Scenario: You have a folder containing the following files:

```bash
photos/photo1.png
photos/photo2.png
photos/photo3.png
```

Give Gemini the following prompt:

```cli
Rename the photos in my "photos" directory based on their contents.
```

Result: Gemini will ask for permission to rename your files.

Select **Allow once** and your files will be renamed:

```bash
photos/yellow_flowers.png
photos/antique_dresser.png
photos/green_android_robot.png
```

## Explain a repository by reading its code

Scenario: You want to understand how a popular open-source utility works by
inspecting its code, not just its README.

Give Gemini CLI the following prompt:

```cli
Clone the 'chalk' repository from https://github.com/chalk/chalk, read its key source files, and explain how it works.
```

Result: Gemini will perform a sequence of actions to answer your request.

1.  First, it will ask for permission to run `git clone` to download the
    repository.
2.  Next, it will find the important source files and ask for permission to read
    them.
3.  Finally, after analyzing the code, it will provide a summary.

Gemini CLI will return an explanation based on the actual source code:

```markdown
The `chalk` library is a popular npm package for styling terminal output with
colors. After analyzing the source code, here's how it works:

- **Core functionality:** The main file sets up a chainable API. Each color or
  modifier (like `bold` or `italic`) is a getter that appends the corresponding
  ANSI escape code to an internal stack.

- **Chainable API:** When you call `chalk.red.bold('Hello')`, you are accessing
  getters. The `red` getter adds the red color code, and the `bold` getter adds
  the bold code.

- **Output generation:** When the chain is treated as a string (e.g., in
  `console.log`), a final `toString()` method is called. This method joins all
  the stored ANSI codes, wraps them around the input string ('Hello'), and adds
  a reset code at the end. This produces the final, styled string that the
  terminal can render.
```

## Combine two spreadsheets into one spreadsheet

Scenario: You have two .csv files: `Revenue - 2023.csv` and
`Revenue - 2024.csv`. Each file contains monthly revenue figures, like so:

```csv
January,0
February,0
March,0
April,900
May,1000
June,1000
July,1200
August,1800
September,2000
October,2400
November,3400
December,2100
```

You want to combine these two .csv files into a single .csv file.

Give Gemini CLI the following prompt:

```cli
Combine the two .csv files into a single .csv file, with each year a different column.
```

Result: Gemini CLI will read each file and then ask for permission to write a
new file. Provide your permission and Gemini CLI will provide the following
.csv:

```csv
Month,2023,2024
January,0,1000
February,0,1200
March,0,2400
April,900,500
May,1000,800
June,1000,900
July,1200,1000
August,1800,400
September,2000,2000
October,2400,3400
November,3400,1800
December,2100,9000
```

## Run unit tests

Scenario: You've written a simple login page. You wish to write unit tests to
ensure that your login page has code coverage.

Give Gemini CLI the following prompt:

```cli
Write unit tests for Login.js.
```

Result: Gemini CLI will ask for permission to write a new file and create a test
for your login page

```javascript
import React from 'react';
import { render, fireEvent, waitFor } from '@testing-library/react';
import Login from './Login';

// Mock the API call for login
jest.mock('./api', () => ({
  login: jest.fn(),
}));

describe('Login component', () => {
  it('renders the login form correctly', () => {
    const { getByLabelText, getByText } = render(<Login />);

    expect(getByLabelText('Username')).toBeInTheDocument();
    expect(getByLabelText('Password')).toBeInTheDocument();
    expect(getByText('Login')).toBeInTheDocument();
  });

  it('shows an error message with invalid credentials', async () => {
    const { getByLabelText, getByText, findByText } = render(<Login />);
    const usernameInput = getByLabelText('Username');
    const passwordInput = getByLabelText('Password');
    const loginButton = getByText('Login');

    // Mock the API to return an error
    require('./api').login.mockRejectedValue(new Error('Invalid credentials'));

    fireEvent.change(usernameInput, { target: { value: 'testuser' } });
    fireEvent.change(passwordInput, { target: { value: 'wrongpassword' } });
    fireEvent.click(loginButton);

    const errorMessage = await findByText('Invalid credentials');
    expect(errorMessage).toBeInTheDocument();
  });

  it('logs in successfully with valid credentials', async () => {
    const onLoginSuccess = jest.fn();
    const { getByLabelText, getByText } = render(
      <Login onLoginSuccess={onLoginSuccess} />,
    );
    const usernameInput = getByLabelText('Username');
    const passwordInput = getByLabelText('Password');
    const loginButton = getByText('Login');

    // Mock the API to return a success message
    require('./api').login.mockResolvedValue({ success: true });

    fireEvent.change(usernameInput, { target: { value: 'testuser' } });
    fireEvent.change(passwordInput, { target: { value: 'correctpassword' } });
    fireEvent.click(loginButton);

    await waitFor(() => {
      expect(onLoginSuccess).toHaveBeenCalled();
    });
  });

  it('disables the submit button while submitting', async () => {
    const { getByLabelText, getByText } = render(<Login />);
    const usernameInput = getByLabelText('Username');
    const passwordInput = getByLabelText('Password');
    const loginButton = getByText('Login');

    // Mock the API to have a delay
    require('./api').login.mockImplementation(
      () =>
        new Promise((resolve) =>
          setTimeout(() => resolve({ success: true }), 1000),
        ),
    );

    fireEvent.change(usernameInput, { target: { value: 'testuser' } });
    fireEvent.change(passwordInput, { target: { value: 'correctpassword' } });
    fireEvent.click(loginButton);

    expect(loginButton).toBeDisabled();

    await waitFor(() => {
      expect(loginButton).not.toBeDisabled();
    });
  });
});
```


---


# Gemini CLI

Within Gemini CLI, `packages/cli` is the frontend for users to send and receive
prompts with the Gemini AI model and its associated tools. For a general
overview of Gemini CLI, see the [main documentation page](/docs).

## Basic features

- **[Commands](/docs/cli/commands):** A reference for all built-in slash commands
- **[Custom commands](/docs/cli/custom-commands):** Create your own commands and
  shortcuts for frequently used prompts.
- **[Headless mode](/docs/cli/headless):** Use Gemini CLI programmatically for
  scripting and automation.
- **[Model selection](/docs/cli/model):** Configure the Gemini AI model used by the
  CLI.
- **[Settings](/docs/cli/settings):** Configure various aspects of the CLI's behavior
  and appearance.
- **[Themes](/docs/cli/themes):** Customizing the CLI's appearance with different
  themes.
- **[Keyboard shortcuts](/docs/cli/keyboard-shortcuts):** A reference for all
  keyboard shortcuts to improve your workflow.
- **[Tutorials](/docs/cli/tutorials):** Step-by-step guides for common tasks.

## Advanced features

- **[Checkpointing](/docs/cli/checkpointing):** Automatically save and restore
  snapshots of your session and files.
- **[Enterprise configuration](/docs/cli/enterprise):** Deploy and manage Gemini CLI
  in an enterprise environment.
- **[Sandboxing](/docs/cli/sandbox):** Isolate tool execution in a secure,
  containerized environment.
- **[Agent Skills](/docs/cli/skills):** (Experimental) Extend the CLI with
  specialized expertise and procedural workflows.
- **[Telemetry](/docs/cli/telemetry):** Configure observability to monitor usage and
  performance.
- **[Token caching](/docs/cli/token-caching):** Optimize API costs by caching tokens.
- **[Trusted folders](/docs/cli/trusted-folders):** A security feature to control
  which projects can use the full capabilities of the CLI.
- **[Ignoring files (.geminiignore)](/docs/cli/gemini-ignore):** Exclude specific
  files and directories from being accessed by tools.
- **[Context files (GEMINI.md)](/docs/cli/gemini-md):** Provide persistent,
  hierarchical context to the model.
- **[System prompt override](/docs/cli/system-prompt):** Replace the built‑in system
  instructions using `GEMINI_SYSTEM_MD`.

## Non-interactive mode

Gemini CLI can be run in a non-interactive mode, which is useful for scripting
and automation. In this mode, you pipe input to the CLI, it executes the
command, and then it exits.

The following example pipes a command to Gemini CLI from your terminal:

```bash
echo "What is fine tuning?" | gemini
```

You can also use the `--prompt` or `-p` flag:

```bash
gemini -p "What is fine tuning?"
```

For comprehensive documentation on headless usage, scripting, automation, and
advanced examples, see the **[Headless mode](/docs/cli/headless)** guide.


---


# CLI commands

Gemini CLI supports several built-in commands to help you manage your session,
customize the interface, and control its behavior. These commands are prefixed
with a forward slash (`/`), an at symbol (`@`), or an exclamation mark (`!`).

## Slash commands (`/`)

Slash commands provide meta-level control over the CLI itself.

### Built-in Commands

- **`/bug`**
  - **Description:** File an issue about Gemini CLI. By default, the issue is
    filed within the GitHub repository for Gemini CLI. The string you enter
    after `/bug` will become the headline for the bug being filed. The default
    `/bug` behavior can be modified using the `advanced.bugCommand` setting in
    your `.gemini/settings.json` files.

- **`/chat`**
  - **Description:** Save and resume conversation history for branching
    conversation state interactively, or resuming a previous state from a later
    session.
  - **Sub-commands:**
    - **`save`**
      - **Description:** Saves the current conversation history. You must add a
        `<tag>` for identifying the conversation state.
      - **Usage:** `/chat save <tag>`
      - **Details on checkpoint location:** The default locations for saved chat
        checkpoints are:
        - Linux/macOS: `~/.gemini/tmp/<project_hash>/`
        - Windows: `C:\Users\<YourUsername>\.gemini\tmp\<project_hash>\`
        - **Behavior:** Chats are saved into a project-specific directory,
          determined by where you run the CLI. Consequently, saved chats are
          only accessible when working within that same project.
        - **Note:** These checkpoints are for manually saving and resuming
          conversation states. For automatic checkpoints created before file
          modifications, see the
          [Checkpointing documentation](/docs/cli/checkpointing).
    - **`resume`**
      - **Description:** Resumes a conversation from a previous save.
      - **Usage:** `/chat resume <tag>`
      - **Note:** You can only resume chats that were saved within the current
        project. To resume a chat from a different project, you must run the
        Gemini CLI from that project's directory.
    - **`list`**
      - **Description:** Lists available tags for chat state resumption.
      - **Note:** This command only lists chats saved within the current
        project. Because chat history is project-scoped, chats saved in other
        project directories will not be displayed.
    - **`delete`**
      - **Description:** Deletes a saved conversation checkpoint.
      - **Usage:** `/chat delete <tag>`
    - **`share`**
      - **Description** Writes the current conversation to a provided Markdown
        or JSON file.
      - **Usage** `/chat share file.md` or `/chat share file.json`. If no
        filename is provided, then the CLI will generate one.

- **`/clear`**
  - **Description:** Clear the terminal screen, including the visible session
    history and scrollback within the CLI. The underlying session data (for
    history recall) might be preserved depending on the exact implementation,
    but the visual display is cleared.
  - **Keyboard shortcut:** Press **Ctrl+L** at any time to perform a clear
    action.

- **`/compress`**
  - **Description:** Replace the entire chat context with a summary. This saves
    on tokens used for future tasks while retaining a high level summary of what
    has happened.

- **`/copy`**
  - **Description:** Copies the last output produced by Gemini CLI to your
    clipboard, for easy sharing or reuse.
  - **Behavior:**
    - Local sessions use system clipboard tools (pbcopy/xclip/clip).
    - Remote sessions (SSH/WSL) use OSC 52 and require terminal support.
  - **Note:** This command requires platform-specific clipboard tools to be
    installed.
    - On Linux, it requires `xclip` or `xsel`. You can typically install them
      using your system's package manager.
    - On macOS, it requires `pbcopy`, and on Windows, it requires `clip`. These
      tools are typically pre-installed on their respective systems.

- **`/directory`** (or **`/dir`**)
  - **Description:** Manage workspace directories for multi-directory support.
  - **Sub-commands:**
    - **`add`**:
      - **Description:** Add a directory to the workspace. The path can be
        absolute or relative to the current working directory. Moreover, the
        reference from home directory is supported as well.
      - **Usage:** `/directory add <path1>,<path2>`
      - **Note:** Disabled in restrictive sandbox profiles. If you're using
        that, use `--include-directories` when starting the session instead.
    - **`show`**:
      - **Description:** Display all directories added by `/directory add` and
        `--include-directories`.
      - **Usage:** `/directory show`

- **`/editor`**
  - **Description:** Open a dialog for selecting supported editors.

- **`/extensions`**
  - **Description:** Lists all active extensions in the current Gemini CLI
    session. See [Gemini CLI Extensions](/docs/extensions).

- **`/help`** (or **`/?`**)
  - **Description:** Display help information about Gemini CLI, including
    available commands and their usage.

- **`/mcp`**
  - **Description:** Manage configured Model Context Protocol (MCP) servers.
  - **Sub-commands:**
    - **`list`** or **`ls`**:
      - **Description:** List configured MCP servers and tools. This is the
        default action if no subcommand is specified.
    - **`desc`**
      - **Description:** List configured MCP servers and tools with
        descriptions.
    - **`schema`**:
      - **Description:** List configured MCP servers and tools with descriptions
        and schemas.
    - **`auth`**:
      - **Description:** Authenticate with an OAuth-enabled MCP server.
      - **Usage:** `/mcp auth <server-name>`
      - **Details:** If `<server-name>` is provided, it initiates the OAuth flow
        for that server. If no server name is provided, it lists all configured
        servers that support OAuth authentication.
    - **`refresh`**:
      - **Description:** Restarts all MCP servers and re-discovers their
        available tools.

- [**`/model`**](/docs/cli/model)
  - **Description:** Opens a dialog to choose your Gemini model.

- **`/memory`**
  - **Description:** Manage the AI's instructional context (hierarchical memory
    loaded from `GEMINI.md` files).
  - **Sub-commands:**
    - **`add`**:
      - **Description:** Adds the following text to the AI's memory. Usage:
        `/memory add <text to remember>`
    - **`show`**:
      - **Description:** Display the full, concatenated content of the current
        hierarchical memory that has been loaded from all `GEMINI.md` files.
        This lets you inspect the instructional context being provided to the
        Gemini model.
    - **`refresh`**:
      - **Description:** Reload the hierarchical instructional memory from all
        `GEMINI.md` files found in the configured locations (global,
        project/ancestors, and sub-directories). This command updates the model
        with the latest `GEMINI.md` content.
    - **`list`**:
      - **Description:** Lists the paths of the GEMINI.md files in use for
        hierarchical memory.
    - **Note:** For more details on how `GEMINI.md` files contribute to
      hierarchical memory, see the
      [CLI Configuration documentation](/docs/get-started/configuration).

- **`/restore`**
  - **Description:** Restores the project files to the state they were in just
    before a tool was executed. This is particularly useful for undoing file
    edits made by a tool. If run without a tool call ID, it will list available
    checkpoints to restore from.
  - **Usage:** `/restore [tool_call_id]`
  - **Note:** Only available if checkpointing is configured via
    [settings](/docs/get-started/configuration). See
    [Checkpointing documentation](/docs/cli/checkpointing) for more details.

- **`/resume`**
  - **Description:** Browse and resume previous conversation sessions. Opens an
    interactive session browser where you can search, filter, and select from
    automatically saved conversations.
  - **Features:**
    - **Session Browser:** Interactive interface showing all saved sessions with
      timestamps, message counts, and first user message for context
    - **Search:** Use `/` to search through conversation content across all
      sessions
    - **Sorting:** Sort sessions by date or message count
    - **Management:** Delete unwanted sessions directly from the browser
    - **Resume:** Select any session to resume and continue the conversation
  - **Note:** All conversations are automatically saved as you chat - no manual
    saving required. See [Session Management](/docs/cli/session-management) for
    complete details.

- [**`/settings`**](/docs/cli/settings)
  - **Description:** Open the settings editor to view and modify Gemini CLI
    settings.
  - **Details:** This command provides a user-friendly interface for changing
    settings that control the behavior and appearance of Gemini CLI. It is
    equivalent to manually editing the `.gemini/settings.json` file, but with
    validation and guidance to prevent errors. See the
    [settings documentation](/docs/cli/settings) for a full list of available
    settings.
  - **Usage:** Simply run `/settings` and the editor will open. You can then
    browse or search for specific settings, view their current values, and
    modify them as desired. Changes to some settings are applied immediately,
    while others require a restart.

- [**`/skills`**](/docs/cli/skills)
  - **Description:** (Experimental) Manage Agent Skills, which provide on-demand
    expertise and specialized workflows.
  - **Sub-commands:**
    - **`list`**:
      - **Description:** List all discovered skills and their current status
        (enabled/disabled).
    - **`enable`**:
      - **Description:** Enable a specific skill by name.
      - **Usage:** `/skills enable <name>`
    - **`disable`**:
      - **Description:** Disable a specific skill by name.
      - **Usage:** `/skills disable <name>`
    - **`reload`**:
      - **Description:** Refresh the list of discovered skills from all tiers
        (workspace, user, and extensions).

- **`/stats`**
  - **Description:** Display detailed statistics for the current Gemini CLI
    session, including token usage, cached token savings (when available), and
    session duration. Note: Cached token information is only displayed when
    cached tokens are being used, which occurs with API key authentication but
    not with OAuth authentication at this time.

- [**`/theme`**](/docs/cli/themes)
  - **Description:** Open a dialog that lets you change the visual theme of
    Gemini CLI.

- **`/auth`**
  - **Description:** Open a dialog that lets you change the authentication
    method.

- **`/about`**
  - **Description:** Show version info. Please share this information when
    filing issues.

- [**`/tools`**](/docs/tools)
  - **Description:** Display a list of tools that are currently available within
    Gemini CLI.
  - **Usage:** `/tools [desc]`
  - **Sub-commands:**
    - **`desc`** or **`descriptions`**:
      - **Description:** Show detailed descriptions of each tool, including each
        tool's name with its full description as provided to the model.
    - **`nodesc`** or **`nodescriptions`**:
      - **Description:** Hide tool descriptions, showing only the tool names.

- **`/privacy`**
  - **Description:** Display the Privacy Notice and allow users to select
    whether they consent to the collection of their data for service improvement
    purposes.

- **`/quit`** (or **`/exit`**)
  - **Description:** Exit Gemini CLI.

- **`/vim`**
  - **Description:** Toggle vim mode on or off. When vim mode is enabled, the
    input area supports vim-style navigation and editing commands in both NORMAL
    and INSERT modes.
  - **Features:**
    - **NORMAL mode:** Navigate with `h`, `j`, `k`, `l`; jump by words with `w`,
      `b`, `e`; go to line start/end with `0`, `$`, `^`; go to specific lines
      with `G` (or `gg` for first line)
    - **INSERT mode:** Standard text input with escape to return to NORMAL mode
    - **Editing commands:** Delete with `x`, change with `c`, insert with `i`,
      `a`, `o`, `O`; complex operations like `dd`, `cc`, `dw`, `cw`
    - **Count support:** Prefix commands with numbers (e.g., `3h`, `5w`, `10G`)
    - **Repeat last command:** Use `.` to repeat the last editing operation
    - **Persistent setting:** Vim mode preference is saved to
      `~/.gemini/settings.json` and restored between sessions
  - **Status indicator:** When enabled, shows `[NORMAL]` or `[INSERT]` in the
    footer

- **`/init`**
  - **Description:** To help users easily create a `GEMINI.md` file, this
    command analyzes the current directory and generates a tailored context
    file, making it simpler for them to provide project-specific instructions to
    the Gemini agent.

### Custom commands

Custom commands allow you to create personalized shortcuts for your most-used
prompts. For detailed instructions on how to create, manage, and use them,
please see the dedicated [Custom Commands documentation](/docs/cli/custom-commands).

## Input prompt shortcuts

These shortcuts apply directly to the input prompt for text manipulation.

- **Undo:**
  - **Keyboard shortcut:** Press **Ctrl+z** to undo the last action in the input
    prompt.

- **Redo:**
  - **Keyboard shortcut:** Press **Ctrl+Shift+Z** to redo the last undone action
    in the input prompt.

## At commands (`@`)

At commands are used to include the content of files or directories as part of
your prompt to Gemini. These commands include git-aware filtering.

- **`@<path_to_file_or_directory>`**
  - **Description:** Inject the content of the specified file or files into your
    current prompt. This is useful for asking questions about specific code,
    text, or collections of files.
  - **Examples:**
    - `@path/to/your/file.txt Explain this text.`
    - `@src/my_project/ Summarize the code in this directory.`
    - `What is this file about? @README.md`
  - **Details:**
    - If a path to a single file is provided, the content of that file is read.
    - If a path to a directory is provided, the command attempts to read the
      content of files within that directory and any subdirectories.
    - Spaces in paths should be escaped with a backslash (e.g.,
      `@My\ Documents/file.txt`).
    - The command uses the `read_many_files` tool internally. The content is
      fetched and then inserted into your query before being sent to the Gemini
      model.
    - **Git-aware filtering:** By default, git-ignored files (like
      `node_modules/`, `dist/`, `.env`, `.git/`) are excluded. This behavior can
      be changed via the `context.fileFiltering` settings.
    - **File types:** The command is intended for text-based files. While it
      might attempt to read any file, binary files or very large files might be
      skipped or truncated by the underlying `read_many_files` tool to ensure
      performance and relevance. The tool indicates if files were skipped.
  - **Output:** The CLI will show a tool call message indicating that
    `read_many_files` was used, along with a message detailing the status and
    the path(s) that were processed.

- **`@` (Lone at symbol)**
  - **Description:** If you type a lone `@` symbol without a path, the query is
    passed as-is to the Gemini model. This might be useful if you are
    specifically talking _about_ the `@` symbol in your prompt.

### Error handling for `@` commands

- If the path specified after `@` is not found or is invalid, an error message
  will be displayed, and the query might not be sent to the Gemini model, or it
  will be sent without the file content.
- If the `read_many_files` tool encounters an error (e.g., permission issues),
  this will also be reported.

## Shell mode and passthrough commands (`!`)

The `!` prefix lets you interact with your system's shell directly from within
Gemini CLI.

- **`!<shell_command>`**
  - **Description:** Execute the given `<shell_command>` using `bash` on
    Linux/macOS or `powershell.exe -NoProfile -Command` on Windows (unless you
    override `ComSpec`). Any output or errors from the command are displayed in
    the terminal.
  - **Examples:**
    - `!ls -la` (executes `ls -la` and returns to Gemini CLI)
    - `!git status` (executes `git status` and returns to Gemini CLI)

- **`!` (Toggle shell mode)**
  - **Description:** Typing `!` on its own toggles shell mode.
    - **Entering shell mode:**
      - When active, shell mode uses a different coloring and a "Shell Mode
        Indicator".
      - While in shell mode, text you type is interpreted directly as a shell
        command.
    - **Exiting shell mode:**
      - When exited, the UI reverts to its standard appearance and normal Gemini
        CLI behavior resumes.

- **Caution for all `!` usage:** Commands you execute in shell mode have the
  same permissions and impact as if you ran them directly in your terminal.

- **Environment variable:** When a command is executed via `!` or in shell mode,
  the `GEMINI_CLI=1` environment variable is set in the subprocess's
  environment. This allows scripts or tools to detect if they are being run from
  within the Gemini CLI.
  
  
---
  
  
# Checkpointing

The Gemini CLI includes a Checkpointing feature that automatically saves a
snapshot of your project's state before any file modifications are made by
AI-powered tools. This allows you to safely experiment with and apply code
changes, knowing you can instantly revert back to the state before the tool was
run.

## How it works

When you approve a tool that modifies the file system (like `write_file` or
`replace`), the CLI automatically creates a "checkpoint." This checkpoint
includes:

1.  **A Git snapshot:** A commit is made in a special, shadow Git repository
    located in your home directory (`~/.gemini/history/<project_hash>`). This
    snapshot captures the complete state of your project files at that moment.
    It does **not** interfere with your own project's Git repository.
2.  **Conversation history:** The entire conversation you've had with the agent
    up to that point is saved.
3.  **The tool call:** The specific tool call that was about to be executed is
    also stored.

If you want to undo the change or simply go back, you can use the `/restore`
command. Restoring a checkpoint will:

- Revert all files in your project to the state captured in the snapshot.
- Restore the conversation history in the CLI.
- Re-propose the original tool call, allowing you to run it again, modify it, or
  simply ignore it.

All checkpoint data, including the Git snapshot and conversation history, is
stored locally on your machine. The Git snapshot is stored in the shadow
repository while the conversation history and tool calls are saved in a JSON
file in your project's temporary directory, typically located at
`~/.gemini/tmp/<project_hash>/checkpoints`.

## Enabling the feature

The Checkpointing feature is disabled by default. To enable it, you need to edit
your `settings.json` file.

> **Note:** The `--checkpointing` command-line flag was removed in version
> 0.11.0. Checkpointing can now only be enabled through the `settings.json`
> configuration file.

Add the following key to your `settings.json`:

```json
{
  "general": {
    "checkpointing": {
      "enabled": true
    }
  }
}
```

## Using the `/restore` command

Once enabled, checkpoints are created automatically. To manage them, you use the
`/restore` command.

### List available checkpoints

To see a list of all saved checkpoints for the current project, simply run:

```
/restore
```

The CLI will display a list of available checkpoint files. These file names are
typically composed of a timestamp, the name of the file being modified, and the
name of the tool that was about to be run (e.g.,
`2025-06-22T10-00-00_000Z-my-file.txt-write_file`).

### Restore a specific checkpoint

To restore your project to a specific checkpoint, use the checkpoint file from
the list:

```
/restore <checkpoint_file>
```

For example:

```
/restore 2025-06-22T10-00-00_000Z-my-file.txt-write_file
```

After running the command, your files and conversation will be immediately
restored to the state they were in when the checkpoint was created, and the
original tool prompt will reappear.



---


# Custom commands

Custom commands let you save and reuse your favorite or most frequently used
prompts as personal shortcuts within Gemini CLI. You can create commands that
are specific to a single project or commands that are available globally across
all your projects, streamlining your workflow and ensuring consistency.

## File locations and precedence

Gemini CLI discovers commands from two locations, loaded in a specific order:

1.  **User commands (global):** Located in `~/.gemini/commands/`. These commands
    are available in any project you are working on.
2.  **Project commands (local):** Located in
    `<your-project-root>/.gemini/commands/`. These commands are specific to the
    current project and can be checked into version control to be shared with
    your team.

If a command in the project directory has the same name as a command in the user
directory, the **project command will always be used.** This allows projects to
override global commands with project-specific versions.

## Naming and namespacing

The name of a command is determined by its file path relative to its `commands`
directory. Subdirectories are used to create namespaced commands, with the path
separator (`/` or `\`) being converted to a colon (`:`).

- A file at `~/.gemini/commands/test.toml` becomes the command `/test`.
- A file at `<project>/.gemini/commands/git/commit.toml` becomes the namespaced
  command `/git:commit`.

## TOML file format (v1)

Your command definition files must be written in the TOML format and use the
`.toml` file extension.

### Required fields

- `prompt` (String): The prompt that will be sent to the Gemini model when the
  command is executed. This can be a single-line or multi-line string.

### Optional fields

- `description` (String): A brief, one-line description of what the command
  does. This text will be displayed next to your command in the `/help` menu.
  **If you omit this field, a generic description will be generated from the
  filename.**

## Handling arguments

Custom commands support two powerful methods for handling arguments. The CLI
automatically chooses the correct method based on the content of your command's
`prompt`.

### 1. Context-aware injection with `{{args}}`

If your `prompt` contains the special placeholder `{{args}}`, the CLI will
replace that placeholder with the text the user typed after the command name.

The behavior of this injection depends on where it is used:

**A. Raw injection (outside shell commands)**

When used in the main body of the prompt, the arguments are injected exactly as
the user typed them.

**Example (`git/fix.toml`):**

```toml
# Invoked via: /git:fix "Button is misaligned"

description = "Generates a fix for a given issue."
prompt = "Please provide a code fix for the issue described here: {{args}}."
```

The model receives:
`Please provide a code fix for the issue described here: "Button is misaligned".`

**B. Using arguments in shell commands (inside `!{...}` blocks)**

When you use `{{args}}` inside a shell injection block (`!{...}`), the arguments
are automatically **shell-escaped** before replacement. This allows you to
safely pass arguments to shell commands, ensuring the resulting command is
syntactically correct and secure while preventing command injection
vulnerabilities.

**Example (`/grep-code.toml`):**

```toml
prompt = """
Please summarize the findings for the pattern `{{args}}`.

Search Results:
!{grep -r {{args}} .}
"""
```

When you run `/grep-code It's complicated`:

1. The CLI sees `{{args}}` used both outside and inside `!{...}`.
2. Outside: The first `{{args}}` is replaced raw with `It's complicated`.
3. Inside: The second `{{args}}` is replaced with the escaped version (e.g., on
   Linux: `"It\'s complicated"`).
4. The command executed is `grep -r "It's complicated" .`.
5. The CLI prompts you to confirm this exact, secure command before execution.
6. The final prompt is sent.

### 2. Default argument handling

If your `prompt` does **not** contain the special placeholder `{{args}}`, the
CLI uses a default behavior for handling arguments.

If you provide arguments to the command (e.g., `/mycommand arg1`), the CLI will
append the full command you typed to the end of the prompt, separated by two
newlines. This allows the model to see both the original instructions and the
specific arguments you just provided.

If you do **not** provide any arguments (e.g., `/mycommand`), the prompt is sent
to the model exactly as it is, with nothing appended.

**Example (`changelog.toml`):**

This example shows how to create a robust command by defining a role for the
model, explaining where to find the user's input, and specifying the expected
format and behavior.

```toml
# In: <project>/.gemini/commands/changelog.toml
# Invoked via: /changelog 1.2.0 added "Support for default argument parsing."

description = "Adds a new entry to the project's CHANGELOG.md file."
prompt = """
# Task: Update Changelog

You are an expert maintainer of this software project. A user has invoked a command to add a new entry to the changelog.

**The user's raw command is appended below your instructions.**

Your task is to parse the `<version>`, `<change_type>`, and `<message>` from their input and use the `write_file` tool to correctly update the `CHANGELOG.md` file.

## Expected Format
The command follows this format: `/changelog <version> <type> <message>`
- `<type>` must be one of: "added", "changed", "fixed", "removed".

## Behavior
1. Read the `CHANGELOG.md` file.
2. Find the section for the specified `<version>`.
3. Add the `<message>` under the correct `<type>` heading.
4. If the version or type section doesn't exist, create it.
5. Adhere strictly to the "Keep a Changelog" format.
"""
```

When you run `/changelog 1.2.0 added "New feature"`, the final text sent to the
model will be the original prompt followed by two newlines and the command you
typed.

### 3. Executing shell commands with `!{...}`

You can make your commands dynamic by executing shell commands directly within
your `prompt` and injecting their output. This is ideal for gathering context
from your local environment, like reading file content or checking the status of
Git.

When a custom command attempts to execute a shell command, Gemini CLI will now
prompt you for confirmation before proceeding. This is a security measure to
ensure that only intended commands can be run.

**How it works:**

1.  **Inject commands:** Use the `!{...}` syntax.
2.  **Argument substitution:** If `{{args}}` is present inside the block, it is
    automatically shell-escaped (see
    [Context-Aware Injection](#1-context-aware-injection-with-args) above).
3.  **Robust parsing:** The parser correctly handles complex shell commands that
    include nested braces, such as JSON payloads. **Note:** The content inside
    `!{...}` must have balanced braces (`{` and `}`). If you need to execute a
    command containing unbalanced braces, consider wrapping it in an external
    script file and calling the script within the `!{...}` block.
4.  **Security check and confirmation:** The CLI performs a security check on
    the final, resolved command (after arguments are escaped and substituted). A
    dialog will appear showing the exact command(s) to be executed.
5.  **Execution and error reporting:** The command is executed. If the command
    fails, the output injected into the prompt will include the error messages
    (stderr) followed by a status line, e.g.,
    `[Shell command exited with code 1]`. This helps the model understand the
    context of the failure.

**Example (`git/commit.toml`):**

This command gets the staged git diff and uses it to ask the model to write a
commit message.

````toml
# In: <project>/.gemini/commands/git/commit.toml
# Invoked via: /git:commit

description = "Generates a Git commit message based on staged changes."

# The prompt uses !{...} to execute the command and inject its output.
prompt = """
Please generate a Conventional Commit message based on the following git diff:

```diff
!{git diff --staged}
```

"""

````

When you run `/git:commit`, the CLI first executes `git diff --staged`, then
replaces `!{git diff --staged}` with the output of that command before sending
the final, complete prompt to the model.

### 4. Injecting file content with `@{...}`

You can directly embed the content of a file or a directory listing into your
prompt using the `@{...}` syntax. This is useful for creating commands that
operate on specific files.

**How it works:**

- **File injection**: `@{path/to/file.txt}` is replaced by the content of
  `file.txt`.
- **Multimodal support**: If the path points to a supported image (e.g., PNG,
  JPEG), PDF, audio, or video file, it will be correctly encoded and injected as
  multimodal input. Other binary files are handled gracefully and skipped.
- **Directory listing**: `@{path/to/dir}` is traversed and each file present
  within the directory and all subdirectories is inserted into the prompt. This
  respects `.gitignore` and `.geminiignore` if enabled.
- **Workspace-aware**: The command searches for the path in the current
  directory and any other workspace directories. Absolute paths are allowed if
  they are within the workspace.
- **Processing order**: File content injection with `@{...}` is processed
  _before_ shell commands (`!{...}`) and argument substitution (`{{args}}`).
- **Parsing**: The parser requires the content inside `@{...}` (the path) to
  have balanced braces (`{` and `}`).

**Example (`review.toml`):**

This command injects the content of a _fixed_ best practices file
(`docs/best-practices.md`) and uses the user's arguments to provide context for
the review.

```toml
# In: <project>/.gemini/commands/review.toml
# Invoked via: /review FileCommandLoader.ts

description = "Reviews the provided context using a best practice guide."
prompt = """
You are an expert code reviewer.

Your task is to review {{args}}.

Use the following best practices when providing your review:

@{docs/best-practices.md}
"""
```

When you run `/review FileCommandLoader.ts`, the `@{docs/best-practices.md}`
placeholder is replaced by the content of that file, and `{{args}}` is replaced
by the text you provided, before the final prompt is sent to the model.

---

## Example: A "Pure Function" refactoring command

Let's create a global command that asks the model to refactor a piece of code.

**1. Create the file and directories:**

First, ensure the user commands directory exists, then create a `refactor`
subdirectory for organization and the final TOML file.

```bash
mkdir -p ~/.gemini/commands/refactor
touch ~/.gemini/commands/refactor/pure.toml
```

**2. Add the content to the file:**

Open `~/.gemini/commands/refactor/pure.toml` in your editor and add the
following content. We are including the optional `description` for best
practice.

```toml
# In: ~/.gemini/commands/refactor/pure.toml
# This command will be invoked via: /refactor:pure

description = "Asks the model to refactor the current context into a pure function."

prompt = """
Please analyze the code I've provided in the current context.
Refactor it into a pure function.

Your response should include:
1. The refactored, pure function code block.
2. A brief explanation of the key changes you made and why they contribute to purity.
"""
```

**3. Run the command:**

That's it! You can now run your command in the CLI. First, you might add a file
to the context, and then invoke your command:

```
> @my-messy-function.js
> /refactor:pure
```

Gemini CLI will then execute the multi-line prompt defined in your TOML file.


---


# Gemini CLI for the enterprise

This document outlines configuration patterns and best practices for deploying
and managing Gemini CLI in an enterprise environment. By leveraging system-level
settings, administrators can enforce security policies, manage tool access, and
ensure a consistent experience for all users.

> **A note on security:** The patterns described in this document are intended
> to help administrators create a more controlled and secure environment for
> using Gemini CLI. However, they should not be considered a foolproof security
> boundary. A determined user with sufficient privileges on their local machine
> may still be able to circumvent these configurations. These measures are
> designed to prevent accidental misuse and enforce corporate policy in a
> managed environment, not to defend against a malicious actor with local
> administrative rights.

## Centralized configuration: The system settings file

The most powerful tools for enterprise administration are the system-wide
settings files. These files allow you to define a baseline configuration
(`system-defaults.json`) and a set of overrides (`settings.json`) that apply to
all users on a machine. For a complete overview of configuration options, see
the [Configuration documentation](/docs/get-started/configuration).

Settings are merged from four files. The precedence order for single-value
settings (like `theme`) is:

1. System Defaults (`system-defaults.json`)
2. User Settings (`~/.gemini/settings.json`)
3. Workspace Settings (`<project>/.gemini/settings.json`)
4. System Overrides (`settings.json`)

This means the System Overrides file has the final say. For settings that are
arrays (`includeDirectories`) or objects (`mcpServers`), the values are merged.

**Example of merging and precedence:**

Here is how settings from different levels are combined.

- **System defaults `system-defaults.json`:**

  ```json
  {
    "ui": {
      "theme": "default-corporate-theme"
    },
    "context": {
      "includeDirectories": ["/etc/gemini-cli/common-context"]
    }
  }
  ```

- **User `settings.json` (`~/.gemini/settings.json`):**

  ```json
  {
    "ui": {
      "theme": "user-preferred-dark-theme"
    },
    "mcpServers": {
      "corp-server": {
        "command": "/usr/local/bin/corp-server-dev"
      },
      "user-tool": {
        "command": "npm start --prefix ~/tools/my-tool"
      }
    },
    "context": {
      "includeDirectories": ["~/gemini-context"]
    }
  }
  ```

- **Workspace `settings.json` (`<project>/.gemini/settings.json`):**

  ```json
  {
    "ui": {
      "theme": "project-specific-light-theme"
    },
    "mcpServers": {
      "project-tool": {
        "command": "npm start"
      }
    },
    "context": {
      "includeDirectories": ["./project-context"]
    }
  }
  ```

- **System overrides `settings.json`:**
  ```json
  {
    "ui": {
      "theme": "system-enforced-theme"
    },
    "mcpServers": {
      "corp-server": {
        "command": "/usr/local/bin/corp-server-prod"
      }
    },
    "context": {
      "includeDirectories": ["/etc/gemini-cli/global-context"]
    }
  }
  ```

This results in the following merged configuration:

- **Final merged configuration:**
  ```json
  {
    "ui": {
      "theme": "system-enforced-theme"
    },
    "mcpServers": {
      "corp-server": {
        "command": "/usr/local/bin/corp-server-prod"
      },
      "user-tool": {
        "command": "npm start --prefix ~/tools/my-tool"
      },
      "project-tool": {
        "command": "npm start"
      }
    },
    "context": {
      "includeDirectories": [
        "/etc/gemini-cli/common-context",
        "~/gemini-context",
        "./project-context",
        "/etc/gemini-cli/global-context"
      ]
    }
  }
  ```

**Why:**

- **`theme`**: The value from the system overrides (`system-enforced-theme`) is
  used, as it has the highest precedence.
- **`mcpServers`**: The objects are merged. The `corp-server` definition from
  the system overrides takes precedence over the user's definition. The unique
  `user-tool` and `project-tool` are included.
- **`includeDirectories`**: The arrays are concatenated in the order of System
  Defaults, User, Workspace, and then System Overrides.

- **Location**:
  - **Linux**: `/etc/gemini-cli/settings.json`
  - **Windows**: `C:\ProgramData\gemini-cli\settings.json`
  - **macOS**: `/Library/Application Support/GeminiCli/settings.json`
  - The path can be overridden using the `GEMINI_CLI_SYSTEM_SETTINGS_PATH`
    environment variable.
- **Control**: This file should be managed by system administrators and
  protected with appropriate file permissions to prevent unauthorized
  modification by users.

By using the system settings file, you can enforce the security and
configuration patterns described below.

### Enforcing system settings with a wrapper script

While the `GEMINI_CLI_SYSTEM_SETTINGS_PATH` environment variable provides
flexibility, a user could potentially override it to point to a different
settings file, bypassing the centrally managed configuration. To mitigate this,
enterprises can deploy a wrapper script or alias that ensures the environment
variable is always set to the corporate-controlled path.

This approach ensures that no matter how the user calls the `gemini` command,
the enterprise settings are always loaded with the highest precedence.

**Example wrapper script:**

Administrators can create a script named `gemini` and place it in a directory
that appears earlier in the user's `PATH` than the actual Gemini CLI binary
(e.g., `/usr/local/bin/gemini`).

```bash
#!/bin/bash

# Enforce the path to the corporate system settings file.
# This ensures that the company's configuration is always applied.
export GEMINI_CLI_SYSTEM_SETTINGS_PATH="/etc/gemini-cli/settings.json"

# Find the original gemini executable.
# This is a simple example; a more robust solution might be needed
# depending on the installation method.
REAL_GEMINI_PATH=$(type -aP gemini | grep -v "^$(type -P gemini)$" | head -n 1)

if [ -z "$REAL_GEMINI_PATH" ]; then
  echo "Error: The original 'gemini' executable was not found." >&2
  exit 1
fi

# Pass all arguments to the real Gemini CLI executable.
exec "$REAL_GEMINI_PATH" "$@"
```

By deploying this script, the `GEMINI_CLI_SYSTEM_SETTINGS_PATH` is set within
the script's environment, and the `exec` command replaces the script process
with the actual Gemini CLI process, which inherits the environment variable.
This makes it significantly more difficult for a user to bypass the enforced
settings.

## Restricting tool access

You can significantly enhance security by controlling which tools the Gemini
model can use. This is achieved through the `tools.core` and `tools.exclude`
settings. For a list of available tools, see the
[Tools documentation](/docs/tools).

### Allowlisting with `coreTools`

The most secure approach is to explicitly add the tools and commands that users
are permitted to execute to an allowlist. This prevents the use of any tool not
on the approved list.

**Example:** Allow only safe, read-only file operations and listing files.

```json
{
  "tools": {
    "core": ["ReadFileTool", "GlobTool", "ShellTool(ls)"]
  }
}
```

### Blocklisting with `excludeTools`

Alternatively, you can add specific tools that are considered dangerous in your
environment to a blocklist.

**Example:** Prevent the use of the shell tool for removing files.

```json
{
  "tools": {
    "exclude": ["ShellTool(rm -rf)"]
  }
}
```

**Security note:** Blocklisting with `excludeTools` is less secure than
allowlisting with `coreTools`, as it relies on blocking known-bad commands, and
clever users may find ways to bypass simple string-based blocks. **Allowlisting
is the recommended approach.**

### Disabling YOLO mode

To ensure that users cannot bypass the confirmation prompt for tool execution,
you can disable YOLO mode at the policy level. This adds a critical layer of
safety, as it prevents the model from executing tools without explicit user
approval.

**Example:** Force all tool executions to require user confirmation.

```json
{
  "security": {
    "disableYoloMode": true
  }
}
```

This setting is highly recommended in an enterprise environment to prevent
unintended tool execution.

## Managing custom tools (MCP servers)

If your organization uses custom tools via
[Model-Context Protocol (MCP) servers](/docs/core/tools-api), it is crucial to
understand how server configurations are managed to apply security policies
effectively.

### How MCP server configurations are merged

Gemini CLI loads `settings.json` files from three levels: System, Workspace, and
User. When it comes to the `mcpServers` object, these configurations are
**merged**:

1.  **Merging:** The lists of servers from all three levels are combined into a
    single list.
2.  **Precedence:** If a server with the **same name** is defined at multiple
    levels (e.g., a server named `corp-api` exists in both system and user
    settings), the definition from the highest-precedence level is used. The
    order of precedence is: **System > Workspace > User**.

This means a user **cannot** override the definition of a server that is already
defined in the system-level settings. However, they **can** add new servers with
unique names.

### Enforcing a catalog of tools

The security of your MCP tool ecosystem depends on a combination of defining the
canonical servers and adding their names to an allowlist.

### Restricting tools within an MCP server

For even greater security, especially when dealing with third-party MCP servers,
you can restrict which specific tools from a server are exposed to the model.
This is done using the `includeTools` and `excludeTools` properties within a
server's definition. This allows you to use a subset of tools from a server
without allowing potentially dangerous ones.

Following the principle of least privilege, it is highly recommended to use
`includeTools` to create an allowlist of only the necessary tools.

**Example:** Only allow the `code-search` and `get-ticket-details` tools from a
third-party MCP server, even if the server offers other tools like
`delete-ticket`.

```json
{
  "mcp": {
    "allowed": ["third-party-analyzer"]
  },
  "mcpServers": {
    "third-party-analyzer": {
      "command": "/usr/local/bin/start-3p-analyzer.sh",
      "includeTools": ["code-search", "get-ticket-details"]
    }
  }
}
```

#### More secure pattern: Define and add to allowlist in system settings

To create a secure, centrally-managed catalog of tools, the system administrator
**must** do both of the following in the system-level `settings.json` file:

1.  **Define the full configuration** for every approved server in the
    `mcpServers` object. This ensures that even if a user defines a server with
    the same name, the secure system-level definition will take precedence.
2.  **Add the names** of those servers to an allowlist using the `mcp.allowed`
    setting. This is a critical security step that prevents users from running
    any servers that are not on this list. If this setting is omitted, the CLI
    will merge and allow any server defined by the user.

**Example system `settings.json`:**

1. Add the _names_ of all approved servers to an allowlist. This will prevent
   users from adding their own servers.

2. Provide the canonical _definition_ for each server on the allowlist.

```json
{
  "mcp": {
    "allowed": ["corp-data-api", "source-code-analyzer"]
  },
  "mcpServers": {
    "corp-data-api": {
      "command": "/usr/local/bin/start-corp-api.sh",
      "timeout": 5000
    },
    "source-code-analyzer": {
      "command": "/usr/local/bin/start-analyzer.sh"
    }
  }
}
```

This pattern is more secure because it uses both definition and an allowlist.
Any server a user defines will either be overridden by the system definition (if
it has the same name) or blocked because its name is not in the `mcp.allowed`
list.

### Less secure pattern: Omitting the allowlist

If the administrator defines the `mcpServers` object but fails to also specify
the `mcp.allowed` allowlist, users may add their own servers.

**Example system `settings.json`:**

This configuration defines servers but does not enforce the allowlist. The
administrator has NOT included the "mcp.allowed" setting.

```json
{
  "mcpServers": {
    "corp-data-api": {
      "command": "/usr/local/bin/start-corp-api.sh"
    }
  }
}
```

In this scenario, a user can add their own server in their local
`settings.json`. Because there is no `mcp.allowed` list to filter the merged
results, the user's server will be added to the list of available tools and
allowed to run.

## Enforcing sandboxing for security

To mitigate the risk of potentially harmful operations, you can enforce the use
of sandboxing for all tool execution. The sandbox isolates tool execution in a
containerized environment.

**Example:** Force all tool execution to happen within a Docker sandbox.

```json
{
  "tools": {
    "sandbox": "docker"
  }
}
```

You can also specify a custom, hardened Docker image for the sandbox by building
a custom `sandbox.Dockerfile` as described in the
[Sandboxing documentation](/docs/cli/sandbox).

## Controlling network access via proxy

In corporate environments with strict network policies, you can configure Gemini
CLI to route all outbound traffic through a corporate proxy. This can be set via
an environment variable, but it can also be enforced for custom tools via the
`mcpServers` configuration.

**Example (for an MCP server):**

```json
{
  "mcpServers": {
    "proxied-server": {
      "command": "node",
      "args": ["mcp_server.js"],
      "env": {
        "HTTP_PROXY": "http://proxy.example.com:8080",
        "HTTPS_PROXY": "http://proxy.example.com:8080"
      }
    }
  }
}
```

## Telemetry and auditing

For auditing and monitoring purposes, you can configure Gemini CLI to send
telemetry data to a central location. This allows you to track tool usage and
other events. For more information, see the
[telemetry documentation](/docs/cli/telemetry).

**Example:** Enable telemetry and send it to a local OTLP collector. If
`otlpEndpoint` is not specified, it defaults to `http://localhost:4317`.

```json
{
  "telemetry": {
    "enabled": true,
    "target": "gcp",
    "logPrompts": false
  }
}
```

**Note:** Ensure that `logPrompts` is set to `false` in an enterprise setting to
avoid collecting potentially sensitive information from user prompts.

## Authentication

You can enforce a specific authentication method for all users by setting the
`enforcedAuthType` in the system-level `settings.json` file. This prevents users
from choosing a different authentication method. See the
[Authentication docs](/docs/cli/authentication) for more details.

**Example:** Enforce the use of Google login for all users.

```json
{
  "enforcedAuthType": "oauth-personal"
}
```

If a user has a different authentication method configured, they will be
prompted to switch to the enforced method. In non-interactive mode, the CLI will
exit with an error if the configured authentication method does not match the
enforced one.

### Restricting logins to corporate domains

For enterprises using Google Workspace, you can enforce that users only
authenticate with their corporate Google accounts. This is a network-level
control that is configured on a proxy server, not within Gemini CLI itself. It
works by intercepting authentication requests to Google and adding a special
HTTP header.

This policy prevents users from logging in with personal Gmail accounts or other
non-corporate Google accounts.

For detailed instructions, see the Google Workspace Admin Help article on
[blocking access to consumer accounts](https://support.google.com/a/answer/1668854?hl=en#zippy=%2Cstep-choose-a-web-proxy-server%2Cstep-configure-the-network-to-block-certain-accounts).

The general steps are as follows:

1.  **Intercept Requests**: Configure your web proxy to intercept all requests
    to `google.com`.
2.  **Add HTTP Header**: For each intercepted request, add the
    `X-GoogApps-Allowed-Domains` HTTP header.
3.  **Specify Domains**: The value of the header should be a comma-separated
    list of your approved Google Workspace domain names.

**Example header:**

```
X-GoogApps-Allowed-Domains: my-corporate-domain.com, secondary-domain.com
```

When this header is present, Google's authentication service will only allow
logins from accounts belonging to the specified domains.

## Putting it all together: example system `settings.json`

Here is an example of a system `settings.json` file that combines several of the
patterns discussed above to create a secure, controlled environment for Gemini
CLI.

```json
{
  "tools": {
    "sandbox": "docker",
    "core": [
      "ReadFileTool",
      "GlobTool",
      "ShellTool(ls)",
      "ShellTool(cat)",
      "ShellTool(grep)"
    ]
  },
  "mcp": {
    "allowed": ["corp-tools"]
  },
  "mcpServers": {
    "corp-tools": {
      "command": "/opt/gemini-tools/start.sh",
      "timeout": 5000
    }
  },
  "telemetry": {
    "enabled": true,
    "target": "gcp",
    "otlpEndpoint": "https://telemetry-prod.example.com:4317",
    "logPrompts": false
  },
  "advanced": {
    "bugCommand": {
      "urlTemplate": "https://servicedesk.example.com/new-ticket?title={title}&details={info}"
    }
  },
  "privacy": {
    "usageStatisticsEnabled": false
  }
}
```

This configuration:

- Forces all tool execution into a Docker sandbox.
- Strictly uses an allowlist for a small set of safe shell commands and file
  tools.
- Defines and allows a single corporate MCP server for custom tools.
- Enables telemetry for auditing, without logging prompt content.
- Redirects the `/bug` command to an internal ticketing system.
- Disables general usage statistics collection.


---


# Headless mode

Headless mode allows you to run Gemini CLI programmatically from command line
scripts and automation tools without any interactive UI. This is ideal for
scripting, automation, CI/CD pipelines, and building AI-powered tools.

- [Headless Mode](#headless-mode)
  - [Overview](#overview)
  - [Basic Usage](#basic-usage)
    - [Direct Prompts](#direct-prompts)
    - [Stdin Input](#stdin-input)
    - [Combining with File Input](#combining-with-file-input)
  - [Output Formats](#output-formats)
    - [Text Output (Default)](#text-output-default)
    - [JSON Output](#json-output)
      - [Response Schema](#response-schema)
      - [Example Usage](#example-usage)
    - [Streaming JSON Output](#streaming-json-output)
      - [When to Use Streaming JSON](#when-to-use-streaming-json)
      - [Event Types](#event-types)
      - [Basic Usage](#basic-usage)
      - [Example Output](#example-output)
      - [Processing Stream Events](#processing-stream-events)
      - [Real-World Examples](#real-world-examples)
    - [File Redirection](#file-redirection)
  - [Configuration Options](#configuration-options)
  - [Examples](#examples)
    - [Code review](#code-review)
    - [Generate commit messages](#generate-commit-messages)
    - [API documentation](#api-documentation)
    - [Batch code analysis](#batch-code-analysis)
    - [Code review](#code-review-1)
    - [Log analysis](#log-analysis)
    - [Release notes generation](#release-notes-generation)
    - [Model and tool usage tracking](#model-and-tool-usage-tracking)
  - [Resources](#resources)

## Overview

The headless mode provides a headless interface to Gemini CLI that:

- Accepts prompts via command line arguments or stdin
- Returns structured output (text or JSON)
- Supports file redirection and piping
- Enables automation and scripting workflows
- Provides consistent exit codes for error handling

## Basic usage

### Direct prompts

Use the `--prompt` (or `-p`) flag to run in headless mode:

```bash
gemini --prompt "What is machine learning?"
```

### Stdin input

Pipe input to Gemini CLI from your terminal:

```bash
echo "Explain this code" | gemini
```

### Combining with file input

Read from files and process with Gemini:

```bash
cat README.md | gemini --prompt "Summarize this documentation"
```

## Output formats

### Text output (default)

Standard human-readable output:

```bash
gemini -p "What is the capital of France?"
```

Response format:

```
The capital of France is Paris.
```

### JSON output

Returns structured data including response, statistics, and metadata. This
format is ideal for programmatic processing and automation scripts.

#### Response schema

The JSON output follows this high-level structure:

```json
{
  "response": "string", // The main AI-generated content answering your prompt
  "stats": {
    // Usage metrics and performance data
    "models": {
      // Per-model API and token usage statistics
      "[model-name]": {
        "api": {
          /* request counts, errors, latency */
        },
        "tokens": {
          /* prompt, response, cached, total counts */
        }
      }
    },
    "tools": {
      // Tool execution statistics
      "totalCalls": "number",
      "totalSuccess": "number",
      "totalFail": "number",
      "totalDurationMs": "number",
      "totalDecisions": {
        /* accept, reject, modify, auto_accept counts */
      },
      "byName": {
        /* per-tool detailed stats */
      }
    },
    "files": {
      // File modification statistics
      "totalLinesAdded": "number",
      "totalLinesRemoved": "number"
    }
  },
  "error": {
    // Present only when an error occurred
    "type": "string", // Error type (e.g., "ApiError", "AuthError")
    "message": "string", // Human-readable error description
    "code": "number" // Optional error code
  }
}
```

#### Example usage

```bash
gemini -p "What is the capital of France?" --output-format json
```

Response:

```json
{
  "response": "The capital of France is Paris.",
  "stats": {
    "models": {
      "gemini-2.5-pro": {
        "api": {
          "totalRequests": 2,
          "totalErrors": 0,
          "totalLatencyMs": 5053
        },
        "tokens": {
          "prompt": 24939,
          "candidates": 20,
          "total": 25113,
          "cached": 21263,
          "thoughts": 154,
          "tool": 0
        }
      },
      "gemini-2.5-flash": {
        "api": {
          "totalRequests": 1,
          "totalErrors": 0,
          "totalLatencyMs": 1879
        },
        "tokens": {
          "prompt": 8965,
          "candidates": 10,
          "total": 9033,
          "cached": 0,
          "thoughts": 30,
          "tool": 28
        }
      }
    },
    "tools": {
      "totalCalls": 1,
      "totalSuccess": 1,
      "totalFail": 0,
      "totalDurationMs": 1881,
      "totalDecisions": {
        "accept": 0,
        "reject": 0,
        "modify": 0,
        "auto_accept": 1
      },
      "byName": {
        "google_web_search": {
          "count": 1,
          "success": 1,
          "fail": 0,
          "durationMs": 1881,
          "decisions": {
            "accept": 0,
            "reject": 0,
            "modify": 0,
            "auto_accept": 1
          }
        }
      }
    },
    "files": {
      "totalLinesAdded": 0,
      "totalLinesRemoved": 0
    }
  }
}
```

### Streaming JSON output

Returns real-time events as newline-delimited JSON (JSONL). Each significant
action (initialization, messages, tool calls, results) emits immediately as it
occurs. This format is ideal for monitoring long-running operations, building
UIs with live progress, and creating automation pipelines that react to events.

#### When to use streaming JSON

Use `--output-format stream-json` when you need:

- **Real-time progress monitoring** - See tool calls and responses as they
  happen
- **Event-driven automation** - React to specific events (e.g., tool failures)
- **Live UI updates** - Build interfaces showing AI agent activity in real-time
- **Detailed execution logs** - Capture complete interaction history with
  timestamps
- **Pipeline integration** - Stream events to logging/monitoring systems

#### Event types

The streaming format emits 6 event types:

1. **`init`** - Session starts (includes session_id, model)
2. **`message`** - User prompts and assistant responses
3. **`tool_use`** - Tool call requests with parameters
4. **`tool_result`** - Tool execution results (success/error)
5. **`error`** - Non-fatal errors and warnings
6. **`result`** - Final session outcome with aggregated stats

#### Basic usage

```bash
# Stream events to console
gemini --output-format stream-json --prompt "What is 2+2?"

# Save event stream to file
gemini --output-format stream-json --prompt "Analyze this code" > events.jsonl

# Parse with jq
gemini --output-format stream-json --prompt "List files" | jq -r '.type'
```

#### Example output

Each line is a complete JSON event:

```jsonl
{"type":"init","timestamp":"2025-10-10T12:00:00.000Z","session_id":"abc123","model":"gemini-2.0-flash-exp"}
{"type":"message","role":"user","content":"List files in current directory","timestamp":"2025-10-10T12:00:01.000Z"}
{"type":"tool_use","tool_name":"Bash","tool_id":"bash-123","parameters":{"command":"ls -la"},"timestamp":"2025-10-10T12:00:02.000Z"}
{"type":"tool_result","tool_id":"bash-123","status":"success","output":"file1.txt\nfile2.txt","timestamp":"2025-10-10T12:00:03.000Z"}
{"type":"message","role":"assistant","content":"Here are the files...","delta":true,"timestamp":"2025-10-10T12:00:04.000Z"}
{"type":"result","status":"success","stats":{"total_tokens":250,"input_tokens":50,"output_tokens":200,"duration_ms":3000,"tool_calls":1},"timestamp":"2025-10-10T12:00:05.000Z"}
```

### File redirection

Save output to files or pipe to other commands:

```bash
# Save to file
gemini -p "Explain Docker" > docker-explanation.txt
gemini -p "Explain Docker" --output-format json > docker-explanation.json

# Append to file
gemini -p "Add more details" >> docker-explanation.txt

# Pipe to other tools
gemini -p "What is Kubernetes?" --output-format json | jq '.response'
gemini -p "Explain microservices" | wc -w
gemini -p "List programming languages" | grep -i "python"
```

## Configuration options

Key command-line options for headless usage:

| Option                  | Description                        | Example                                            |
| ----------------------- | ---------------------------------- | -------------------------------------------------- |
| `--prompt`, `-p`        | Run in headless mode               | `gemini -p "query"`                                |
| `--output-format`       | Specify output format (text, json) | `gemini -p "query" --output-format json`           |
| `--model`, `-m`         | Specify the Gemini model           | `gemini -p "query" -m gemini-2.5-flash`            |
| `--debug`, `-d`         | Enable debug mode                  | `gemini -p "query" --debug`                        |
| `--include-directories` | Include additional directories     | `gemini -p "query" --include-directories src,docs` |
| `--yolo`, `-y`          | Auto-approve all actions           | `gemini -p "query" --yolo`                         |
| `--approval-mode`       | Set approval mode                  | `gemini -p "query" --approval-mode auto_edit`      |

For complete details on all available configuration options, settings files, and
environment variables, see the
[Configuration Guide](/docs/get-started/configuration).

## Examples

#### Code review

```bash
cat src/auth.py | gemini -p "Review this authentication code for security issues" > security-review.txt
```

#### Generate commit messages

```bash
result=$(git diff --cached | gemini -p "Write a concise commit message for these changes" --output-format json)
echo "$result" | jq -r '.response'
```

#### API documentation

```bash
result=$(cat api/routes.js | gemini -p "Generate OpenAPI spec for these routes" --output-format json)
echo "$result" | jq -r '.response' > openapi.json
```

#### Batch code analysis

```bash
for file in src/*.py; do
    echo "Analyzing $file..."
    result=$(cat "$file" | gemini -p "Find potential bugs and suggest improvements" --output-format json)
    echo "$result" | jq -r '.response' > "reports/$(basename "$file").analysis"
    echo "Completed analysis for $(basename "$file")" >> reports/progress.log
done
```

#### Code review

```bash
result=$(git diff origin/main...HEAD | gemini -p "Review these changes for bugs, security issues, and code quality" --output-format json)
echo "$result" | jq -r '.response' > pr-review.json
```

#### Log analysis

```bash
grep "ERROR" /var/log/app.log | tail -20 | gemini -p "Analyze these errors and suggest root cause and fixes" > error-analysis.txt
```

#### Release notes generation

```bash
result=$(git log --oneline v1.0.0..HEAD | gemini -p "Generate release notes from these commits" --output-format json)
response=$(echo "$result" | jq -r '.response')
echo "$response"
echo "$response" >> CHANGELOG.md
```

#### Model and tool usage tracking

```bash
result=$(gemini -p "Explain this database schema" --include-directories db --output-format json)
total_tokens=$(echo "$result" | jq -r '.stats.models // {} | to_entries | map(.value.tokens.total) | add // 0')
models_used=$(echo "$result" | jq -r '.stats.models // {} | keys | join(", ") | if . == "" then "none" else . end')
tool_calls=$(echo "$result" | jq -r '.stats.tools.totalCalls // 0')
tools_used=$(echo "$result" | jq -r '.stats.tools.byName // {} | keys | join(", ") | if . == "" then "none" else . end')
echo "$(date): $total_tokens tokens, $tool_calls tool calls ($tools_used) used with models: $models_used" >> usage.log
echo "$result" | jq -r '.response' > schema-docs.md
echo "Recent usage trends:"
tail -5 usage.log
```

## Resources

- [CLI Configuration](/docs/get-started/configuration) - Complete configuration
  guide
- [Authentication](/docs/get-started/authentication) - Setup authentication
- [Commands](/docs/cli/commands) - Interactive commands reference
- [Tutorials](/docs/cli/tutorials) - Step-by-step automation guides


---


# Gemini CLI keyboard shortcuts

Gemini CLI ships with a set of default keyboard shortcuts for editing input,
navigating history, and controlling the UI. Use this reference to learn the
available combinations.

<!-- KEYBINDINGS-AUTOGEN:START -->

#### Basic Controls

| Action                                                          | Keys       |
| --------------------------------------------------------------- | ---------- |
| Confirm the current selection or choice.                        | `Enter`    |
| Dismiss dialogs or cancel the current focus.                    | `Esc`      |
| Cancel the current request or quit the CLI when input is empty. | `Ctrl + C` |
| Exit the CLI when the input buffer is empty.                    | `Ctrl + D` |

#### Cursor Movement

| Action                                      | Keys                                                         |
| ------------------------------------------- | ------------------------------------------------------------ |
| Move the cursor to the start of the line.   | `Ctrl + A`<br />`Home (no Shift, Ctrl)`                      |
| Move the cursor to the end of the line.     | `Ctrl + E`<br />`End (no Shift, Ctrl)`                       |
| Move the cursor up one line.                | `Up Arrow (no Shift, Alt, Ctrl, Cmd)`                        |
| Move the cursor down one line.              | `Down Arrow (no Shift, Alt, Ctrl, Cmd)`                      |
| Move the cursor one character to the left.  | `Left Arrow (no Shift, Alt, Ctrl, Cmd)`<br />`Ctrl + B`      |
| Move the cursor one character to the right. | `Right Arrow (no Shift, Alt, Ctrl, Cmd)`<br />`Ctrl + F`     |
| Move the cursor one word to the left.       | `Ctrl + Left Arrow`<br />`Alt + Left Arrow`<br />`Alt + B`   |
| Move the cursor one word to the right.      | `Ctrl + Right Arrow`<br />`Alt + Right Arrow`<br />`Alt + F` |

#### Editing

| Action                                           | Keys                                                      |
| ------------------------------------------------ | --------------------------------------------------------- |
| Delete from the cursor to the end of the line.   | `Ctrl + K`                                                |
| Delete from the cursor to the start of the line. | `Ctrl + U`                                                |
| Clear all text in the input field.               | `Ctrl + C`                                                |
| Delete the previous word.                        | `Ctrl + Backspace`<br />`Alt + Backspace`<br />`Ctrl + W` |
| Delete the next word.                            | `Ctrl + Delete`<br />`Alt + Delete`                       |
| Delete the character to the left.                | `Backspace`<br />`Ctrl + H`                               |
| Delete the character to the right.               | `Delete`<br />`Ctrl + D`                                  |
| Undo the most recent text edit.                  | `Ctrl + Z (no Shift)`                                     |
| Redo the most recent undone text edit.           | `Shift + Ctrl + Z`                                        |

#### Scrolling

| Action                   | Keys                              |
| ------------------------ | --------------------------------- |
| Scroll content up.       | `Shift + Up Arrow`                |
| Scroll content down.     | `Shift + Down Arrow`              |
| Scroll to the top.       | `Ctrl + Home`<br />`Shift + Home` |
| Scroll to the bottom.    | `Ctrl + End`<br />`Shift + End`   |
| Scroll up by one page.   | `Page Up`                         |
| Scroll down by one page. | `Page Down`                       |

#### History & Search

| Action                                       | Keys                  |
| -------------------------------------------- | --------------------- |
| Show the previous entry in history.          | `Ctrl + P (no Shift)` |
| Show the next entry in history.              | `Ctrl + N (no Shift)` |
| Start reverse search through history.        | `Ctrl + R`            |
| Submit the selected reverse-search match.    | `Enter (no Ctrl)`     |
| Accept a suggestion while reverse searching. | `Tab`                 |
| Browse and rewind previous interactions.     | `Double Esc`          |

#### Navigation

| Action                           | Keys                                        |
| -------------------------------- | ------------------------------------------- |
| Move selection up in lists.      | `Up Arrow (no Shift)`                       |
| Move selection down in lists.    | `Down Arrow (no Shift)`                     |
| Move up within dialog options.   | `Up Arrow (no Shift)`<br />`K (no Shift)`   |
| Move down within dialog options. | `Down Arrow (no Shift)`<br />`J (no Shift)` |

#### Suggestions & Completions

| Action                                  | Keys                                               |
| --------------------------------------- | -------------------------------------------------- |
| Accept the inline suggestion.           | `Tab`<br />`Enter (no Ctrl)`                       |
| Move to the previous completion option. | `Up Arrow (no Shift)`<br />`Ctrl + P (no Shift)`   |
| Move to the next completion option.     | `Down Arrow (no Shift)`<br />`Ctrl + N (no Shift)` |
| Expand an inline suggestion.            | `Right Arrow`                                      |
| Collapse an inline suggestion.          | `Left Arrow`                                       |

#### Text Input

| Action                                         | Keys                                                                                      |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Submit the current prompt.                     | `Enter (no Shift, Alt, Ctrl, Cmd)`                                                        |
| Insert a newline without submitting.           | `Ctrl + Enter`<br />`Cmd + Enter`<br />`Alt + Enter`<br />`Shift + Enter`<br />`Ctrl + J` |
| Open the current prompt in an external editor. | `Ctrl + X`                                                                                |
| Paste from the clipboard.                      | `Ctrl + V`<br />`Cmd + V`<br />`Alt + V`                                                  |

#### App Controls

| Action                                                                                                | Keys             |
| ----------------------------------------------------------------------------------------------------- | ---------------- |
| Toggle detailed error information.                                                                    | `F12`            |
| Toggle the full TODO list.                                                                            | `Ctrl + T`       |
| Show IDE context details.                                                                             | `Ctrl + G`       |
| Toggle Markdown rendering.                                                                            | `Alt + M`        |
| Toggle copy mode when in alternate buffer mode.                                                       | `Ctrl + S`       |
| Toggle YOLO (auto-approval) mode for tool calls.                                                      | `Ctrl + Y`       |
| Cycle through approval modes: default (prompt), auto_edit (auto-approve edits), and plan (read-only). | `Shift + Tab`    |
| Expand a height-constrained response to show additional lines when not in alternate buffer mode.      | `Ctrl + S`       |
| Focus the shell input from the gemini input.                                                          | `Tab (no Shift)` |
| Focus the Gemini input from the shell input.                                                          | `Tab`            |
| Clear the terminal screen and redraw the UI.                                                          | `Ctrl + L`       |
| Restart the application.                                                                              | `R`              |

<!-- KEYBINDINGS-AUTOGEN:END -->

## Additional context-specific shortcuts

- `Option+B/F/M` (macOS only): Are interpreted as `Cmd+B/F/M` even if your
  terminal isn't configured to send Meta with Option.
- `!` on an empty prompt: Enter or exit shell mode.
- `\` (at end of a line) + `Enter`: Insert a newline without leaving single-line
  mode.
- `Esc` pressed twice quickly: Clear the input prompt if it is not empty,
  otherwise browse and rewind previous interactions.
- `Up Arrow` / `Down Arrow`: When the cursor is at the top or bottom of a
  single-line input, navigate backward or forward through prompt history.
- `Number keys (1-9, multi-digit)` inside selection dialogs: Jump directly to
  the numbered radio option and confirm when the full number is entered.
  
  
---


# Gemini CLI model selection

Select your Gemini CLI model. The `/model` command lets you configure the model
used by Gemini CLI, giving you more control over your results. Use **Pro**
models for complex tasks and reasoning, **Flash** models for high speed results,
or the (recommended) **Auto** setting to choose the best model for your tasks.

> **Note:** The `/model` command (and the `--model` flag) does not override the
> model used by sub-agents. Consequently, even when using the `/model` flag you
> may see other models used in your model usage reports.

## How to use the `/model` command

Use the following command in Gemini CLI:

```
/model
```

Running this command will open a dialog with your options:

| Option            | Description                                                    | Models                                                                 |
| ----------------- | -------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Auto (Gemini 3)   | Let the system choose the best Gemini 3 model for your task.   | gemini-3-pro-preview (if enabled), gemini-3-flash-preview (if enabled) |
| Auto (Gemini 2.5) | Let the system choose the best Gemini 2.5 model for your task. | gemini-2.5-pro, gemini-2.5-flash                                       |
| Manual            | Select a specific model.                                       | Any available model.                                                   |

We recommend selecting one of the above **Auto** options. However, you can
select **Manual** to select a specific model from those available.

### Gemini 3 and preview features

> **Note:** Gemini 3 is not currently available on all account types. To learn
> more about Gemini 3 access, refer to
> [Gemini 3 on Gemini CLI](/docs/get-started/gemini-3).

To enable Gemini 3 Pro and Gemini 3 Flash (if available), enable
[**Preview Features** by using the `settings` command](/docs/cli/settings).

You can also use the `--model` flag to specify a particular Gemini model on
startup. For more details, refer to the
[configuration documentation](/docs/get-started/configuration).

Changes to these settings will be applied to all subsequent interactions with
Gemini CLI.

## Best practices for model selection

- **Default to Auto.** For most users, the _Auto_ option model provides a
  balance between speed and performance, automatically selecting the correct
  model based on the complexity of the task. Example: Developing a web
  application could include a mix of complex tasks (building architecture and
  scaffolding the project) and simple tasks (generating CSS).

- **Switch to Pro if you aren't getting the results you want.** If you think you
  need your model to be a little "smarter," you can manually select Pro. Pro
  will provide you with the highest levels of reasoning and creativity. Example:
  A complex or multi-stage debugging task.

- **Switch to Flash or Flash-Lite if you need faster results.** If you need a
  simple response quickly, Flash or Flash-Lite is the best option. Example:
  Converting a JSON object to a YAML string.
  
  
---


# Sandboxing in the Gemini CLI

This document provides a guide to sandboxing in the Gemini CLI, including
prerequisites, quickstart, and configuration.

## Prerequisites

Before using sandboxing, you need to install and set up the Gemini CLI:

```bash
npm install -g @google/gemini-cli
```

To verify the installation:

```bash
gemini --version
```

## Overview of sandboxing

Sandboxing isolates potentially dangerous operations (such as shell commands or
file modifications) from your host system, providing a security barrier between
AI operations and your environment.

The benefits of sandboxing include:

- **Security**: Prevent accidental system damage or data loss.
- **Isolation**: Limit file system access to project directory.
- **Consistency**: Ensure reproducible environments across different systems.
- **Safety**: Reduce risk when working with untrusted code or experimental
  commands.

## Sandboxing methods

Your ideal method of sandboxing may differ depending on your platform and your
preferred container solution.

### 1. macOS Seatbelt (macOS only)

Lightweight, built-in sandboxing using `sandbox-exec`.

**Default profile**: `permissive-open` - restricts writes outside project
directory but allows most other operations.

### 2. Container-based (Docker/Podman)

Cross-platform sandboxing with complete process isolation.

**Note**: Requires building the sandbox image locally or using a published image
from your organization's registry.

## Quickstart

```bash
# Enable sandboxing with command flag
gemini -s -p "analyze the code structure"

# Use environment variable
export GEMINI_SANDBOX=true
gemini -p "run the test suite"

# Configure in settings.json
{
  "tools": {
    "sandbox": "docker"
  }
}
```

## Configuration

### Enable sandboxing (in order of precedence)

1. **Command flag**: `-s` or `--sandbox`
2. **Environment variable**: `GEMINI_SANDBOX=true|docker|podman|sandbox-exec`
3. **Settings file**: `"sandbox": true` in the `tools` object of your
   `settings.json` file (e.g., `{"tools": {"sandbox": true}}`).

### macOS Seatbelt profiles

Built-in profiles (set via `SEATBELT_PROFILE` env var):

- `permissive-open` (default): Write restrictions, network allowed
- `permissive-closed`: Write restrictions, no network
- `permissive-proxied`: Write restrictions, network via proxy
- `restrictive-open`: Strict restrictions, network allowed
- `restrictive-closed`: Maximum restrictions

### Custom sandbox flags

For container-based sandboxing, you can inject custom flags into the `docker` or
`podman` command using the `SANDBOX_FLAGS` environment variable. This is useful
for advanced configurations, such as disabling security features for specific
use cases.

**Example (Podman)**:

To disable SELinux labeling for volume mounts, you can set the following:

```bash
export SANDBOX_FLAGS="--security-opt label=disable"
```

Multiple flags can be provided as a space-separated string:

```bash
export SANDBOX_FLAGS="--flag1 --flag2=value"
```

## Linux UID/GID handling

The sandbox automatically handles user permissions on Linux. Override these
permissions with:

```bash
export SANDBOX_SET_UID_GID=true   # Force host UID/GID
export SANDBOX_SET_UID_GID=false  # Disable UID/GID mapping
```

## Troubleshooting

### Common issues

**"Operation not permitted"**

- Operation requires access outside sandbox.
- Try more permissive profile or add mount points.

**Missing commands**

- Add to custom Dockerfile.
- Install via `sandbox.bashrc`.

**Network issues**

- Check sandbox profile allows network.
- Verify proxy configuration.

### Debug mode

```bash
DEBUG=1 gemini -s -p "debug command"
```

**Note:** If you have `DEBUG=true` in a project's `.env` file, it won't affect
gemini-cli due to automatic exclusion. Use `.gemini/.env` files for gemini-cli
specific debug settings.

### Inspect sandbox

```bash
# Check environment
gemini -s -p "run shell command: env | grep SANDBOX"

# List mounts
gemini -s -p "run shell command: mount | grep workspace"
```

## Security notes

- Sandboxing reduces but doesn't eliminate all risks.
- Use the most restrictive profile that allows your work.
- Container overhead is minimal after first build.
- GUI applications may not work in sandboxes.

## Related documentation

- [Configuration](/docs/get-started/configuration): Full configuration options.
- [Commands](/docs/cli/commands): Available commands.
- [Troubleshooting](/docs/troubleshooting): General troubleshooting.



---


# Session Management

Gemini CLI includes robust session management features that automatically save
your conversation history. This allows you to interrupt your work and resume
exactly where you left off, review past interactions, and manage your
conversation history effectively.

## Automatic Saving

Every time you interact with Gemini CLI, your session is automatically saved.
This happens in the background without any manual intervention.

- **What is saved:** The complete conversation history, including:
  - Your prompts and the model's responses.
  - All tool executions (inputs and outputs).
  - Token usage statistics (input/output/cached, etc.).
  - Assistant thoughts/reasoning summaries (when available).
- **Location:** Sessions are stored in `~/.gemini/tmp/<project_hash>/chats/`.
- **Scope:** Sessions are project-specific. Switching directories to a different
  project will switch to that project's session history.

## Resuming Sessions

You can resume a previous session to continue the conversation with all prior
context restored.

### From the Command Line

When starting the CLI, you can use the `--resume` (or `-r`) flag:

- **Resume latest:**

  ```bash
  gemini --resume
  ```

  This immediately loads the most recent session.

- **Resume by index:** First, list available sessions (see
  [Listing Sessions](#listing-sessions)), then use the index number:

  ```bash
  gemini --resume 1
  ```

- **Resume by ID:** You can also provide the full session UUID:
  ```bash
  gemini --resume a1b2c3d4-e5f6-7890-abcd-ef1234567890
  ```

### From the Interactive Interface

While the CLI is running, you can use the `/resume` slash command to open the
**Session Browser**:

```text
/resume
```

This opens an interactive interface where you can:

- **Browse:** Scroll through a list of your past sessions.
- **Preview:** See details like the session date, message count, and the first
  user prompt.
- **Search:** Press `/` to enter search mode, then type to filter sessions by ID
  or content.
- **Select:** Press `Enter` to resume the selected session.

## Managing Sessions

### Listing Sessions

To see a list of all available sessions for the current project from the command
line:

```bash
gemini --list-sessions
```

Output example:

```text
Available sessions for this project (3):

  1. Fix bug in auth (2 days ago) [a1b2c3d4]
  2. Refactor database schema (5 hours ago) [e5f67890]
  3. Update documentation (Just now) [abcd1234]
```

### Deleting Sessions

You can remove old or unwanted sessions to free up space or declutter your
history.

**From the Command Line:** Use the `--delete-session` flag with an index or ID:

```bash
gemini --delete-session 2
```

**From the Session Browser:**

1.  Open the browser with `/resume`.
2.  Navigate to the session you want to remove.
3.  Press `x`.

## Configuration

You can configure how Gemini CLI manages your session history in your
`settings.json` file.

### Session Retention

To prevent your history from growing indefinitely, you can enable automatic
cleanup policies.

```json
{
  "general": {
    "sessionRetention": {
      "enabled": true,
      "maxAge": "30d", // Keep sessions for 30 days
      "maxCount": 50 // Keep the 50 most recent sessions
    }
  }
}
```

- **`enabled`**: (boolean) Master switch for session cleanup. Default is
  `false`.
- **`maxAge`**: (string) Duration to keep sessions (e.g., "24h", "7d", "4w").
  Sessions older than this will be deleted.
- **`maxCount`**: (number) Maximum number of sessions to retain. The oldest
  sessions exceeding this count will be deleted.
- **`minRetention`**: (string) Minimum retention period (safety limit). Defaults
  to `"1d"`; sessions newer than this period are never deleted by automatic
  cleanup.

### Session Limits

You can also limit the length of individual sessions to prevent context windows
from becoming too large and expensive.

```json
{
  "model": {
    "maxSessionTurns": 100
  }
}
```

- **`maxSessionTurns`**: (number) The maximum number of turns (user + model
  exchanges) allowed in a single session. Set to `-1` for unlimited (default).

  **Behavior when limit is reached:**
  - **Interactive Mode:** The CLI shows an informational message and stops
    sending requests to the model. You must manually start a new session.
  - **Non-Interactive Mode:** The CLI exits with an error.



---


# Agent Skills

_Note: This is an experimental feature enabled via `experimental.skills`. You
can also search for "Skills" within the `/settings` interactive UI to toggle
this and manage other skill-related settings._

Agent Skills allow you to extend Gemini CLI with specialized expertise,
procedural workflows, and task-specific resources. Based on the
[Agent Skills](https://agentskills.io) open standard, a "skill" is a
self-contained directory that packages instructions and assets into a
discoverable capability.

## Overview

Unlike general context files ([`GEMINI.md`](/docs/cli/gemini-md)), which provide
persistent workspace-wide background, Skills represent **on-demand expertise**.
This allows Gemini to maintain a vast library of specialized capabilities—such
as security auditing, cloud deployments, or codebase migrations—without
cluttering the model's immediate context window.

Gemini autonomously decides when to employ a skill based on your request and the
skill's description. When a relevant skill is identified, the model "pulls in"
the full instructions and resources required to complete the task using the
`activate_skill` tool.

## Key Benefits

- **Shared Expertise:** Package complex workflows (like a specific team's PR
  review process) into a folder that anyone can use.
- **Repeatable Workflows:** Ensure complex multi-step tasks are performed
  consistently by providing a procedural framework.
- **Resource Bundling:** Include scripts, templates, or example data alongside
  instructions so the agent has everything it needs.
- **Progressive Disclosure:** Only skill metadata (name and description) is
  loaded initially. Detailed instructions and resources are only disclosed when
  the model explicitly activates the skill, saving context tokens.

## Skill Discovery Tiers

Gemini CLI discovers skills from three primary locations:

1.  **Workspace Skills** (`.gemini/skills/`): Workspace-specific skills that are
    typically committed to version control and shared with the team.
2.  **User Skills** (`~/.gemini/skills/`): Personal skills available across all
    your workspaces.
3.  **Extension Skills**: Skills bundled within installed
    [extensions](/docs/extensions).

**Precedence:** If multiple skills share the same name, higher-precedence
locations override lower ones: **Workspace > User > Extension**.

## Managing Skills

### In an Interactive Session

Use the `/skills` slash command to view and manage available expertise:

- `/skills list` (default): Shows all discovered skills and their status.
- `/skills disable <name>`: Prevents a specific skill from being used.
- `/skills enable <name>`: Re-enables a disabled skill.
- `/skills reload`: Refreshes the list of discovered skills from all tiers.

_Note: `/skills disable` and `/skills enable` default to the `user` scope. Use
`--scope workspace` to manage workspace-specific settings._

### From the Terminal

The `gemini skills` command provides management utilities:

```bash
# List all discovered skills
gemini skills list

# Install a skill from a Git repository, local directory, or zipped skill file (.skill)
# Uses the user scope by default (~/.gemini/skills)
gemini skills install https://github.com/user/repo.git
gemini skills install /path/to/local/skill
gemini skills install /path/to/local/my-expertise.skill

# Install a specific skill from a monorepo or subdirectory using --path
gemini skills install https://github.com/my-org/my-skills.git --path skills/frontend-design

# Install to the workspace scope (.gemini/skills)
gemini skills install /path/to/skill --scope workspace

# Uninstall a skill by name
gemini skills uninstall my-expertise --scope workspace

# Enable a skill (globally)
gemini skills enable my-expertise

# Disable a skill. Can use --scope to specify workspace or user (defaults to workspace)
gemini skills disable my-expertise --scope workspace
```

## Creating a Skill

A skill is a directory containing a `SKILL.md` file at its root. This file uses
YAML frontmatter for metadata and Markdown for instructions.

### Folder Structure

Skills are self-contained directories. At a minimum, a skill requires a
`SKILL.md` file, but can include other resources:

```text
my-skill/
├── SKILL.md       (Required) Instructions and metadata
├── scripts/       (Optional) Executable scripts/tools
├── references/    (Optional) Static documentation and examples
└── assets/        (Optional) Templates and binary resources
```

### Basic Structure (SKILL.md)

```markdown
---
name: <unique-name>
description: <what the skill does and when Gemini should use it>
---

<your instructions for how the agent should behave / use the skill>
```

- **`name`**: A unique identifier (lowercase, alphanumeric, and dashes).
- **`description`**: The most critical field. Gemini uses this to decide when
  the skill is relevant. Be specific about the expertise provided.
- **Body**: Everything below the second `---` is injected as expert procedural
  guidance for the model.

### Example: Team Code Reviewer

Create `~/.gemini/skills/code-reviewer/SKILL.md`:

```markdown
---
name: code-reviewer
description:
  Expertise in reviewing code for style, security, and performance. Use when the
  user asks for "feedback," a "review," or to "check" their changes.
---

# Code Reviewer

You are an expert code reviewer. When reviewing code, follow this workflow:

1.  **Analyze**: Review the staged changes or specific files provided. Ensure
    that the changes are scoped properly and represent minimal changes required
    to address the issue.
2.  **Style**: Ensure code follows the workspace's conventions and idiomatic
    patterns as described in the `GEMINI.md` file.
3.  **Security**: Flag any potential security vulnerabilities.
4.  **Tests**: Verify that new logic has corresponding test coverage and that
    the test coverage adequately validates the changes.

Provide your feedback as a concise bulleted list of "Strengths" and
"Opportunities."
```

### Resource Conventions

While you can structure your skill directory however you like, the Agent Skills
standard encourages these conventions:

- **`scripts/`**: Executable scripts (bash, python, node) the agent can run.
- **`references/`**: Static documentation, schemas, or example data for the
  agent to consult.
- **`assets/`**: Code templates, boilerplate, or binary resources.

When a skill is activated, Gemini CLI provides the model with a tree view of the
entire skill directory, allowing it to discover and utilize these assets.

## How it Works (Security & Privacy)

1.  **Discovery**: At the start of a session, Gemini CLI scans the discovery
    tiers and injects the name and description of all enabled skills into the
    system prompt.
2.  **Activation**: When Gemini identifies a task matching a skill's
    description, it calls the `activate_skill` tool.
3.  **Consent**: You will see a confirmation prompt in the UI detailing the
    skill's name, purpose, and the directory path it will gain access to.
4.  **Injection**: Upon your approval:
    - The `SKILL.md` body and folder structure is added to the conversation
      history.
    - The skill's directory is added to the agent's allowed file paths, granting
      it permission to read any bundled assets.
5.  **Execution**: The model proceeds with the specialized expertise active. It
    is instructed to prioritize the skill's procedural guidance within reason.



---


# Gemini CLI settings 
(`/settings` command)

Control your Gemini CLI experience with the `/settings` command. The `/settings`
command opens a dialog to view and edit all your Gemini CLI settings, including
your UI experience, keybindings, and accessibility features.

Your Gemini CLI settings are stored in a `settings.json` file. In addition to
using the `/settings` command, you can also edit them in one of the following
locations:

- **User settings**: `~/.gemini/settings.json`
- **Workspace settings**: `your-project/.gemini/settings.json`

Note: Workspace settings override user settings.

## Settings reference

Here is a list of all the available settings, grouped by category and ordered as
they appear in the UI.

<!-- SETTINGS-AUTOGEN:START -->

### General

| UI Label                        | Setting                            | Description                                                   | Default |
| ------------------------------- | ---------------------------------- | ------------------------------------------------------------- | ------- |
| Preview Features (e.g., models) | `general.previewFeatures`          | Enable preview features (e.g., preview models).               | `false` |
| Vim Mode                        | `general.vimMode`                  | Enable Vim keybindings                                        | `false` |
| Enable Auto Update              | `general.enableAutoUpdate`         | Enable automatic updates.                                     | `true`  |
| Enable Prompt Completion        | `general.enablePromptCompletion`   | Enable AI-powered prompt completion suggestions while typing. | `false` |
| Debug Keystroke Logging         | `general.debugKeystrokeLogging`    | Enable debug logging of keystrokes to the console.            | `false` |
| Enable Session Cleanup          | `general.sessionRetention.enabled` | Enable automatic session cleanup                              | `false` |

### Output

| UI Label      | Setting         | Description                                            | Default  |
| ------------- | --------------- | ------------------------------------------------------ | -------- |
| Output Format | `output.format` | The format of the CLI output. Can be `text` or `json`. | `"text"` |

### UI

| UI Label                       | Setting                                 | Description                                                                                                                                                       | Default |
| ------------------------------ | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| Hide Window Title              | `ui.hideWindowTitle`                    | Hide the window title bar                                                                                                                                         | `false` |
| Show Thoughts in Title         | `ui.showStatusInTitle`                  | Show Gemini CLI model thoughts in the terminal window title during the working phase                                                                              | `false` |
| Dynamic Window Title           | `ui.dynamicWindowTitle`                 | Update the terminal window title with current status icons (Ready: ◇, Action Required: ✋, Working: ✦)                                                            | `true`  |
| Show Home Directory Warning    | `ui.showHomeDirectoryWarning`           | Show a warning when running Gemini CLI in the home directory.                                                                                                     | `true`  |
| Hide Tips                      | `ui.hideTips`                           | Hide helpful tips in the UI                                                                                                                                       | `false` |
| Hide Banner                    | `ui.hideBanner`                         | Hide the application banner                                                                                                                                       | `false` |
| Hide Context Summary           | `ui.hideContextSummary`                 | Hide the context summary (GEMINI.md, MCP servers) above the input.                                                                                                | `false` |
| Hide CWD                       | `ui.footer.hideCWD`                     | Hide the current working directory path in the footer.                                                                                                            | `false` |
| Hide Sandbox Status            | `ui.footer.hideSandboxStatus`           | Hide the sandbox status indicator in the footer.                                                                                                                  | `false` |
| Hide Model Info                | `ui.footer.hideModelInfo`               | Hide the model name and context usage in the footer.                                                                                                              | `false` |
| Hide Context Window Percentage | `ui.footer.hideContextPercentage`       | Hides the context window remaining percentage.                                                                                                                    | `true`  |
| Hide Footer                    | `ui.hideFooter`                         | Hide the footer from the UI                                                                                                                                       | `false` |
| Show Memory Usage              | `ui.showMemoryUsage`                    | Display memory usage information in the UI                                                                                                                        | `false` |
| Show Line Numbers              | `ui.showLineNumbers`                    | Show line numbers in the chat.                                                                                                                                    | `true`  |
| Show Citations                 | `ui.showCitations`                      | Show citations for generated text in the chat.                                                                                                                    | `false` |
| Show Model Info In Chat        | `ui.showModelInfoInChat`                | Show the model name in the chat for each model turn.                                                                                                              | `false` |
| Use Full Width                 | `ui.useFullWidth`                       | Use the entire width of the terminal for output.                                                                                                                  | `true`  |
| Use Alternate Screen Buffer    | `ui.useAlternateBuffer`                 | Use an alternate screen buffer for the UI, preserving shell history.                                                                                              | `false` |
| Incremental Rendering          | `ui.incrementalRendering`               | Enable incremental rendering for the UI. This option will reduce flickering but may cause rendering artifacts. Only supported when useAlternateBuffer is enabled. | `true`  |
| Enable Loading Phrases         | `ui.accessibility.enableLoadingPhrases` | Enable loading phrases during operations.                                                                                                                         | `true`  |
| Screen Reader Mode             | `ui.accessibility.screenReader`         | Render output in plain-text to be more screen reader accessible                                                                                                   | `false` |

### IDE

| UI Label | Setting       | Description                  | Default |
| -------- | ------------- | ---------------------------- | ------- |
| IDE Mode | `ide.enabled` | Enable IDE integration mode. | `false` |

### Model

| UI Label                | Setting                      | Description                                                                            | Default |
| ----------------------- | ---------------------------- | -------------------------------------------------------------------------------------- | ------- |
| Max Session Turns       | `model.maxSessionTurns`      | Maximum number of user/model/tool turns to keep in a session. -1 means unlimited.      | `-1`    |
| Compression Threshold   | `model.compressionThreshold` | The fraction of context usage at which to trigger context compression (e.g. 0.2, 0.3). | `0.5`   |
| Skip Next Speaker Check | `model.skipNextSpeakerCheck` | Skip the next speaker check.                                                           | `true`  |

### Context

| UI Label                             | Setting                                           | Description                                                                                                                                     | Default |
| ------------------------------------ | ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| Memory Discovery Max Dirs            | `context.discoveryMaxDirs`                        | Maximum number of directories to search for memory.                                                                                             | `200`   |
| Load Memory From Include Directories | `context.loadMemoryFromIncludeDirectories`        | Controls how /memory refresh loads GEMINI.md files. When true, include directories are scanned; when false, only the current directory is used. | `false` |
| Respect .gitignore                   | `context.fileFiltering.respectGitIgnore`          | Respect .gitignore files when searching.                                                                                                        | `true`  |
| Respect .geminiignore                | `context.fileFiltering.respectGeminiIgnore`       | Respect .geminiignore files when searching.                                                                                                     | `true`  |
| Enable Recursive File Search         | `context.fileFiltering.enableRecursiveFileSearch` | Enable recursive file search functionality when completing @ references in the prompt.                                                          | `true`  |
| Enable Fuzzy Search                  | `context.fileFiltering.enableFuzzySearch`         | Enable fuzzy search when searching for files.                                                                                                   | `true`  |

### Tools

| UI Label                         | Setting                              | Description                                                                                                                                                                    | Default     |
| -------------------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------- |
| Enable Interactive Shell         | `tools.shell.enableInteractiveShell` | Use node-pty for an interactive shell experience. Fallback to child_process still applies.                                                                                     | `true`      |
| Show Color                       | `tools.shell.showColor`              | Show color in shell output.                                                                                                                                                    | `false`     |
| Auto Accept                      | `tools.autoAccept`                   | Automatically accept and execute tool calls that are considered safe (e.g., read-only operations).                                                                             | `false`     |
| Approval Mode                    | `tools.approvalMode`                 | The default approval mode for tool execution. 'default' prompts for approval, 'auto_edit' auto-approves edit tools, and 'plan' is read-only mode. 'yolo' is not supported yet. | `"default"` |
| Use Ripgrep                      | `tools.useRipgrep`                   | Use ripgrep for file content search instead of the fallback implementation. Provides faster search performance.                                                                | `true`      |
| Enable Tool Output Truncation    | `tools.enableToolOutputTruncation`   | Enable truncation of large tool outputs.                                                                                                                                       | `true`      |
| Tool Output Truncation Threshold | `tools.truncateToolOutputThreshold`  | Truncate tool output if it is larger than this many characters. Set to -1 to disable.                                                                                          | `4000000`   |
| Tool Output Truncation Lines     | `tools.truncateToolOutputLines`      | The number of lines to keep when truncating tool output.                                                                                                                       | `1000`      |
| Disable LLM Correction           | `tools.disableLLMCorrection`         | Disable LLM-based error correction for edit tools. When enabled, tools will fail immediately if exact string matches are not found, instead of attempting to self-correct.     | `true`      |

### Security

| UI Label                              | Setting                                         | Description                                                                     | Default |
| ------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------------------- | ------- |
| Disable YOLO Mode                     | `security.disableYoloMode`                      | Disable YOLO mode, even if enabled by a flag.                                   | `false` |
| Allow Permanent Tool Approval         | `security.enablePermanentToolApproval`          | Enable the "Allow for all future sessions" option in tool confirmation dialogs. | `false` |
| Blocks extensions from Git            | `security.blockGitExtensions`                   | Blocks installing and loading extensions from Git.                              | `false` |
| Folder Trust                          | `security.folderTrust.enabled`                  | Setting to track whether Folder trust is enabled.                               | `false` |
| Enable Environment Variable Redaction | `security.environmentVariableRedaction.enabled` | Enable redaction of environment variables that may contain secrets.             | `false` |

### Experimental

| UI Label         | Setting                      | Description                                                                         | Default |
| ---------------- | ---------------------------- | ----------------------------------------------------------------------------------- | ------- |
| Agent Skills     | `experimental.skills`        | Enable Agent Skills (experimental).                                                 | `false` |
| Use OSC 52 Paste | `experimental.useOSC52Paste` | Use OSC 52 sequence for pasting instead of clipboardy (useful for remote sessions). | `false` |
| Plan             | `experimental.plan`          | Enable planning features (Plan Mode and tools).                                     | `false` |

### HooksConfig

| UI Label           | Setting                     | Description                                      | Default |
| ------------------ | --------------------------- | ------------------------------------------------ | ------- |
| Hook Notifications | `hooksConfig.notifications` | Show visual indicators when hooks are executing. | `true`  |

<!-- SETTINGS-AUTOGEN:END -->


---


# Observability with OpenTelemetry

Learn how to enable and setup OpenTelemetry for Gemini CLI.

- [Observability with OpenTelemetry](#observability-with-opentelemetry)
  - [Key benefits](#key-benefits)
  - [OpenTelemetry integration](#opentelemetry-integration)
  - [Configuration](#configuration)
  - [Google Cloud telemetry](#google-cloud-telemetry)
    - [Prerequisites](#prerequisites)
    - [Authenticating with CLI Credentials](#authenticating-with-cli-credentials)
    - [Direct export (recommended)](#direct-export-recommended)
    - [Collector-based export (advanced)](#collector-based-export-advanced)
    - [Monitoring Dashboards](#monitoring-dashboards)
  - [Local telemetry](#local-telemetry)
    - [File-based output (recommended)](#file-based-output-recommended)
    - [Collector-based export (advanced)](#collector-based-export-advanced-1)
  - [Logs and metrics](#logs-and-metrics)
    - [Logs](#logs)
      - [Sessions](#sessions)
      - [Approval Mode](#approval-mode)
      - [Tools](#tools)
      - [Files](#files)
      - [API](#api)
      - [Model routing](#model-routing)
      - [Chat and streaming](#chat-and-streaming)
      - [Resilience](#resilience)
      - [Extensions](#extensions)
      - [Agent runs](#agent-runs)
      - [IDE](#ide)
      - [UI](#ui)
    - [Metrics](#metrics)
      - [Custom](#custom)
        - [Sessions](#sessions-1)
        - [Tools](#tools-1)
        - [API](#api-1)
        - [Token usage](#token-usage)
        - [Files](#files-1)
        - [Chat and streaming](#chat-and-streaming-1)
        - [Model routing](#model-routing-1)
        - [Agent runs](#agent-runs-1)
        - [UI](#ui-1)
        - [Performance](#performance)
      - [GenAI semantic convention](#genai-semantic-convention)

## Key benefits

- **🔍 Usage analytics**: Understand interaction patterns and feature adoption
  across your team
- **⚡ Performance monitoring**: Track response times, token consumption, and
  resource utilization
- **🐛 Real-time debugging**: Identify bottlenecks, failures, and error patterns
  as they occur
- **📊 Workflow optimization**: Make informed decisions to improve
  configurations and processes
- **🏢 Enterprise governance**: Monitor usage across teams, track costs, ensure
  compliance, and integrate with existing monitoring infrastructure

## OpenTelemetry integration

Built on **[OpenTelemetry]** — the vendor-neutral, industry-standard
observability framework — Gemini CLI's observability system provides:

- **Universal compatibility**: Export to any OpenTelemetry backend (Google
  Cloud, Jaeger, Prometheus, Datadog, etc.)
- **Standardized data**: Use consistent formats and collection methods across
  your toolchain
- **Future-proof integration**: Connect with existing and future observability
  infrastructure
- **No vendor lock-in**: Switch between backends without changing your
  instrumentation

[OpenTelemetry]: https://opentelemetry.io/

## Configuration

All telemetry behavior is controlled through your `.gemini/settings.json` file.
Environment variables can be used to override the settings in the file.

| Setting        | Environment Variable             | Description                                         | Values            | Default                 |
| -------------- | -------------------------------- | --------------------------------------------------- | ----------------- | ----------------------- |
| `enabled`      | `GEMINI_TELEMETRY_ENABLED`       | Enable or disable telemetry                         | `true`/`false`    | `false`                 |
| `target`       | `GEMINI_TELEMETRY_TARGET`        | Where to send telemetry data                        | `"gcp"`/`"local"` | `"local"`               |
| `otlpEndpoint` | `GEMINI_TELEMETRY_OTLP_ENDPOINT` | OTLP collector endpoint                             | URL string        | `http://localhost:4317` |
| `otlpProtocol` | `GEMINI_TELEMETRY_OTLP_PROTOCOL` | OTLP transport protocol                             | `"grpc"`/`"http"` | `"grpc"`                |
| `outfile`      | `GEMINI_TELEMETRY_OUTFILE`       | Save telemetry to file (overrides `otlpEndpoint`)   | file path         | -                       |
| `logPrompts`   | `GEMINI_TELEMETRY_LOG_PROMPTS`   | Include prompts in telemetry logs                   | `true`/`false`    | `true`                  |
| `useCollector` | `GEMINI_TELEMETRY_USE_COLLECTOR` | Use external OTLP collector (advanced)              | `true`/`false`    | `false`                 |
| `useCliAuth`   | `GEMINI_TELEMETRY_USE_CLI_AUTH`  | Use CLI credentials for telemetry (GCP target only) | `true`/`false`    | `false`                 |

**Note on boolean environment variables:** For the boolean settings (`enabled`,
`logPrompts`, `useCollector`), setting the corresponding environment variable to
`true` or `1` will enable the feature. Any other value will disable it.

For detailed information about all configuration options, see the
[Configuration guide](/docs/get-started/configuration).

## Google Cloud telemetry

### Prerequisites

Before using either method below, complete these steps:

1. Set your Google Cloud project ID:
   - For telemetry in a separate project from inference:
     ```bash
     export OTLP_GOOGLE_CLOUD_PROJECT="your-telemetry-project-id"
     ```
   - For telemetry in the same project as inference:
     ```bash
     export GOOGLE_CLOUD_PROJECT="your-project-id"
     ```

2. Authenticate with Google Cloud:
   - If using a user account:
     ```bash
     gcloud auth application-default login
     ```
   - If using a service account:
     ```bash
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account.json"
     ```
3. Make sure your account or service account has these IAM roles:
   - Cloud Trace Agent
   - Monitoring Metric Writer
   - Logs Writer

4. Enable the required Google Cloud APIs (if not already enabled):
   ```bash
   gcloud services enable \
     cloudtrace.googleapis.com \
     monitoring.googleapis.com \
     logging.googleapis.com \
     --project="$OTLP_GOOGLE_CLOUD_PROJECT"
   ```

### Authenticating with CLI Credentials

By default, the telemetry collector for Google Cloud uses Application Default
Credentials (ADC). However, you can configure it to use the same OAuth
credentials that you use to log in to the Gemini CLI. This is useful in
environments where you don't have ADC set up.

To enable this, set the `useCliAuth` property in your `telemetry` settings to
`true`:

```json
{
  "telemetry": {
    "enabled": true,
    "target": "gcp",
    "useCliAuth": true
  }
}
```

**Important:**

- This setting requires the use of **Direct Export** (in-process exporters).
- It **cannot** be used with `useCollector: true`. If you enable both, telemetry
  will be disabled and an error will be logged.
- The CLI will automatically use your credentials to authenticate with Google
  Cloud Trace, Metrics, and Logging APIs.

### Direct export (recommended)

Sends telemetry directly to Google Cloud services. No collector needed.

1. Enable telemetry in your `.gemini/settings.json`:
   ```json
   {
     "telemetry": {
       "enabled": true,
       "target": "gcp"
     }
   }
   ```
2. Run Gemini CLI and send prompts.
3. View logs and metrics:
   - Open the Google Cloud Console in your browser after sending prompts:
     - Logs: https://console.cloud.google.com/logs/
     - Metrics: https://console.cloud.google.com/monitoring/metrics-explorer
     - Traces: https://console.cloud.google.com/traces/list

### Collector-based export (advanced)

For custom processing, filtering, or routing, use an OpenTelemetry collector to
forward data to Google Cloud.

1. Configure your `.gemini/settings.json`:
   ```json
   {
     "telemetry": {
       "enabled": true,
       "target": "gcp",
       "useCollector": true
     }
   }
   ```
2. Run the automation script:
   ```bash
   npm run telemetry -- --target=gcp
   ```
   This will:
   - Start a local OTEL collector that forwards to Google Cloud
   - Configure your workspace
   - Provide links to view traces, metrics, and logs in Google Cloud Console
   - Save collector logs to `~/.gemini/tmp/<projectHash>/otel/collector-gcp.log`
   - Stop collector on exit (e.g. `Ctrl+C`)
3. Run Gemini CLI and send prompts.
4. View logs and metrics:
   - Open the Google Cloud Console in your browser after sending prompts:
     - Logs: https://console.cloud.google.com/logs/
     - Metrics: https://console.cloud.google.com/monitoring/metrics-explorer
     - Traces: https://console.cloud.google.com/traces/list
   - Open `~/.gemini/tmp/<projectHash>/otel/collector-gcp.log` to view local
     collector logs.

### Monitoring Dashboards

Gemini CLI provides a pre-configured
[Google Cloud Monitoring](https://cloud.google.com/monitoring) dashboard to
visualize your telemetry.

This dashboard can be found under **Google Cloud Monitoring Dashboard
Templates** as "**Gemini CLI Monitoring**".

![Gemini CLI Monitoring Dashboard Overview](/docs/assets/monitoring-dashboard-overview.png)

![Gemini CLI Monitoring Dashboard Metrics](/docs/assets/monitoring-dashboard-metrics.png)

![Gemini CLI Monitoring Dashboard Logs](/docs/assets/monitoring-dashboard-logs.png)

To learn more, check out this blog post:
[Instant insights: Gemini CLI’s new pre-configured monitoring dashboards](https://cloud.google.com/blog/topics/developers-practitioners/instant-insights-gemini-clis-new-pre-configured-monitoring-dashboards/).

## Local telemetry

For local development and debugging, you can capture telemetry data locally:

### File-based output (recommended)

1. Enable telemetry in your `.gemini/settings.json`:
   ```json
   {
     "telemetry": {
       "enabled": true,
       "target": "local",
       "otlpEndpoint": "",
       "outfile": ".gemini/telemetry.log"
     }
   }
   ```
2. Run Gemini CLI and send prompts.
3. View logs and metrics in the specified file (e.g., `.gemini/telemetry.log`).

### Collector-based export (advanced)

1. Run the automation script:
   ```bash
   npm run telemetry -- --target=local
   ```
   This will:
   - Download and start Jaeger and OTEL collector
   - Configure your workspace for local telemetry
   - Provide a Jaeger UI at http://localhost:16686
   - Save logs/metrics to `~/.gemini/tmp/<projectHash>/otel/collector.log`
   - Stop collector on exit (e.g. `Ctrl+C`)
2. Run Gemini CLI and send prompts.
3. View traces at http://localhost:16686 and logs/metrics in the collector log
   file.

## Logs and metrics

The following section describes the structure of logs and metrics generated for
Gemini CLI.

The `session.id`, `installation.id`, and `user.email` (available only when
authenticated with a Google account) are included as common attributes on all
logs and metrics.

### Logs

Logs are timestamped records of specific events. The following events are logged
for Gemini CLI, grouped by category.

#### Sessions

Captures startup configuration and user prompt submissions.

- `gemini_cli.config`: Emitted once at startup with the CLI configuration.
  - **Attributes**:
    - `model` (string)
    - `embedding_model` (string)
    - `sandbox_enabled` (boolean)
    - `core_tools_enabled` (string)
    - `approval_mode` (string)
    - `api_key_enabled` (boolean)
    - `vertex_ai_enabled` (boolean)
    - `log_user_prompts_enabled` (boolean)
    - `file_filtering_respect_git_ignore` (boolean)
    - `debug_mode` (boolean)
    - `mcp_servers` (string)
    - `mcp_servers_count` (int)
    - `extensions` (string)
    - `extension_ids` (string)
    - `extension_count` (int)
    - `mcp_tools` (string, if applicable)
    - `mcp_tools_count` (int, if applicable)
    - `output_format` ("text", "json", or "stream-json")

- `gemini_cli.user_prompt`: Emitted when a user submits a prompt.
  - **Attributes**:
    - `prompt_length` (int)
    - `prompt_id` (string)
    - `prompt` (string; excluded if `telemetry.logPrompts` is `false`)
    - `auth_type` (string)

#### Approval Mode

Tracks changes and duration of approval modes.

- `approval_mode_switch`: Approval mode was changed.
  - **Attributes**:
    - `from_mode` (string)
    - `to_mode` (string)

- `approval_mode_duration`: Duration spent in an approval mode.
  - **Attributes**:
    - `mode` (string)
    - `duration_ms` (int)

#### Tools

Captures tool executions, output truncation, and Edit behavior.

- `gemini_cli.tool_call`: Emitted for each tool (function) call.
  - **Attributes**:
    - `function_name`
    - `function_args`
    - `duration_ms`
    - `success` (boolean)
    - `decision` ("accept", "reject", "auto_accept", or "modify", if applicable)
    - `error` (if applicable)
    - `error_type` (if applicable)
    - `prompt_id` (string)
    - `tool_type` ("native" or "mcp")
    - `mcp_server_name` (string, if applicable)
    - `extension_name` (string, if applicable)
    - `extension_id` (string, if applicable)
    - `content_length` (int, if applicable)
    - `metadata` (if applicable)

- `gemini_cli.tool_output_truncated`: Output of a tool call was truncated.
  - **Attributes**:
    - `tool_name` (string)
    - `original_content_length` (int)
    - `truncated_content_length` (int)
    - `threshold` (int)
    - `lines` (int)
    - `prompt_id` (string)

- `gemini_cli.edit_strategy`: Edit strategy chosen.
  - **Attributes**:
    - `strategy` (string)

- `gemini_cli.edit_correction`: Edit correction result.
  - **Attributes**:
    - `correction` ("success" | "failure")

- `gen_ai.client.inference.operation.details`: This event provides detailed
  information about the GenAI operation, aligned with [OpenTelemetry GenAI
  semantic conventions for events].
  - **Attributes**:
    - `gen_ai.request.model` (string)
    - `gen_ai.provider.name` (string)
    - `gen_ai.operation.name` (string)
    - `gen_ai.input.messages` (json string)
    - `gen_ai.output.messages` (json string)
    - `gen_ai.response.finish_reasons` (array of strings)
    - `gen_ai.usage.input_tokens` (int)
    - `gen_ai.usage.output_tokens` (int)
    - `gen_ai.request.temperature` (float)
    - `gen_ai.request.top_p` (float)
    - `gen_ai.request.top_k` (int)
    - `gen_ai.request.max_tokens` (int)
    - `gen_ai.system_instructions` (json string)
    - `server.address` (string)
    - `server.port` (int)

#### Files

Tracks file operations performed by tools.

- `gemini_cli.file_operation`: Emitted for each file operation.
  - **Attributes**:
    - `tool_name` (string)
    - `operation` ("create" | "read" | "update")
    - `lines` (int, optional)
    - `mimetype` (string, optional)
    - `extension` (string, optional)
    - `programming_language` (string, optional)

#### API

Captures Gemini API requests, responses, and errors.

- `gemini_cli.api_request`: Request sent to Gemini API.
  - **Attributes**:
    - `model` (string)
    - `prompt_id` (string)
    - `request_text` (string, optional)

- `gemini_cli.api_response`: Response received from Gemini API.
  - **Attributes**:
    - `model` (string)
    - `status_code` (int|string)
    - `duration_ms` (int)
    - `input_token_count` (int)
    - `output_token_count` (int)
    - `cached_content_token_count` (int)
    - `thoughts_token_count` (int)
    - `tool_token_count` (int)
    - `total_token_count` (int)
    - `response_text` (string, optional)
    - `prompt_id` (string)
    - `auth_type` (string)
    - `finish_reasons` (array of strings)

- `gemini_cli.api_error`: API request failed.
  - **Attributes**:
    - `model` (string)
    - `error` (string)
    - `error_type` (string)
    - `status_code` (int|string)
    - `duration_ms` (int)
    - `prompt_id` (string)
    - `auth_type` (string)

- `gemini_cli.malformed_json_response`: `generateJson` response could not be
  parsed.
  - **Attributes**:
    - `model` (string)

#### Model routing

- `gemini_cli.slash_command`: A slash command was executed.
  - **Attributes**:
    - `command` (string)
    - `subcommand` (string, optional)
    - `status` ("success" | "error")

- `gemini_cli.slash_command.model`: Model was selected via slash command.
  - **Attributes**:
    - `model_name` (string)

- `gemini_cli.model_routing`: Model router made a decision.
  - **Attributes**:
    - `decision_model` (string)
    - `decision_source` (string)
    - `routing_latency_ms` (int)
    - `reasoning` (string, optional)
    - `failed` (boolean)
    - `error_message` (string, optional)

#### Chat and streaming

- `gemini_cli.chat_compression`: Chat context was compressed.
  - **Attributes**:
    - `tokens_before` (int)
    - `tokens_after` (int)

- `gemini_cli.chat.invalid_chunk`: Invalid chunk received from a stream.
  - **Attributes**:
    - `error.message` (string, optional)

- `gemini_cli.chat.content_retry`: Retry triggered due to a content error.
  - **Attributes**:
    - `attempt_number` (int)
    - `error_type` (string)
    - `retry_delay_ms` (int)
    - `model` (string)

- `gemini_cli.chat.content_retry_failure`: All content retries failed.
  - **Attributes**:
    - `total_attempts` (int)
    - `final_error_type` (string)
    - `total_duration_ms` (int, optional)
    - `model` (string)

- `gemini_cli.conversation_finished`: Conversation session ended.
  - **Attributes**:
    - `approvalMode` (string)
    - `turnCount` (int)

- `gemini_cli.next_speaker_check`: Next speaker determination.
  - **Attributes**:
    - `prompt_id` (string)
    - `finish_reason` (string)
    - `result` (string)

#### Resilience

Records fallback mechanisms for models and network operations.

- `gemini_cli.flash_fallback`: Switched to a flash model as fallback.
  - **Attributes**:
    - `auth_type` (string)

- `gemini_cli.ripgrep_fallback`: Switched to grep as fallback for file search.
  - **Attributes**:
    - `error` (string, optional)

- `gemini_cli.web_fetch_fallback_attempt`: Attempted web-fetch fallback.
  - **Attributes**:
    - `reason` ("private_ip" | "primary_failed")

#### Extensions

Tracks extension lifecycle and settings changes.

- `gemini_cli.extension_install`: An extension was installed.
  - **Attributes**:
    - `extension_name` (string)
    - `extension_version` (string)
    - `extension_source` (string)
    - `status` (string)

- `gemini_cli.extension_uninstall`: An extension was uninstalled.
  - **Attributes**:
    - `extension_name` (string)
    - `status` (string)

- `gemini_cli.extension_enable`: An extension was enabled.
  - **Attributes**:
    - `extension_name` (string)
    - `setting_scope` (string)

- `gemini_cli.extension_disable`: An extension was disabled.
  - **Attributes**:
    - `extension_name` (string)
    - `setting_scope` (string)

- `gemini_cli.extension_update`: An extension was updated.
  - **Attributes**:
    - `extension_name` (string)
    - `extension_version` (string)
    - `extension_previous_version` (string)
    - `extension_source` (string)
    - `status` (string)

#### Agent runs

- `gemini_cli.agent.start`: Agent run started.
  - **Attributes**:
    - `agent_id` (string)
    - `agent_name` (string)

- `gemini_cli.agent.finish`: Agent run finished.
  - **Attributes**:
    - `agent_id` (string)
    - `agent_name` (string)
    - `duration_ms` (int)
    - `turn_count` (int)
    - `terminate_reason` (string)

#### IDE

Captures IDE connectivity and conversation lifecycle events.

- `gemini_cli.ide_connection`: IDE companion connection.
  - **Attributes**:
    - `connection_type` (string)

#### UI

Tracks terminal rendering issues and related signals.

- `kitty_sequence_overflow`: Terminal kitty control sequence overflow.
  - **Attributes**:
    - `sequence_length` (int)
    - `truncated_sequence` (string)

### Metrics

Metrics are numerical measurements of behavior over time.

#### Custom

##### Sessions

Counts CLI sessions at startup.

- `gemini_cli.session.count` (Counter, Int): Incremented once per CLI startup.

##### Tools

Measures tool usage and latency.

- `gemini_cli.tool.call.count` (Counter, Int): Counts tool calls.
  - **Attributes**:
    - `function_name`
    - `success` (boolean)
    - `decision` (string: "accept", "reject", "modify", or "auto_accept", if
      applicable)
    - `tool_type` (string: "mcp" or "native", if applicable)

- `gemini_cli.tool.call.latency` (Histogram, ms): Measures tool call latency.
  - **Attributes**:
    - `function_name`

##### API

Tracks API request volume and latency.

- `gemini_cli.api.request.count` (Counter, Int): Counts all API requests.
  - **Attributes**:
    - `model`
    - `status_code`
    - `error_type` (if applicable)

- `gemini_cli.api.request.latency` (Histogram, ms): Measures API request
  latency.
  - **Attributes**:
    - `model`
  - Note: Overlaps with `gen_ai.client.operation.duration` (GenAI conventions).

##### Token usage

Tracks tokens used by model and type.

- `gemini_cli.token.usage` (Counter, Int): Counts tokens used.
  - **Attributes**:
    - `model`
    - `type` ("input", "output", "thought", "cache", or "tool")
  - Note: Overlaps with `gen_ai.client.token.usage` for `input`/`output`.

##### Files

Counts file operations with basic context.

- `gemini_cli.file.operation.count` (Counter, Int): Counts file operations.
  - **Attributes**:
    - `operation` ("create", "read", "update")
    - `lines` (Int, optional)
    - `mimetype` (string, optional)
    - `extension` (string, optional)
    - `programming_language` (string, optional)

- `gemini_cli.lines.changed` (Counter, Int): Number of lines changed (from file
  diffs).
  - **Attributes**:
    - `function_name`
    - `type` ("added" or "removed")

##### Chat and streaming

Resilience counters for compression, invalid chunks, and retries.

- `gemini_cli.chat_compression` (Counter, Int): Counts chat compression
  operations.
  - **Attributes**:
    - `tokens_before` (Int)
    - `tokens_after` (Int)

- `gemini_cli.chat.invalid_chunk.count` (Counter, Int): Counts invalid chunks
  from streams.

- `gemini_cli.chat.content_retry.count` (Counter, Int): Counts retries due to
  content errors.

- `gemini_cli.chat.content_retry_failure.count` (Counter, Int): Counts requests
  where all content retries failed.

##### Model routing

Routing latency/failures and slash-command selections.

- `gemini_cli.slash_command.model.call_count` (Counter, Int): Counts model
  selections via slash command.
  - **Attributes**:
    - `slash_command.model.model_name` (string)

- `gemini_cli.model_routing.latency` (Histogram, ms): Model routing decision
  latency.
  - **Attributes**:
    - `routing.decision_model` (string)
    - `routing.decision_source` (string)

- `gemini_cli.model_routing.failure.count` (Counter, Int): Counts model routing
  failures.
  - **Attributes**:
    - `routing.decision_source` (string)
    - `routing.error_message` (string)

##### Agent runs

Agent lifecycle metrics: runs, durations, and turns.

- `gemini_cli.agent.run.count` (Counter, Int): Counts agent runs.
  - **Attributes**:
    - `agent_name` (string)
    - `terminate_reason` (string)

- `gemini_cli.agent.duration` (Histogram, ms): Agent run durations.
  - **Attributes**:
    - `agent_name` (string)

- `gemini_cli.agent.turns` (Histogram, turns): Turns taken per agent run.
  - **Attributes**:
    - `agent_name` (string)

##### UI

UI stability signals such as flicker count.

- `gemini_cli.ui.flicker.count` (Counter, Int): Counts UI frames that flicker
  (render taller than terminal).

##### Performance

Optional performance monitoring for startup, CPU/memory, and phase timing.

- `gemini_cli.startup.duration` (Histogram, ms): CLI startup time by phase.
  - **Attributes**:
    - `phase` (string)
    - `details` (map, optional)

- `gemini_cli.memory.usage` (Histogram, bytes): Memory usage.
  - **Attributes**:
    - `memory_type` ("heap_used", "heap_total", "external", "rss")
    - `component` (string, optional)

- `gemini_cli.cpu.usage` (Histogram, percent): CPU usage percentage.
  - **Attributes**:
    - `component` (string, optional)

- `gemini_cli.tool.queue.depth` (Histogram, count): Number of tools in the
  execution queue.

- `gemini_cli.tool.execution.breakdown` (Histogram, ms): Tool time by phase.
  - **Attributes**:
    - `function_name` (string)
    - `phase` ("validation", "preparation", "execution", "result_processing")

- `gemini_cli.api.request.breakdown` (Histogram, ms): API request time by phase.
  - **Attributes**:
    - `model` (string)
    - `phase` ("request_preparation", "network_latency", "response_processing",
      "token_processing")

- `gemini_cli.token.efficiency` (Histogram, ratio): Token efficiency metrics.
  - **Attributes**:
    - `model` (string)
    - `metric` (string)
    - `context` (string, optional)

- `gemini_cli.performance.score` (Histogram, score): Composite performance
  score.
  - **Attributes**:
    - `category` (string)
    - `baseline` (number, optional)

- `gemini_cli.performance.regression` (Counter, Int): Regression detection
  events.
  - **Attributes**:
    - `metric` (string)
    - `severity` ("low", "medium", "high")
    - `current_value` (number)
    - `baseline_value` (number)

- `gemini_cli.performance.regression.percentage_change` (Histogram, percent):
  Percent change from baseline when regression detected.
  - **Attributes**:
    - `metric` (string)
    - `severity` ("low", "medium", "high")
    - `current_value` (number)
    - `baseline_value` (number)

- `gemini_cli.performance.baseline.comparison` (Histogram, percent): Comparison
  to baseline.
  - **Attributes**:
    - `metric` (string)
    - `category` (string)
    - `current_value` (number)
    - `baseline_value` (number)

#### GenAI semantic convention

The following metrics comply with [OpenTelemetry GenAI semantic conventions] for
standardized observability across GenAI applications:

- `gen_ai.client.token.usage` (Histogram, token): Number of input and output
  tokens used per operation.
  - **Attributes**:
    - `gen_ai.operation.name` (string): The operation type (e.g.,
      "generate_content", "chat")
    - `gen_ai.provider.name` (string): The GenAI provider ("gcp.gen_ai" or
      "gcp.vertex_ai")
    - `gen_ai.token.type` (string): The token type ("input" or "output")
    - `gen_ai.request.model` (string, optional): The model name used for the
      request
    - `gen_ai.response.model` (string, optional): The model name that generated
      the response
    - `server.address` (string, optional): GenAI server address
    - `server.port` (int, optional): GenAI server port

- `gen_ai.client.operation.duration` (Histogram, s): GenAI operation duration in
  seconds.
  - **Attributes**:
    - `gen_ai.operation.name` (string): The operation type (e.g.,
      "generate_content", "chat")
    - `gen_ai.provider.name` (string): The GenAI provider ("gcp.gen_ai" or
      "gcp.vertex_ai")
    - `gen_ai.request.model` (string, optional): The model name used for the
      request
    - `gen_ai.response.model` (string, optional): The model name that generated
      the response
    - `server.address` (string, optional): GenAI server address
    - `server.port` (int, optional): GenAI server port
    - `error.type` (string, optional): Error type if the operation failed

[OpenTelemetry GenAI semantic conventions]:
  https://github.com/open-telemetry/semantic-conventions/blob/main/docs/gen-ai/gen-ai-metrics.md
[OpenTelemetry GenAI semantic conventions for events]:
  https://github.com/open-telemetry/semantic-conventions/blob/8b4f210f43136e57c1f6f47292eb6d38e3bf30bb/docs/gen-ai/gen-ai-events.md
  

---


# Themes

Gemini CLI supports a variety of themes to customize its color scheme and
appearance. You can change the theme to suit your preferences via the `/theme`
command or `"theme":` configuration setting.

## Available themes

Gemini CLI comes with a selection of pre-defined themes, which you can list
using the `/theme` command within Gemini CLI:

- **Dark themes:**
  - `ANSI`
  - `Atom One`
  - `Ayu`
  - `Default`
  - `Dracula`
  - `GitHub`
- **Light themes:**
  - `ANSI Light`
  - `Ayu Light`
  - `Default Light`
  - `GitHub Light`
  - `Google Code`
  - `Xcode`

### Changing themes

1.  Enter `/theme` into Gemini CLI.
2.  A dialog or selection prompt appears, listing the available themes.
3.  Using the arrow keys, select a theme. Some interfaces might offer a live
    preview or highlight as you select.
4.  Confirm your selection to apply the theme.

**Note:** If a theme is defined in your `settings.json` file (either by name or
by a file path), you must remove the `"theme"` setting from the file before you
can change the theme using the `/theme` command.

### Theme persistence

Selected themes are saved in Gemini CLI's
[configuration](/docs/get-started/configuration) so your preference is
remembered across sessions.

---

## Custom color themes

Gemini CLI allows you to create your own custom color themes by specifying them
in your `settings.json` file. This gives you full control over the color palette
used in the CLI.

### How to define a custom theme

Add a `customThemes` block to your user, project, or system `settings.json`
file. Each custom theme is defined as an object with a unique name and a set of
color keys. For example:

```json
{
  "ui": {
    "customThemes": {
      "MyCustomTheme": {
        "name": "MyCustomTheme",
        "type": "custom",
        "Background": "#181818",
        ...
      }
    }
  }
}
```

**Color keys:**

- `Background`
- `Foreground`
- `LightBlue`
- `AccentBlue`
- `AccentPurple`
- `AccentCyan`
- `AccentGreen`
- `AccentYellow`
- `AccentRed`
- `Comment`
- `Gray`
- `DiffAdded` (optional, for added lines in diffs)
- `DiffRemoved` (optional, for removed lines in diffs)

You can also override individual UI text roles by adding a nested `text` object.
This object supports the keys `primary`, `secondary`, `link`, `accent`, and
`response`. When `text.response` is provided it takes precedence over
`text.primary` for rendering model responses in chat.

**Required properties:**

- `name` (must match the key in the `customThemes` object and be a string)
- `type` (must be the string `"custom"`)
- `Background`
- `Foreground`
- `LightBlue`
- `AccentBlue`
- `AccentPurple`
- `AccentCyan`
- `AccentGreen`
- `AccentYellow`
- `AccentRed`
- `Comment`
- `Gray`

You can use either hex codes (e.g., `#FF0000`) **or** standard CSS color names
(e.g., `coral`, `teal`, `blue`) for any color value. See
[CSS color names](https://developer.mozilla.org/en-US/docs/Web/CSS/color_value#color_keywords)
for a full list of supported names.

You can define multiple custom themes by adding more entries to the
`customThemes` object.

### Loading themes from a file

In addition to defining custom themes in `settings.json`, you can also load a
theme directly from a JSON file by specifying the file path in your
`settings.json`. This is useful for sharing themes or keeping them separate from
your main configuration.

To load a theme from a file, set the `theme` property in your `settings.json` to
the path of your theme file:

```json
{
  "ui": {
    "theme": "/path/to/your/theme.json"
  }
}
```

The theme file must be a valid JSON file that follows the same structure as a
custom theme defined in `settings.json`.

**Example `my-theme.json`:**

```json
{
  "name": "My File Theme",
  "type": "custom",
  "Background": "#282A36",
  "Foreground": "#F8F8F2",
  "LightBlue": "#82AAFF",
  "AccentBlue": "#61AFEF",
  "AccentPurple": "#BD93F9",
  "AccentCyan": "#8BE9FD",
  "AccentGreen": "#50FA7B",
  "AccentYellow": "#F1FA8C",
  "AccentRed": "#FF5555",
  "Comment": "#6272A4",
  "Gray": "#ABB2BF",
  "DiffAdded": "#A6E3A1",
  "DiffRemoved": "#F38BA8",
  "GradientColors": ["#4796E4", "#847ACE", "#C3677F"]
}
```

**Security note:** For your safety, Gemini CLI will only load theme files that
are located within your home directory. If you attempt to load a theme from
outside your home directory, a warning will be displayed and the theme will not
be loaded. This is to prevent loading potentially malicious theme files from
untrusted sources.

### Example custom theme

<img src="../assets/theme-custom.png" alt="Custom theme example" width="600" />

### Using your custom theme

- Select your custom theme using the `/theme` command in Gemini CLI. Your custom
  theme will appear in the theme selection dialog.
- Or, set it as the default by adding `"theme": "MyCustomTheme"` to the `ui`
  object in your `settings.json`.
- Custom themes can be set at the user, project, or system level, and follow the
  same [configuration precedence](/docs/get-started/configuration) as other
  settings.

---

## Dark themes

### ANSI

<img src="/assets/theme-ansi.png" alt="ANSI theme" width="600" />

### Atom OneDark

<img src="/assets/theme-atom-one.png" alt="Atom One theme" width="600">

### Ayu

<img src="/assets/theme-ayu.png" alt="Ayu theme" width="600">

### Default

<img src="/assets/theme-default.png" alt="Default theme" width="600">

### Dracula

<img src="/assets/theme-dracula.png" alt="Dracula theme" width="600">

### GitHub

<img src="/assets/theme-github.png" alt="GitHub theme" width="600">

## Light themes

### ANSI Light

<img src="/assets/theme-ansi-light.png" alt="ANSI Light theme" width="600">

### Ayu Light

<img src="/assets/theme-ayu-light.png" alt="Ayu Light theme" width="600">

### Default Light

<img src="/assets/theme-default-light.png" alt="Default Light theme" width="600">

### GitHub Light

<img src="/assets/theme-github-light.png" alt="GitHub Light theme" width="600">

### Google Code

<img src="/assets/theme-google-light.png" alt="Google Code theme" width="600">

### Xcode

<img src="/assets/theme-xcode-light.png" alt="Xcode Light theme" width="600">



---


# Token caching and cost optimization

Gemini CLI automatically optimizes API costs through token caching when using
API key authentication (Gemini API key or Vertex AI). This feature reuses
previous system instructions and context to reduce the number of tokens
processed in subsequent requests.

**Token caching is available for:**

- API key users (Gemini API key)
- Vertex AI users (with project and location setup)

**Token caching is not available for:**

- OAuth users (Google Personal/Enterprise accounts) - the Code Assist API does
  not support cached content creation at this time

You can view your token usage and cached token savings using the `/stats`
command. When cached tokens are available, they will be displayed in the stats
output.


---


# Trusted Folders

The Trusted Folders feature is a security setting that gives you control over
which projects can use the full capabilities of the Gemini CLI. It prevents
potentially malicious code from running by asking you to approve a folder before
the CLI loads any project-specific configurations from it.

## Enabling the feature

The Trusted Folders feature is **disabled by default**. To use it, you must
first enable it in your settings.

Add the following to your user `settings.json` file:

```json
{
  "security": {
    "folderTrust": {
      "enabled": true
    }
  }
}
```

## How it works: The trust dialog

Once the feature is enabled, the first time you run the Gemini CLI from a
folder, a dialog will automatically appear, prompting you to make a choice:

- **Trust folder**: Grants full trust to the current folder (e.g.,
  `my-project`).
- **Trust parent folder**: Grants trust to the parent directory (e.g.,
  `safe-projects`), which automatically trusts all of its subdirectories as
  well. This is useful if you keep all your safe projects in one place.
- **Don't trust**: Marks the folder as untrusted. The CLI will operate in a
  restricted "safe mode."

Your choice is saved in a central file (`~/.gemini/trustedFolders.json`), so you
will only be asked once per folder.

## Why trust matters: The impact of an untrusted workspace

When a folder is **untrusted**, the Gemini CLI runs in a restricted "safe mode"
to protect you. In this mode, the following features are disabled:

1.  **Workspace settings are ignored**: The CLI will **not** load the
    `.gemini/settings.json` file from the project. This prevents the loading of
    custom tools and other potentially dangerous configurations.

2.  **Environment variables are ignored**: The CLI will **not** load any `.env`
    files from the project.

3.  **Extension management is restricted**: You **cannot install, update, or
    uninstall** extensions.

4.  **Tool auto-acceptance is disabled**: You will always be prompted before any
    tool is run, even if you have auto-acceptance enabled globally.

5.  **Automatic memory loading is disabled**: The CLI will not automatically
    load files into context from directories specified in local settings.

6.  **MCP servers do not connect**: The CLI will not attempt to connect to any
    [Model Context Protocol (MCP)](/docs/tools/mcp-server) servers.

7.  **Custom commands are not loaded**: The CLI will not load any custom
    commands from .toml files, including both project-specific and global user
    commands.

Granting trust to a folder unlocks the full functionality of the Gemini CLI for
that workspace.

## Managing your trust settings

If you need to change a decision or see all your settings, you have a couple of
options:

- **Change the current folder's trust**: Run the `/permissions` command from
  within the CLI. This will bring up the same interactive dialog, allowing you
  to change the trust level for the current folder.

- **View all trust rules**: To see a complete list of all your trusted and
  untrusted folder rules, you can inspect the contents of the
  `~/.gemini/trustedFolders.json` file in your home directory.

## The trust check process (advanced)

For advanced users, it's helpful to know the exact order of operations for how
trust is determined:

1.  **IDE trust signal**: If you are using the
    [IDE Integration](/docs/ide-integration), the CLI first asks the IDE
    if the workspace is trusted. The IDE's response takes highest priority.

2.  **Local trust file**: If the IDE is not connected, the CLI checks the
    central `~/.gemini/trustedFolders.json` file.



---


# Tutorials

This page contains tutorials for interacting with Gemini CLI.

## Agent Skills

- [Getting Started with Agent Skills](/docs/cli/tutorials/skills-getting-started)

## Setting up a Model Context Protocol (MCP) server

> [!CAUTION] Before using a third-party MCP server, ensure you trust its source
> and understand the tools it provides. Your use of third-party servers is at
> your own risk.

This tutorial demonstrates how to set up an MCP server, using the
[GitHub MCP server](https://github.com/github/github-mcp-server) as an example.
The GitHub MCP server provides tools for interacting with GitHub repositories,
such as creating issues and commenting on pull requests.

### Prerequisites

Before you begin, ensure you have the following installed and configured:

- **Docker:** Install and run [Docker].
- **GitHub Personal Access Token (PAT):** Create a new [classic] or
  [fine-grained] PAT with the necessary scopes.

[Docker]: https://www.docker.com/
[classic]: https://github.com/settings/tokens/new
[fine-grained]: https://github.com/settings/personal-access-tokens/new

### Guide

#### Configure the MCP server in `settings.json`

In your project's root directory, create or open the
[`.gemini/settings.json` file](/docs/get-started/configuration). Within the
file, add the `mcpServers` configuration block, which provides instructions for
how to launch the GitHub MCP server.

```json
{
  "mcpServers": {
    "github": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "-e",
        "GITHUB_PERSONAL_ACCESS_TOKEN",
        "ghcr.io/github/github-mcp-server"
      ],
      "env": {
        "GITHUB_PERSONAL_ACCESS_TOKEN": "${GITHUB_PERSONAL_ACCESS_TOKEN}"
      }
    }
  }
}
```

#### Set your GitHub token

> [!CAUTION] Using a broadly scoped personal access token that has access to
> personal and private repositories can lead to information from the private
> repository being leaked into the public repository. We recommend using a
> fine-grained access token that doesn't share access to both public and private
> repositories.

Use an environment variable to store your GitHub PAT:

```bash
GITHUB_PERSONAL_ACCESS_TOKEN="pat_YourActualGitHubTokenHere"
```

Gemini CLI uses this value in the `mcpServers` configuration that you defined in
the `settings.json` file.

#### Launch Gemini CLI and verify the connection

When you launch Gemini CLI, it automatically reads your configuration and
launches the GitHub MCP server in the background. You can then use natural
language prompts to ask Gemini CLI to perform GitHub actions. For example:

```bash
"get all open issues assigned to me in the 'foo/bar' repo and prioritize them"
```


---


# Uninstalling the CLI

Your uninstall method depends on how you ran the CLI. Follow the instructions
for either npx or a global npm installation.

## Method 1: Using npx

npx runs packages from a temporary cache without a permanent installation. To
"uninstall" the CLI, you must clear this cache, which will remove gemini-cli and
any other packages previously executed with npx.

The npx cache is a directory named `_npx` inside your main npm cache folder. You
can find your npm cache path by running `npm config get cache`.

**For macOS / Linux**

```bash
# The path is typically ~/.npm/_npx
rm -rf "$(npm config get cache)/_npx"
```

**For Windows**

_Command Prompt_

```cmd
:: The path is typically %LocalAppData%\npm-cache\_npx
rmdir /s /q "%LocalAppData%\npm-cache\_npx"
```

_PowerShell_

```powershell
# The path is typically $env:LocalAppData\npm-cache\_npx
Remove-Item -Path (Join-Path $env:LocalAppData "npm-cache\_npx") -Recurse -Force
```

## Method 2: Using npm (global install)

If you installed the CLI globally (e.g., `npm install -g @google/gemini-cli`),
use the `npm uninstall` command with the `-g` flag to remove it.

```bash
npm uninstall -g @google/gemini-cli
```

This command completely removes the package from your system.

## Method 3: Homebrew

If you installed the CLI globally using Homebrew (e.g.,
`brew install gemini-cli`), use the `brew uninstall` command to remove it.

```bash
brew uninstall gemini-cli
```

## Method 4: MacPorts

If you installed the CLI globally using MacPorts (e.g.,
`sudo port install gemini-cli`), use the `port uninstall` command to remove it.

```bash
sudo port uninstall gemini-cli
```



---


# System Prompt Override 
(GEMINI_SYSTEM_MD)

The core system instructions that guide Gemini CLI can be completely replaced
with your own Markdown file. This feature is controlled via the
`GEMINI_SYSTEM_MD` environment variable.

## Overview

The `GEMINI_SYSTEM_MD` variable instructs the CLI to use an external Markdown
file for its system prompt, completely overriding the built-in default. This is
a full replacement, not a merge. If you use a custom file, none of the original
core instructions will apply unless you include them yourself.

This feature is intended for advanced users who need to enforce strict,
project-specific behavior or create a customized persona.

> Tip: You can export the current default system prompt to a file first, review
> it, and then selectively modify or replace it (see
> [“Export the default prompt”](#export-the-default-prompt-recommended)).

## How to enable

You can set the environment variable temporarily in your shell, or persist it
via a `.gemini/.env` file. See
[Persisting Environment Variables](/docs/get-started/authentication#persisting-environment-variables).

- Use the project default path (`.gemini/system.md`):
  - `GEMINI_SYSTEM_MD=true` or `GEMINI_SYSTEM_MD=1`
  - The CLI reads `./.gemini/system.md` (relative to your current project
    directory).

- Use a custom file path:
  - `GEMINI_SYSTEM_MD=/absolute/path/to/my-system.md`
  - Relative paths are supported and resolved from the current working
    directory.
  - Tilde expansion is supported (e.g., `~/my-system.md`).

- Disable the override (use built‑in prompt):
  - `GEMINI_SYSTEM_MD=false` or `GEMINI_SYSTEM_MD=0` or unset the variable.

If the override is enabled but the target file does not exist, the CLI will
error with: `missing system prompt file '<path>'`.

## Quick examples

- One‑off session using a project file:
  - `GEMINI_SYSTEM_MD=1 gemini`
- Persist for a project using `.gemini/.env`:
  - Create `.gemini/system.md`, then add to `.gemini/.env`:
    - `GEMINI_SYSTEM_MD=1`
- Use a custom file under your home directory:
  - `GEMINI_SYSTEM_MD=~/prompts/SYSTEM.md gemini`

## UI indicator

When `GEMINI_SYSTEM_MD` is active, the CLI shows a `|⌐■_■|` indicator in the UI
to signal custom system‑prompt mode.

## Variable Substitution

When using a custom system prompt file, you can use the following variables to
dynamically include built-in content:

- `${AgentSkills}`: Injects a complete section (including header) of all
  available agent skills.
- `${SubAgents}`: Injects a complete section (including header) of available
  sub-agents.
- `${AvailableTools}`: Injects a bulleted list of all currently enabled tool
  names.
- Tool Name Variables: Injects the actual name of a tool using the pattern:
  `${toolName}_ToolName` (e.g., `${write_file_ToolName}`,
  `${run_shell_command_ToolName}`).

  This pattern is generated dynamically for all available tools.

### Example

```markdown
# Custom System Prompt

You are a helpful assistant. ${AgentSkills}
${SubAgents}

## Tooling

The following tools are available to you: ${AvailableTools}

You can use ${write_file_ToolName} to save logs.
```

## Export the default prompt (recommended)

Before overriding, export the current default prompt so you can review required
safety and workflow rules.

- Write the built‑in prompt to the project default path:
  - `GEMINI_WRITE_SYSTEM_MD=1 gemini`
- Or write to a custom path:
  - `GEMINI_WRITE_SYSTEM_MD=~/prompts/DEFAULT_SYSTEM.md gemini`

This creates the file and writes the current built‑in system prompt to it.

## Best practices: SYSTEM.md vs GEMINI.md

- SYSTEM.md (firmware):
  - Non‑negotiable operational rules: safety, tool‑use protocols, approvals, and
    mechanics that keep the CLI reliable.
  - Stable across tasks and projects (or per project when needed).
- GEMINI.md (strategy):
  - Persona, goals, methodologies, and project/domain context.
  - Evolves per task; relies on SYSTEM.md for safe execution.

Keep SYSTEM.md minimal but complete for safety and tool operation. Keep
GEMINI.md focused on high‑level guidance and project specifics.

## Troubleshooting

- Error: `missing system prompt file '…'`
  - Ensure the referenced path exists and is readable.
  - For `GEMINI_SYSTEM_MD=1|true`, create `./.gemini/system.md` in your project.
- Override not taking effect
  - Confirm the variable is loaded (use `.gemini/.env` or export in your shell).
  - Paths are resolved from the current working directory; try an absolute path.
- Restore defaults
  - Unset `GEMINI_SYSTEM_MD` or set it to `0`/`false`.
 


---


# Gemini CLI core

Gemini CLI's core package (`packages/core`) is the backend portion of Gemini
CLI, handling communication with the Gemini API, managing tools, and processing
requests sent from `packages/cli`. For a general overview of Gemini CLI, see the
[main documentation page](/docs).

## Navigating this section

- **[Core tools API](/docs/core/tools-api):** Information on how tools are defined,
  registered, and used by the core.
- **[Memory Import Processor](/docs/core/memport):** Documentation for the modular
  GEMINI.md import feature using @file.md syntax.
- **[Policy Engine](/docs/core/policy-engine):** Use the Policy Engine for
  fine-grained control over tool execution.

## Role of the core

While the `packages/cli` portion of Gemini CLI provides the user interface,
`packages/core` is responsible for:

- **Gemini API interaction:** Securely communicating with the Google Gemini API,
  sending user prompts, and receiving model responses.
- **Prompt engineering:** Constructing effective prompts for the Gemini model,
  potentially incorporating conversation history, tool definitions, and
  instructional context from `GEMINI.md` files.
- **Tool management & orchestration:**
  - Registering available tools (e.g., file system tools, shell command
    execution).
  - Interpreting tool use requests from the Gemini model.
  - Executing the requested tools with the provided arguments.
  - Returning tool execution results to the Gemini model for further processing.
- **Session and state management:** Keeping track of the conversation state,
  including history and any relevant context required for coherent interactions.
- **Configuration:** Managing core-specific configurations, such as API key
  access, model selection, and tool settings.

## Security considerations

The core plays a vital role in security:

- **API key management:** It handles the `GEMINI_API_KEY` and ensures it's used
  securely when communicating with the Gemini API.
- **Tool execution:** When tools interact with the local system (e.g.,
  `run_shell_command`), the core (and its underlying tool implementations) must
  do so with appropriate caution, often involving sandboxing mechanisms to
  prevent unintended modifications.

## Chat history compression

To ensure that long conversations don't exceed the token limits of the Gemini
model, the core includes a chat history compression feature.

When a conversation approaches the token limit for the configured model, the
core automatically compresses the conversation history before sending it to the
model. This compression is designed to be lossless in terms of the information
conveyed, but it reduces the overall number of tokens used.

You can find the token limits for each model in the
[Google AI documentation](https://ai.google.dev/gemini-api/docs/models).

## Model fallback

Gemini CLI includes a model fallback mechanism to ensure that you can continue
to use the CLI even if the default "pro" model is rate-limited.

If you are using the default "pro" model and the CLI detects that you are being
rate-limited, it automatically switches to the "flash" model for the current
session. This allows you to continue working without interruption.

Internal utility calls that use `gemini-2.5-flash-lite` (for example, prompt
completion and classification) silently fall back to `gemini-2.5-flash` and
`gemini-2.5-pro` when quota is exhausted, without changing the configured model.

## File discovery service

The file discovery service is responsible for finding files in the project that
are relevant to the current context. It is used by the `@` command and other
tools that need to access files.

## Memory discovery service

The memory discovery service is responsible for finding and loading the
`GEMINI.md` files that provide context to the model. It searches for these files
in a hierarchical manner, starting from the current working directory and moving
up to the project root and the user's home directory. It also searches in
subdirectories.

This allows you to have global, project-level, and component-level context
files, which are all combined to provide the model with the most relevant
information.

You can use the [`/memory` command](/docs/cli/commands) to `show`, `add`, and
`refresh` the content of loaded `GEMINI.md` files.

## Citations

When Gemini finds it is reciting text from a source it appends the citation to
the output. It is enabled by default but can be disabled with the
ui.showCitations setting.

- When proposing an edit the citations display before giving the user the option
  to accept.
- Citations are always shown at the end of the model’s turn.
- We deduplicate citations and display them in alphabetical order.



---


# Gemini CLI core: Tools API

The Gemini CLI core (`packages/core`) features a robust system for defining,
registering, and executing tools. These tools extend the capabilities of the
Gemini model, allowing it to interact with the local environment, fetch web
content, and perform various actions beyond simple text generation.

## Core concepts

- **Tool (`tools.ts`):** An interface and base class (`BaseTool`) that defines
  the contract for all tools. Each tool must have:
  - `name`: A unique internal name (used in API calls to Gemini).
  - `displayName`: A user-friendly name.
  - `description`: A clear explanation of what the tool does, which is provided
    to the Gemini model.
  - `parameterSchema`: A JSON schema defining the parameters that the tool
    accepts. This is crucial for the Gemini model to understand how to call the
    tool correctly.
  - `validateToolParams()`: A method to validate incoming parameters.
  - `getDescription()`: A method to provide a human-readable description of what
    the tool will do with specific parameters before execution.
  - `shouldConfirmExecute()`: A method to determine if user confirmation is
    required before execution (e.g., for potentially destructive operations).
  - `execute()`: The core method that performs the tool's action and returns a
    `ToolResult`.

- **`ToolResult` (`tools.ts`):** An interface defining the structure of a tool's
  execution outcome:
  - `llmContent`: The factual content to be included in the history sent back to
    the LLM for context. This can be a simple string or a `PartListUnion` (an
    array of `Part` objects and strings) for rich content.
  - `returnDisplay`: A user-friendly string (often Markdown) or a special object
    (like `FileDiff`) for display in the CLI.

- **Returning rich content:** Tools are not limited to returning simple text.
  The `llmContent` can be a `PartListUnion`, which is an array that can contain
  a mix of `Part` objects (for images, audio, etc.) and `string`s. This allows a
  single tool execution to return multiple pieces of rich content.

- **Tool registry (`tool-registry.ts`):** A class (`ToolRegistry`) responsible
  for:
  - **Registering tools:** Holding a collection of all available built-in tools
    (e.g., `ReadFileTool`, `ShellTool`).
  - **Discovering tools:** It can also discover tools dynamically:
    - **Command-based discovery:** If `tools.discoveryCommand` is configured in
      settings, this command is executed. It's expected to output JSON
      describing custom tools, which are then registered as `DiscoveredTool`
      instances.
    - **MCP-based discovery:** If `mcp.serverCommand` is configured, the
      registry can connect to a Model Context Protocol (MCP) server to list and
      register tools (`DiscoveredMCPTool`).
  - **Providing schemas:** Exposing the `FunctionDeclaration` schemas of all
    registered tools to the Gemini model, so it knows what tools are available
    and how to use them.
  - **Retrieving tools:** Allowing the core to get a specific tool by name for
    execution.

## Built-in tools

The core comes with a suite of pre-defined tools, typically found in
`packages/core/src/tools/`. These include:

- **File system tools:**
  - `LSTool` (`ls.ts`): Lists directory contents.
  - `ReadFileTool` (`read-file.ts`): Reads the content of a single file.
  - `WriteFileTool` (`write-file.ts`): Writes content to a file.
  - `GrepTool` (`grep.ts`): Searches for patterns in files.
  - `GlobTool` (`glob.ts`): Finds files matching glob patterns.
  - `EditTool` (`edit.ts`): Performs in-place modifications to files (often
    requiring confirmation).
  - `ReadManyFilesTool` (`read-many-files.ts`): Reads and concatenates content
    from multiple files or glob patterns (used by the `@` command in CLI).
- **Execution tools:**
  - `ShellTool` (`shell.ts`): Executes arbitrary shell commands (requires
    careful sandboxing and user confirmation).
- **Web tools:**
  - `WebFetchTool` (`web-fetch.ts`): Fetches content from a URL.
  - `WebSearchTool` (`web-search.ts`): Performs a web search.
- **Memory tools:**
  - `MemoryTool` (`memoryTool.ts`): Interacts with the AI's memory.

Each of these tools extends `BaseTool` and implements the required methods for
its specific functionality.

## Tool execution flow

1.  **Model request:** The Gemini model, based on the user's prompt and the
    provided tool schemas, decides to use a tool and returns a `FunctionCall`
    part in its response, specifying the tool name and arguments.
2.  **Core receives request:** The core parses this `FunctionCall`.
3.  **Tool retrieval:** It looks up the requested tool in the `ToolRegistry`.
4.  **Parameter validation:** The tool's `validateToolParams()` method is
    called.
5.  **Confirmation (if needed):**
    - The tool's `shouldConfirmExecute()` method is called.
    - If it returns details for confirmation, the core communicates this back to
      the CLI, which prompts the user.
    - The user's decision (e.g., proceed, cancel) is sent back to the core.
6.  **Execution:** If validated and confirmed (or if no confirmation is needed),
    the core calls the tool's `execute()` method with the provided arguments and
    an `AbortSignal` (for potential cancellation).
7.  **Result processing:** The `ToolResult` from `execute()` is received by the
    core.
8.  **Response to model:** The `llmContent` from the `ToolResult` is packaged as
    a `FunctionResponse` and sent back to the Gemini model so it can continue
    generating a user-facing response.
9.  **Display to user:** The `returnDisplay` from the `ToolResult` is sent to
    the CLI to show the user what the tool did.

## Extending with custom tools

While direct programmatic registration of new tools by users isn't explicitly
detailed as a primary workflow in the provided files for typical end-users, the
architecture supports extension through:

- **Command-based discovery:** Advanced users or project administrators can
  define a `tools.discoveryCommand` in `settings.json`. This command, when run
  by the Gemini CLI core, should output a JSON array of `FunctionDeclaration`
  objects. The core will then make these available as `DiscoveredTool`
  instances. The corresponding `tools.callCommand` would then be responsible for
  actually executing these custom tools.
- **MCP server(s):** For more complex scenarios, one or more MCP servers can be
  set up and configured via the `mcpServers` setting in `settings.json`. The
  Gemini CLI core can then discover and use tools exposed by these servers. As
  mentioned, if you have multiple MCP servers, the tool names will be prefixed
  with the server name from your configuration (e.g.,
  `serverAlias__actualToolName`).

This tool system provides a flexible and powerful way to augment the Gemini
model's capabilities, making the Gemini CLI a versatile assistant for a wide
range of tasks.



---


# Memory Import Processor

The Memory Import Processor is a feature that allows you to modularize your
GEMINI.md files by importing content from other files using the `@file.md`
syntax.

## Overview

This feature enables you to break down large GEMINI.md files into smaller, more
manageable components that can be reused across different contexts. The import
processor supports both relative and absolute paths, with built-in safety
features to prevent circular imports and ensure file access security.

## Syntax

Use the `@` symbol followed by the path to the file you want to import:

```markdown
# Main GEMINI.md file

This is the main content.

@./components/instructions.md

More content here.

@./shared/configuration.md
```

## Supported path formats

### Relative paths

- `@./file.md` - Import from the same directory
- `@../file.md` - Import from parent directory
- `@./components/file.md` - Import from subdirectory

### Absolute paths

- `@/absolute/path/to/file.md` - Import using absolute path

## Examples

### Basic import

```markdown
# My GEMINI.md

Welcome to my project!

@./get-started.md

## Features

@./features/overview.md
```

### Nested imports

The imported files can themselves contain imports, creating a nested structure:

```markdown
# main.md

@./header.md @./content.md @./footer.md
```

```markdown
# header.md

# Project Header

@./shared/title.md
```

## Safety features

### Circular import detection

The processor automatically detects and prevents circular imports:

```markdown
# file-a.md

@./file-b.md
```

```markdown
# file-b.md

@./file-a.md <!-- This will be detected and prevented -->
```

### File access security

The `validateImportPath` function ensures that imports are only allowed from
specified directories, preventing access to sensitive files outside the allowed
scope.

### Maximum import depth

To prevent infinite recursion, there's a configurable maximum import depth
(default: 5 levels).

## Error handling

### Missing files

If a referenced file doesn't exist, the import will fail gracefully with an
error comment in the output.

### File access errors

Permission issues or other file system errors are handled gracefully with
appropriate error messages.

## Code region detection

The import processor uses the `marked` library to detect code blocks and inline
code spans, ensuring that `@` imports inside these regions are properly ignored.
This provides robust handling of nested code blocks and complex Markdown
structures.

## Import tree structure

The processor returns an import tree that shows the hierarchy of imported files,
similar to Claude's `/memory` feature. This helps users debug problems with
their GEMINI.md files by showing which files were read and their import
relationships.

Example tree structure:

```
Memory Files
 L project: GEMINI.md
            L a.md
              L b.md
                L c.md
              L d.md
                L e.md
                  L f.md
            L included.md
```

The tree preserves the order that files were imported and shows the complete
import chain for debugging purposes.

## Comparison to Claude Code's `/memory` (`claude.md`) approach

Claude Code's `/memory` feature (as seen in `claude.md`) produces a flat, linear
document by concatenating all included files, always marking file boundaries
with clear comments and path names. It does not explicitly present the import
hierarchy, but the LLM receives all file contents and paths, which is sufficient
for reconstructing the hierarchy if needed.

> [!NOTE] The import tree is mainly for clarity during development and has
> limited relevance to LLM consumption.

## API reference

### `processImports(content, basePath, debugMode?, importState?)`

Processes import statements in GEMINI.md content.

**Parameters:**

- `content` (string): The content to process for imports
- `basePath` (string): The directory path where the current file is located
- `debugMode` (boolean, optional): Whether to enable debug logging (default:
  false)
- `importState` (ImportState, optional): State tracking for circular import
  prevention

**Returns:** Promise&lt;ProcessImportsResult&gt; - Object containing processed
content and import tree

### `ProcessImportsResult`

```typescript
interface ProcessImportsResult {
  content: string; // The processed content with imports resolved
  importTree: MemoryFile; // Tree structure showing the import hierarchy
}
```

### `MemoryFile`

```typescript
interface MemoryFile {
  path: string; // The file path
  imports?: MemoryFile[]; // Direct imports, in the order they were imported
}
```

### `validateImportPath(importPath, basePath, allowedDirectories)`

Validates import paths to ensure they are safe and within allowed directories.

**Parameters:**

- `importPath` (string): The import path to validate
- `basePath` (string): The base directory for resolving relative paths
- `allowedDirectories` (string[]): Array of allowed directory paths

**Returns:** boolean - Whether the import path is valid

### `findProjectRoot(startDir)`

Finds the project root by searching for a `.git` directory upwards from the
given start directory. Implemented as an **async** function using non-blocking
file system APIs to avoid blocking the Node.js event loop.

**Parameters:**

- `startDir` (string): The directory to start searching from

**Returns:** Promise&lt;string&gt; - The project root directory (or the start
directory if no `.git` is found)

## Best Practices

1. **Use descriptive file names** for imported components
2. **Keep imports shallow** - avoid deeply nested import chains
3. **Document your structure** - maintain a clear hierarchy of imported files
4. **Test your imports** - ensure all referenced files exist and are accessible
5. **Use relative paths** when possible for better portability

## Troubleshooting

### Common issues

1. **Import not working**: Check that the file exists and the path is correct
2. **Circular import warnings**: Review your import structure for circular
   references
3. **Permission errors**: Ensure the files are readable and within allowed
   directories
4. **Path resolution issues**: Use absolute paths if relative paths aren't
   resolving correctly

### Debug mode

Enable debug mode to see detailed logging of the import process:

```typescript
const result = await processImports(content, basePath, true);
```


---


# Policy engine

The Gemini CLI includes a powerful policy engine that provides fine-grained
control over tool execution. It allows users and administrators to define rules
that determine whether a tool call should be allowed, denied, or require user
confirmation.

## Quick start

To create your first policy:

1.  **Create the policy directory** if it doesn't exist:
    ```bash
    mkdir -p ~/.gemini/policies
    ```
2.  **Create a new policy file** (e.g., `~/.gemini/policies/my-rules.toml`). You
    can use any filename ending in `.toml`; all such files in this directory
    will be loaded and combined:
    ```toml
    [[rule]]
    toolName = "run_shell_command"
    commandPrefix = "git status"
    decision = "allow"
    priority = 100
    ```
3.  **Run a command** that triggers the policy (e.g., ask Gemini CLI to
    `git status`). The tool will now execute automatically without prompting for
    confirmation.

## Core concepts

The policy engine operates on a set of rules. Each rule is a combination of
conditions and a resulting decision. When a large language model wants to
execute a tool, the policy engine evaluates all rules to find the
highest-priority rule that matches the tool call.

A rule consists of the following main components:

- **Conditions**: Criteria that a tool call must meet for the rule to apply.
  This can include the tool's name, the arguments provided to it, or the current
  approval mode.
- **Decision**: The action to take if the rule matches (`allow`, `deny`, or
  `ask_user`).
- **Priority**: A number that determines the rule's precedence. Higher numbers
  win.

For example, this rule will ask for user confirmation before executing any `git`
command.

```toml
[[rule]]
toolName = "run_shell_command"
commandPrefix = "git "
decision = "ask_user"
priority = 100
```

### Conditions

Conditions are the criteria that a tool call must meet for a rule to apply. The
primary conditions are the tool's name and its arguments.

#### Tool Name

The `toolName` in the rule must match the name of the tool being called.

- **Wildcards**: For Model-hosting-protocol (MCP) servers, you can use a
  wildcard. A `toolName` of `my-server__*` will match any tool from the
  `my-server` MCP.

#### Arguments pattern

If `argsPattern` is specified, the tool's arguments are converted to a stable
JSON string, which is then tested against the provided regular expression. If
the arguments don't match the pattern, the rule does not apply.

### Decisions

There are three possible decisions a rule can enforce:

- `allow`: The tool call is executed automatically without user interaction.
- `deny`: The tool call is blocked and is not executed.
- `ask_user`: The user is prompted to approve or deny the tool call. (In
  non-interactive mode, this is treated as `deny`.)

### Priority system and tiers

The policy engine uses a sophisticated priority system to resolve conflicts when
multiple rules match a single tool call. The core principle is simple: **the
rule with the highest priority wins**.

To provide a clear hierarchy, policies are organized into three tiers. Each tier
has a designated number that forms the base of the final priority calculation.

| Tier    | Base | Description                                                                |
| :------ | :--- | :------------------------------------------------------------------------- |
| Default | 1    | Built-in policies that ship with the Gemini CLI.                           |
| User    | 2    | Custom policies defined by the user.                                       |
| Admin   | 3    | Policies managed by an administrator (e.g., in an enterprise environment). |

Within a TOML policy file, you assign a priority value from **0 to 999**. The
engine transforms this into a final priority using the following formula:

`final_priority = tier_base + (toml_priority / 1000)`

This system guarantees that:

- Admin policies always override User and Default policies.
- User policies always override Default policies.
- You can still order rules within a single tier with fine-grained control.

For example:

- A `priority: 50` rule in a Default policy file becomes `1.050`.
- A `priority: 100` rule in a User policy file becomes `2.100`.
- A `priority: 20` rule in an Admin policy file becomes `3.020`.

### Approval modes

Approval modes allow the policy engine to apply different sets of rules based on
the CLI's operational mode. A rule can be associated with one or more modes
(e.g., `yolo`, `autoEdit`). The rule will only be active if the CLI is running
in one of its specified modes. If a rule has no modes specified, it is always
active.

## Rule matching

When a tool call is made, the engine checks it against all active rules,
starting from the highest priority. The first rule that matches determines the
outcome.

A rule matches a tool call if all of its conditions are met:

1.  **Tool name**: The `toolName` in the rule must match the name of the tool
    being called.
    - **Wildcards**: For Model-hosting-protocol (MCP) servers, you can use a
      wildcard. A `toolName` of `my-server__*` will match any tool from the
      `my-server` MCP.
2.  **Arguments pattern**: If `argsPattern` is specified, the tool's arguments
    are converted to a stable JSON string, which is then tested against the
    provided regular expression. If the arguments don't match the pattern, the
    rule does not apply.

## Configuration

Policies are defined in `.toml` files. The CLI loads these files from Default,
User, and (if configured) Admin directories.

### TOML rule schema

Here is a breakdown of the fields available in a TOML policy rule:

```toml
[[rule]]
# A unique name for the tool, or an array of names.
toolName = "run_shell_command"

# (Optional) The name of an MCP server. Can be combined with toolName
# to form a composite name like "mcpName__toolName".
mcpName = "my-custom-server"

# (Optional) A regex to match against the tool's arguments.
argsPattern = '"command":"(git|npm)'

# (Optional) A string or array of strings that a shell command must start with.
# This is syntactic sugar for `toolName = "run_shell_command"` and an `argsPattern`.
commandPrefix = "git "

# (Optional) A regex to match against the entire shell command.
# This is also syntactic sugar for `toolName = "run_shell_command"`.
# Note: This pattern is tested against the JSON representation of the arguments (e.g., `{"command":"<your_command>"}`), so anchors like `^` or `$` will apply to the full JSON string, not just the command text.
# You cannot use commandPrefix and commandRegex in the same rule.
commandRegex = "^git (commit|push)"

# The decision to take. Must be "allow", "deny", or "ask_user".
decision = "ask_user"

# The priority of the rule, from 0 to 999.
priority = 10

# (Optional) An array of approval modes where this rule is active.
modes = ["autoEdit"]
```

### Using arrays (lists)

To apply the same rule to multiple tools or command prefixes, you can provide an
array of strings for the `toolName` and `commandPrefix` fields.

**Example:**

This single rule will apply to both the `write_file` and `replace` tools.

```toml
[[rule]]
toolName = ["write_file", "replace"]
decision = "ask_user"
priority = 10
```

### Special syntax for `run_shell_command`

To simplify writing policies for `run_shell_command`, you can use
`commandPrefix` or `commandRegex` instead of the more complex `argsPattern`.

- `commandPrefix`: Matches if the `command` argument starts with the given
  string.
- `commandRegex`: Matches if the `command` argument matches the given regular
  expression.

**Example:**

This rule will ask for user confirmation before executing any `git` command.

```toml
[[rule]]
toolName = "run_shell_command"
commandPrefix = "git "
decision = "ask_user"
priority = 100
```

### Special syntax for MCP tools

You can create rules that target tools from Model-hosting-protocol (MCP) servers
using the `mcpName` field or a wildcard pattern.

**1. Using `mcpName`**

To target a specific tool from a specific server, combine `mcpName` and
`toolName`.

```toml
# Allows the `search` tool on the `my-jira-server` MCP
[[rule]]
mcpName = "my-jira-server"
toolName = "search"
decision = "allow"
priority = 200
```

**2. Using a wildcard**

To create a rule that applies to _all_ tools on a specific MCP server, specify
only the `mcpName`.

```toml
# Denies all tools from the `untrusted-server` MCP
[[rule]]
mcpName = "untrusted-server"
decision = "deny"
priority = 500
```

## Default policies

The Gemini CLI ships with a set of default policies to provide a safe
out-of-the-box experience.

- **Read-only tools** (like `read_file`, `glob`) are generally **allowed**.
- **Agent delegation** (like `delegate_to_agent`) defaults to **`ask_user`** to
  ensure remote agents can prompt for confirmation, but local sub-agent actions
  are executed silently and checked individually.
- **Write tools** (like `write_file`, `run_shell_command`) default to
  **`ask_user`**.
- In **`yolo`** mode, a high-priority rule allows all tools.
- In **`autoEdit`** mode, rules allow certain write operations to happen without
  prompting.
 


---


# Gemini CLI tools

The Gemini CLI includes built-in tools that the Gemini model uses to interact
with your local environment, access information, and perform actions. These
tools enhance the CLI's capabilities, enabling it to go beyond text generation
and assist with a wide range of tasks.

## Overview of Gemini CLI tools

In the context of the Gemini CLI, tools are specific functions or modules that
the Gemini model can request to be executed. For example, if you ask Gemini to
"Summarize the contents of `my_document.txt`," the model will likely identify
the need to read that file and will request the execution of the `read_file`
tool.

The core component (`packages/core`) manages these tools, presents their
definitions (schemas) to the Gemini model, executes them when requested, and
returns the results to the model for further processing into a user-facing
response.

These tools provide the following capabilities:

- **Access local information:** Tools allow Gemini to access your local file
  system, read file contents, list directories, etc.
- **Execute commands:** With tools like `run_shell_command`, Gemini can run
  shell commands (with appropriate safety measures and user confirmation).
- **Interact with the web:** Tools can fetch content from URLs.
- **Take actions:** Tools can modify files, write new files, or perform other
  actions on your system (again, typically with safeguards).
- **Ground responses:** By using tools to fetch real-time or specific local
  data, Gemini's responses can be more accurate, relevant, and grounded in your
  actual context.

## How to use Gemini CLI tools

To use Gemini CLI tools, provide a prompt to the Gemini CLI. The process works
as follows:

1.  You provide a prompt to the Gemini CLI.
2.  The CLI sends the prompt to the core.
3.  The core, along with your prompt and conversation history, sends a list of
    available tools and their descriptions/schemas to the Gemini API.
4.  The Gemini model analyzes your request. If it determines that a tool is
    needed, its response will include a request to execute a specific tool with
    certain parameters.
5.  The core receives this tool request, validates it, and (often after user
    confirmation for sensitive operations) executes the tool.
6.  The output from the tool is sent back to the Gemini model.
7.  The Gemini model uses the tool's output to formulate its final answer, which
    is then sent back through the core to the CLI and displayed to you.

You will typically see messages in the CLI indicating when a tool is being
called and whether it succeeded or failed.

## Security and confirmation

Many tools, especially those that can modify your file system or execute
commands (`write_file`, `edit`, `run_shell_command`), are designed with safety
in mind. The Gemini CLI will typically:

- **Require confirmation:** Prompt you before executing potentially sensitive
  operations, showing you what action is about to be taken.
- **Utilize sandboxing:** All tools are subject to restrictions enforced by
  sandboxing (see [Sandboxing in the Gemini CLI](/docs/cli/sandbox)). This means
  that when operating in a sandbox, any tools (including MCP servers) you wish
  to use must be available _inside_ the sandbox environment. For example, to run
  an MCP server through `npx`, the `npx` executable must be installed within the
  sandbox's Docker image or be available in the `sandbox-exec` environment.

It's important to always review confirmation prompts carefully before allowing a
tool to proceed.

## Learn more about Gemini CLI's tools

Gemini CLI's built-in tools can be broadly categorized as follows:

- **[File System Tools](/docs/tools/file-system):** For interacting with files and
  directories (reading, writing, listing, searching, etc.).
- **[Shell Tool](/docs/tools/shell) (`run_shell_command`):** For executing shell
  commands.
- **[Web Fetch Tool](/docs/tools/web-fetch) (`web_fetch`):** For retrieving content
  from URLs.
- **[Web Search Tool](/docs/tools/web-search) (`google_web_search`):** For searching
  the web.
- **[Memory Tool](/docs/tools/memory) (`save_memory`):** For saving and recalling
  information across sessions.
- **[Todo Tool](/docs/tools/todos) (`write_todos`):** For managing subtasks of complex
  requests.

Additionally, these tools incorporate:

- **[MCP servers](/docs/tools/mcp-server)**: MCP servers act as a bridge between the
  Gemini model and your local environment or other services like APIs.
- **[Agent Skills](/docs/cli/skills)**: (Experimental) On-demand expertise
  packages that are activated via the `activate_skill` tool to provide
  specialized guidance and resources.
- **[Sandboxing](/docs/cli/sandbox)**: Sandboxing isolates the model and its
  changes from your environment to reduce potential risk.



---


# Gemini CLI file system tools

The Gemini CLI provides a comprehensive suite of tools for interacting with the
local file system. These tools allow the Gemini model to read from, write to,
list, search, and modify files and directories, all under your control and
typically with confirmation for sensitive operations.

**Note:** All file system tools operate within a `rootDirectory` (usually the
current working directory where you launched the CLI) for security. Paths that
you provide to these tools are generally expected to be absolute or are resolved
relative to this root directory.

## 1. `list_directory` (ReadFolder)

`list_directory` lists the names of files and subdirectories directly within a
specified directory path. It can optionally ignore entries matching provided
glob patterns.

- **Tool name:** `list_directory`
- **Display name:** ReadFolder
- **File:** `ls.ts`
- **Parameters:**
  - `path` (string, required): The absolute path to the directory to list.
  - `ignore` (array of strings, optional): A list of glob patterns to exclude
    from the listing (e.g., `["*.log", ".git"]`).
  - `respect_git_ignore` (boolean, optional): Whether to respect `.gitignore`
    patterns when listing files. Defaults to `true`.
- **Behavior:**
  - Returns a list of file and directory names.
  - Indicates whether each entry is a directory.
  - Sorts entries with directories first, then alphabetically.
- **Output (`llmContent`):** A string like:
  `Directory listing for /path/to/your/folder:\n[DIR] subfolder1\nfile1.txt\nfile2.png`
- **Confirmation:** No.

## 2. `read_file` (ReadFile)

`read_file` reads and returns the content of a specified file. This tool handles
text, images (PNG, JPG, GIF, WEBP, SVG, BMP), audio files (MP3, WAV, AIFF, AAC,
OGG, FLAC), and PDF files. For text files, it can read specific line ranges.
Other binary file types are generally skipped.

- **Tool name:** `read_file`
- **Display name:** ReadFile
- **File:** `read-file.ts`
- **Parameters:**
  - `path` (string, required): The absolute path to the file to read.
  - `offset` (number, optional): For text files, the 0-based line number to
    start reading from. Requires `limit` to be set.
  - `limit` (number, optional): For text files, the maximum number of lines to
    read. If omitted, reads a default maximum (e.g., 2000 lines) or the entire
    file if feasible.
- **Behavior:**
  - For text files: Returns the content. If `offset` and `limit` are used,
    returns only that slice of lines. Indicates if content was truncated due to
    line limits or line length limits.
  - For image, audio, and PDF files: Returns the file content as a
    base64-encoded data structure suitable for model consumption.
  - For other binary files: Attempts to identify and skip them, returning a
    message indicating it's a generic binary file.
- **Output:** (`llmContent`):
  - For text files: The file content, potentially prefixed with a truncation
    message (e.g.,
    `[File content truncated: showing lines 1-100 of 500 total lines...]\nActual file content...`).
  - For image/audio/PDF files: An object containing `inlineData` with `mimeType`
    and base64 `data` (e.g.,
    `{ inlineData: { mimeType: 'image/png', data: 'base64encodedstring' } }`).
  - For other binary files: A message like
    `Cannot display content of binary file: /path/to/data.bin`.
- **Confirmation:** No.

## 3. `write_file` (WriteFile)

`write_file` writes content to a specified file. If the file exists, it will be
overwritten. If the file doesn't exist, it (and any necessary parent
directories) will be created.

- **Tool name:** `write_file`
- **Display name:** WriteFile
- **File:** `write-file.ts`
- **Parameters:**
  - `file_path` (string, required): The absolute path to the file to write to.
  - `content` (string, required): The content to write into the file.
- **Behavior:**
  - Writes the provided `content` to the `file_path`.
  - Creates parent directories if they don't exist.
- **Output (`llmContent`):** A success message, e.g.,
  `Successfully overwrote file: /path/to/your/file.txt` or
  `Successfully created and wrote to new file: /path/to/new/file.txt`.
- **Confirmation:** Yes. Shows a diff of changes and asks for user approval
  before writing.

## 4. `glob` (FindFiles)

`glob` finds files matching specific glob patterns (e.g., `src/**/*.ts`,
`*.md`), returning absolute paths sorted by modification time (newest first).

- **Tool name:** `glob`
- **Display name:** FindFiles
- **File:** `glob.ts`
- **Parameters:**
  - `pattern` (string, required): The glob pattern to match against (e.g.,
    `"*.py"`, `"src/**/*.js"`).
  - `path` (string, optional): The absolute path to the directory to search
    within. If omitted, searches the tool's root directory.
  - `case_sensitive` (boolean, optional): Whether the search should be
    case-sensitive. Defaults to `false`.
  - `respect_git_ignore` (boolean, optional): Whether to respect .gitignore
    patterns when finding files. Defaults to `true`.
- **Behavior:**
  - Searches for files matching the glob pattern within the specified directory.
  - Returns a list of absolute paths, sorted with the most recently modified
    files first.
  - Ignores common nuisance directories like `node_modules` and `.git` by
    default.
- **Output (`llmContent`):** A message like:
  `Found 5 file(s) matching "*.ts" within src, sorted by modification time (newest first):\nsrc/file1.ts\nsrc/subdir/file2.ts...`
- **Confirmation:** No.

## 5. `search_file_content` (SearchText)

`search_file_content` searches for a regular expression pattern within the
content of files in a specified directory. Can filter files by a glob pattern.
Returns the lines containing matches, along with their file paths and line
numbers.

- **Tool name:** `search_file_content`
- **Display name:** SearchText
- **File:** `grep.ts`
- **Parameters:**
  - `pattern` (string, required): The regular expression (regex) to search for
    (e.g., `"function\s+myFunction"`).
  - `path` (string, optional): The absolute path to the directory to search
    within. Defaults to the current working directory.
  - `include` (string, optional): A glob pattern to filter which files are
    searched (e.g., `"*.js"`, `"src/**/*.{ts,tsx}"`). If omitted, searches most
    files (respecting common ignores).
- **Behavior:**
  - Uses `git grep` if available in a Git repository for speed; otherwise, falls
    back to system `grep` or a JavaScript-based search.
  - Returns a list of matching lines, each prefixed with its file path (relative
    to the search directory) and line number.
- **Output (`llmContent`):** A formatted string of matches, e.g.:
  ```
  Found 3 matches for pattern "myFunction" in path "." (filter: "*.ts"):
  ---
  File: src/utils.ts
  L15: export function myFunction() {
  L22:   myFunction.call();
  ---
  File: src/index.ts
  L5: import { myFunction } from './utils';
  ---
  ```
- **Confirmation:** No.

## 6. `replace` (Edit)

`replace` replaces text within a file. By default, replaces a single occurrence,
but can replace multiple occurrences when `expected_replacements` is specified.
This tool is designed for precise, targeted changes and requires significant
context around the `old_string` to ensure it modifies the correct location.

- **Tool name:** `replace`
- **Display name:** Edit
- **File:** `edit.ts`
- **Parameters:**
  - `file_path` (string, required): The absolute path to the file to modify.
  - `old_string` (string, required): The exact literal text to replace.

    **CRITICAL:** This string must uniquely identify the single instance to
    change. It should include at least 3 lines of context _before_ and _after_
    the target text, matching whitespace and indentation precisely. If
    `old_string` is empty, the tool attempts to create a new file at `file_path`
    with `new_string` as content.

  - `new_string` (string, required): The exact literal text to replace
    `old_string` with.
  - `expected_replacements` (number, optional): The number of occurrences to
    replace. Defaults to `1`.

- **Behavior:**
  - If `old_string` is empty and `file_path` does not exist, creates a new file
    with `new_string` as content.
  - If `old_string` is provided, it reads the `file_path` and attempts to find
    exactly one occurrence of `old_string`.
  - If one occurrence is found, it replaces it with `new_string`.
  - **Enhanced reliability (multi-stage edit correction):** To significantly
    improve the success rate of edits, especially when the model-provided
    `old_string` might not be perfectly precise, the tool incorporates a
    multi-stage edit correction mechanism.
    - If the initial `old_string` isn't found or matches multiple locations, the
      tool can leverage the Gemini model to iteratively refine `old_string` (and
      potentially `new_string`).
    - This self-correction process attempts to identify the unique segment the
      model intended to modify, making the `replace` operation more robust even
      with slightly imperfect initial context.
- **Failure conditions:** Despite the correction mechanism, the tool will fail
  if:
  - `file_path` is not absolute or is outside the root directory.
  - `old_string` is not empty, but the `file_path` does not exist.
  - `old_string` is empty, but the `file_path` already exists.
  - `old_string` is not found in the file after attempts to correct it.
  - `old_string` is found multiple times, and the self-correction mechanism
    cannot resolve it to a single, unambiguous match.
- **Output (`llmContent`):**
  - On success:
    `Successfully modified file: /path/to/file.txt (1 replacements).` or
    `Created new file: /path/to/new_file.txt with provided content.`
  - On failure: An error message explaining the reason (e.g.,
    `Failed to edit, 0 occurrences found...`,
    `Failed to edit, expected 1 occurrences but found 2...`).
- **Confirmation:** Yes. Shows a diff of the proposed changes and asks for user
  approval before writing to the file.

These file system tools provide a foundation for the Gemini CLI to understand
and interact with your local project context.



---



# Shell tool
(`run_shell_command`)

This document describes the `run_shell_command` tool for the Gemini CLI.

## Description

Use `run_shell_command` to interact with the underlying system, run scripts, or
perform command-line operations. `run_shell_command` executes a given shell
command, including interactive commands that require user input (e.g., `vim`,
`git rebase -i`) if the `tools.shell.enableInteractiveShell` setting is set to
`true`.

On Windows, commands are executed with `powershell.exe -NoProfile -Command`
(unless you explicitly point `ComSpec` at another shell). On other platforms,
they are executed with `bash -c`.

### Arguments

`run_shell_command` takes the following arguments:

- `command` (string, required): The exact shell command to execute.
- `description` (string, optional): A brief description of the command's
  purpose, which will be shown to the user.
- `directory` (string, optional): The directory (relative to the project root)
  in which to execute the command. If not provided, the command runs in the
  project root.

## How to use `run_shell_command` with the Gemini CLI

When using `run_shell_command`, the command is executed as a subprocess.
`run_shell_command` can start background processes using `&`. The tool returns
detailed information about the execution, including:

- `Command`: The command that was executed.
- `Directory`: The directory where the command was run.
- `Stdout`: Output from the standard output stream.
- `Stderr`: Output from the standard error stream.
- `Error`: Any error message reported by the subprocess.
- `Exit Code`: The exit code of the command.
- `Signal`: The signal number if the command was terminated by a signal.
- `Background PIDs`: A list of PIDs for any background processes started.

Usage:

```
run_shell_command(command="Your commands.", description="Your description of the command.", directory="Your execution directory.")
```

## `run_shell_command` examples

List files in the current directory:

```
run_shell_command(command="ls -la")
```

Run a script in a specific directory:

```
run_shell_command(command="./my_script.sh", directory="scripts", description="Run my custom script")
```

Start a background server:

```
run_shell_command(command="npm run dev &", description="Start development server in background")
```

## Configuration

You can configure the behavior of the `run_shell_command` tool by modifying your
`settings.json` file or by using the `/settings` command in the Gemini CLI.

### Enabling interactive commands

To enable interactive commands, you need to set the
`tools.shell.enableInteractiveShell` setting to `true`. This will use `node-pty`
for shell command execution, which allows for interactive sessions. If
`node-pty` is not available, it will fall back to the `child_process`
implementation, which does not support interactive commands.

**Example `settings.json`:**

```json
{
  "tools": {
    "shell": {
      "enableInteractiveShell": true
    }
  }
}
```

### Showing color in output

To show color in the shell output, you need to set the `tools.shell.showColor`
setting to `true`. **Note: This setting only applies when
`tools.shell.enableInteractiveShell` is enabled.**

**Example `settings.json`:**

```json
{
  "tools": {
    "shell": {
      "showColor": true
    }
  }
}
```

### Setting the pager

You can set a custom pager for the shell output by setting the
`tools.shell.pager` setting. The default pager is `cat`. **Note: This setting
only applies when `tools.shell.enableInteractiveShell` is enabled.**

**Example `settings.json`:**

```json
{
  "tools": {
    "shell": {
      "pager": "less"
    }
  }
}
```

## Interactive commands

The `run_shell_command` tool now supports interactive commands by integrating a
pseudo-terminal (pty). This allows you to run commands that require real-time
user input, such as text editors (`vim`, `nano`), terminal-based UIs (`htop`),
and interactive version control operations (`git rebase -i`).

When an interactive command is running, you can send input to it from the Gemini
CLI. To focus on the interactive shell, press `Tab`. The terminal output,
including complex TUIs, will be rendered correctly.

## Important notes

- **Security:** Be cautious when executing commands, especially those
  constructed from user input, to prevent security vulnerabilities.
- **Error handling:** Check the `Stderr`, `Error`, and `Exit Code` fields to
  determine if a command executed successfully.
- **Background processes:** When a command is run in the background with `&`,
  the tool will return immediately and the process will continue to run in the
  background. The `Background PIDs` field will contain the process ID of the
  background process.

## Environment variables

When `run_shell_command` executes a command, it sets the `GEMINI_CLI=1`
environment variable in the subprocess's environment. This allows scripts or
tools to detect if they are being run from within the Gemini CLI.

## Command restrictions

You can restrict the commands that can be executed by the `run_shell_command`
tool by using the `tools.core` and `tools.exclude` settings in your
configuration file.

- `tools.core`: To restrict `run_shell_command` to a specific set of commands,
  add entries to the `core` list under the `tools` category in the format
  `run_shell_command(<command>)`. For example,
  `"tools": {"core": ["run_shell_command(git)"]}` will only allow `git`
  commands. Including the generic `run_shell_command` acts as a wildcard,
  allowing any command not explicitly blocked.
- `tools.exclude`: To block specific commands, add entries to the `exclude` list
  under the `tools` category in the format `run_shell_command(<command>)`. For
  example, `"tools": {"exclude": ["run_shell_command(rm)"]}` will block `rm`
  commands.

The validation logic is designed to be secure and flexible:

1.  **Command chaining disabled**: The tool automatically splits commands
    chained with `&&`, `||`, or `;` and validates each part separately. If any
    part of the chain is disallowed, the entire command is blocked.
2.  **Prefix matching**: The tool uses prefix matching. For example, if you
    allow `git`, you can run `git status` or `git log`.
3.  **Blocklist precedence**: The `tools.exclude` list is always checked first.
    If a command matches a blocked prefix, it will be denied, even if it also
    matches an allowed prefix in `tools.core`.

### Command restriction examples

**Allow only specific command prefixes**

To allow only `git` and `npm` commands, and block all others:

```json
{
  "tools": {
    "core": ["run_shell_command(git)", "run_shell_command(npm)"]
  }
}
```

- `git status`: Allowed
- `npm install`: Allowed
- `ls -l`: Blocked

**Block specific command prefixes**

To block `rm` and allow all other commands:

```json
{
  "tools": {
    "core": ["run_shell_command"],
    "exclude": ["run_shell_command(rm)"]
  }
}
```

- `rm -rf /`: Blocked
- `git status`: Allowed
- `npm install`: Allowed

**Blocklist takes precedence**

If a command prefix is in both `tools.core` and `tools.exclude`, it will be
blocked.

```json
{
  "tools": {
    "core": ["run_shell_command(git)"],
    "exclude": ["run_shell_command(git push)"]
  }
}
```

- `git push origin main`: Blocked
- `git status`: Allowed

**Block all shell commands**

To block all shell commands, add the `run_shell_command` wildcard to
`tools.exclude`:

```json
{
  "tools": {
    "exclude": ["run_shell_command"]
  }
}
```

- `ls -l`: Blocked
- `any other command`: Blocked

## Security note for `excludeTools`

Command-specific restrictions in `excludeTools` for `run_shell_command` are
based on simple string matching and can be easily bypassed. This feature is
**not a security mechanism** and should not be relied upon to safely execute
untrusted code. It is recommended to use `coreTools` to explicitly select
commands that can be executed.


---



# Web fetch tool
(`web_fetch`)

This document describes the `web_fetch` tool for the Gemini CLI.

## Description

Use `web_fetch` to summarize, compare, or extract information from web pages.
The `web_fetch` tool processes content from one or more URLs (up to 20) embedded
in a prompt. `web_fetch` takes a natural language prompt and returns a generated
response.

### Arguments

`web_fetch` takes one argument:

- `prompt` (string, required): A comprehensive prompt that includes the URL(s)
  (up to 20) to fetch and specific instructions on how to process their content.
  For example:
  `"Summarize https://example.com/article and extract key points from https://another.com/data"`.
  The prompt must contain at least one URL starting with `http://` or
  `https://`.

## How to use `web_fetch` with the Gemini CLI

To use `web_fetch` with the Gemini CLI, provide a natural language prompt that
contains URLs. The tool will ask for confirmation before fetching any URLs. Once
confirmed, the tool will process URLs through Gemini API's `urlContext`.

If the Gemini API cannot access the URL, the tool will fall back to fetching
content directly from the local machine. The tool will format the response,
including source attribution and citations where possible. The tool will then
provide the response to the user.

Usage:

```
web_fetch(prompt="Your prompt, including a URL such as https://google.com.")
```

## `web_fetch` examples

Summarize a single article:

```
web_fetch(prompt="Can you summarize the main points of https://example.com/news/latest")
```

Compare two articles:

```
web_fetch(prompt="What are the differences in the conclusions of these two papers: https://arxiv.org/abs/2401.0001 and https://arxiv.org/abs/2401.0002?")
```

## Important notes

- **URL processing:** `web_fetch` relies on the Gemini API's ability to access
  and process the given URLs.
- **Output quality:** The quality of the output will depend on the clarity of
  the instructions in the prompt.



---


# Web search tool
(`google_web_search`)

This document describes the `google_web_search` tool.

## Description

Use `google_web_search` to perform a web search using Google Search via the
Gemini API. The `google_web_search` tool returns a summary of web results with
sources.

### Arguments

`google_web_search` takes one argument:

- `query` (string, required): The search query.

## How to use `google_web_search` with the Gemini CLI

The `google_web_search` tool sends a query to the Gemini API, which then
performs a web search. `google_web_search` will return a generated response
based on the search results, including citations and sources.

Usage:

```
google_web_search(query="Your query goes here.")
```

## `google_web_search` examples

Get information on a topic:

```
google_web_search(query="latest advancements in AI-powered code generation")
```

## Important notes

- **Response returned:** The `google_web_search` tool returns a processed
  summary, not a raw list of search results.
- **Citations:** The response includes citations to the sources used to generate
  the summary.



---



# Memory tool
(`save_memory`)

This document describes the `save_memory` tool for the Gemini CLI.

## Description

Use `save_memory` to save and recall information across your Gemini CLI
sessions. With `save_memory`, you can direct the CLI to remember key details
across sessions, providing personalized and directed assistance.

### Arguments

`save_memory` takes one argument:

- `fact` (string, required): The specific fact or piece of information to
  remember. This should be a clear, self-contained statement written in natural
  language.

## How to use `save_memory` with the Gemini CLI

The tool appends the provided `fact` to a special `GEMINI.md` file located in
the user's home directory (`~/.gemini/GEMINI.md`). This file can be configured
to have a different name.

Once added, the facts are stored under a `## Gemini Added Memories` section.
This file is loaded as context in subsequent sessions, allowing the CLI to
recall the saved information.

Usage:

```
save_memory(fact="Your fact here.")
```

### `save_memory` examples

Remember a user preference:

```
save_memory(fact="My preferred programming language is Python.")
```

Store a project-specific detail:

```
save_memory(fact="The project I'm currently working on is called 'gemini-cli'.")
```

## Important notes

- **General usage:** This tool should be used for concise, important facts. It
  is not intended for storing large amounts of data or conversational history.
- **Memory file:** The memory file is a plain text Markdown file, so you can
  view and edit it manually if needed.



---


# Todo tool
(`write_todos`)

This document describes the `write_todos` tool for the Gemini CLI.

## Description

The `write_todos` tool allows the Gemini agent to create and manage a list of
subtasks for complex user requests. This provides you, the user, with greater
visibility into the agent's plan and its current progress. It also helps with
alignment where the agent is less likely to lose track of its current goal.

### Arguments

`write_todos` takes one argument:

- `todos` (array of objects, required): The complete list of todo items. This
  replaces the existing list. Each item includes:
  - `description` (string): The task description.
  - `status` (string): The current status (`pending`, `in_progress`,
    `completed`, or `cancelled`).

## Behavior

The agent uses this tool to break down complex multi-step requests into a clear
plan.

- **Progress tracking:** The agent updates this list as it works, marking tasks
  as `completed` when done.
- **Single focus:** Only one task will be marked `in_progress` at a time,
  indicating exactly what the agent is currently working on.
- **Dynamic updates:** The plan may evolve as the agent discovers new
  information, leading to new tasks being added or unnecessary ones being
  cancelled.

When active, the current `in_progress` task is displayed above the input box,
keeping you informed of the immediate action. You can toggle the full view of
the todo list at any time by pressing `Ctrl+T`.

Usage example (internal representation):

```javascript
write_todos({
  todos: [
    { description: 'Initialize new React project', status: 'completed' },
    { description: 'Implement state management', status: 'in_progress' },
    { description: 'Create API service', status: 'pending' },
  ],
});
```

## Important notes

- **Enabling:** This tool is enabled by default. You can disable it in your
  `settings.json` file by setting `"useWriteTodos": false`.

- **Intended use:** This tool is primarily used by the agent for complex,
  multi-turn tasks. It is generally not used for simple, single-turn questions.



---


# MCP servers with the Gemini CLI

This document provides a guide to configuring and using Model Context Protocol
(MCP) servers with the Gemini CLI.

## What is an MCP server?

An MCP server is an application that exposes tools and resources to the Gemini
CLI through the Model Context Protocol, allowing it to interact with external
systems and data sources. MCP servers act as a bridge between the Gemini model
and your local environment or other services like APIs.

An MCP server enables the Gemini CLI to:

- **Discover tools:** List available tools, their descriptions, and parameters
  through standardized schema definitions.
- **Execute tools:** Call specific tools with defined arguments and receive
  structured responses.
- **Access resources:** Read data from specific resources that the server
  exposes (files, API payloads, reports, etc.).

With an MCP server, you can extend the Gemini CLI's capabilities to perform
actions beyond its built-in features, such as interacting with databases, APIs,
custom scripts, or specialized workflows.

## Core integration architecture

The Gemini CLI integrates with MCP servers through a sophisticated discovery and
execution system built into the core package (`packages/core/src/tools/`):

### Discovery Layer (`mcp-client.ts`)

The discovery process is orchestrated by `discoverMcpTools()`, which:

1. **Iterates through configured servers** from your `settings.json`
   `mcpServers` configuration
2. **Establishes connections** using appropriate transport mechanisms (Stdio,
   SSE, or Streamable HTTP)
3. **Fetches tool definitions** from each server using the MCP protocol
4. **Sanitizes and validates** tool schemas for compatibility with the Gemini
   API
5. **Registers tools** in the global tool registry with conflict resolution
6. **Fetches and registers resources** if the server exposes any

### Execution layer (`mcp-tool.ts`)

Each discovered MCP tool is wrapped in a `DiscoveredMCPTool` instance that:

- **Handles confirmation logic** based on server trust settings and user
  preferences
- **Manages tool execution** by calling the MCP server with proper parameters
- **Processes responses** for both the LLM context and user display
- **Maintains connection state** and handles timeouts

### Transport mechanisms

The Gemini CLI supports three MCP transport types:

- **Stdio Transport:** Spawns a subprocess and communicates via stdin/stdout
- **SSE Transport:** Connects to Server-Sent Events endpoints
- **Streamable HTTP Transport:** Uses HTTP streaming for communication

## Working with MCP resources

Some MCP servers expose contextual “resources” in addition to the tools and
prompts. Gemini CLI discovers these automatically and gives you the possibility
to reference them in the chat.

### Discovery and listing

- When discovery runs, the CLI fetches each server’s `resources/list` results.
- The `/mcp` command displays a Resources section alongside Tools and Prompts
  for every connected server.

This returns a concise, plain-text list of URIs plus metadata.

### Referencing resources in a conversation

You can use the same `@` syntax already known for referencing local files:

```
@server://resource/path
```

Resource URIs appear in the completion menu together with filesystem paths. When
you submit the message, the CLI calls `resources/read` and injects the content
in the conversation.

## How to set up your MCP server

The Gemini CLI uses the `mcpServers` configuration in your `settings.json` file
to locate and connect to MCP servers. This configuration supports multiple
servers with different transport mechanisms.

### Configure the MCP server in settings.json

You can configure MCP servers in your `settings.json` file in two main ways:
through the top-level `mcpServers` object for specific server definitions, and
through the `mcp` object for global settings that control server discovery and
execution.

#### Global MCP settings (`mcp`)

The `mcp` object in your `settings.json` allows you to define global rules for
all MCP servers.

- **`mcp.serverCommand`** (string): A global command to start an MCP server.
- **`mcp.allowed`** (array of strings): A list of MCP server names to allow. If
  this is set, only servers from this list (matching the keys in the
  `mcpServers` object) will be connected to.
- **`mcp.excluded`** (array of strings): A list of MCP server names to exclude.
  Servers in this list will not be connected to.

**Example:**

```json
{
  "mcp": {
    "allowed": ["my-trusted-server"],
    "excluded": ["experimental-server"]
  }
}
```

#### Server-specific configuration (`mcpServers`)

The `mcpServers` object is where you define each individual MCP server you want
the CLI to connect to.

### Configuration structure

Add an `mcpServers` object to your `settings.json` file:

```json
{ ...file contains other config objects
  "mcpServers": {
    "serverName": {
      "command": "path/to/server",
      "args": ["--arg1", "value1"],
      "env": {
        "API_KEY": "$MY_API_TOKEN"
      },
      "cwd": "./server-directory",
      "timeout": 30000,
      "trust": false
    }
  }
}
```

### Configuration properties

Each server configuration supports the following properties:

#### Required (one of the following)

- **`command`** (string): Path to the executable for Stdio transport
- **`url`** (string): SSE endpoint URL (e.g., `"http://localhost:8080/sse"`)
- **`httpUrl`** (string): HTTP streaming endpoint URL

#### Optional

- **`args`** (string[]): Command-line arguments for Stdio transport
- **`headers`** (object): Custom HTTP headers when using `url` or `httpUrl`
- **`env`** (object): Environment variables for the server process. Values can
  reference environment variables using `$VAR_NAME` or `${VAR_NAME}` syntax
- **`cwd`** (string): Working directory for Stdio transport
- **`timeout`** (number): Request timeout in milliseconds (default: 600,000ms =
  10 minutes)
- **`trust`** (boolean): When `true`, bypasses all tool call confirmations for
  this server (default: `false`)
- **`includeTools`** (string[]): List of tool names to include from this MCP
  server. When specified, only the tools listed here will be available from this
  server (allowlist behavior). If not specified, all tools from the server are
  enabled by default.
- **`excludeTools`** (string[]): List of tool names to exclude from this MCP
  server. Tools listed here will not be available to the model, even if they are
  exposed by the server. **Note:** `excludeTools` takes precedence over
  `includeTools` - if a tool is in both lists, it will be excluded.
- **`targetAudience`** (string): The OAuth Client ID allowlisted on the
  IAP-protected application you are trying to access. Used with
  `authProviderType: 'service_account_impersonation'`.
- **`targetServiceAccount`** (string): The email address of the Google Cloud
  Service Account to impersonate. Used with
  `authProviderType: 'service_account_impersonation'`.

### OAuth support for remote MCP servers

The Gemini CLI supports OAuth 2.0 authentication for remote MCP servers using
SSE or HTTP transports. This enables secure access to MCP servers that require
authentication.

#### Automatic OAuth discovery

For servers that support OAuth discovery, you can omit the OAuth configuration
and let the CLI discover it automatically:

```json
{
  "mcpServers": {
    "discoveredServer": {
      "url": "https://api.example.com/sse"
    }
  }
}
```

The CLI will automatically:

- Detect when a server requires OAuth authentication (401 responses)
- Discover OAuth endpoints from server metadata
- Perform dynamic client registration if supported
- Handle the OAuth flow and token management

#### Authentication flow

When connecting to an OAuth-enabled server:

1. **Initial connection attempt** fails with 401 Unauthorized
2. **OAuth discovery** finds authorization and token endpoints
3. **Browser opens** for user authentication (requires local browser access)
4. **Authorization code** is exchanged for access tokens
5. **Tokens are stored** securely for future use
6. **Connection retry** succeeds with valid tokens

#### Browser redirect requirements

**Important:** OAuth authentication requires that your local machine can:

- Open a web browser for authentication
- Receive redirects on `http://localhost:7777/oauth/callback`

This feature will not work in:

- Headless environments without browser access
- Remote SSH sessions without X11 forwarding
- Containerized environments without browser support

#### Managing OAuth authentication

Use the `/mcp auth` command to manage OAuth authentication:

```bash
# List servers requiring authentication
/mcp auth

# Authenticate with a specific server
/mcp auth serverName

# Re-authenticate if tokens expire
/mcp auth serverName
```

#### OAuth configuration properties

- **`enabled`** (boolean): Enable OAuth for this server
- **`clientId`** (string): OAuth client identifier (optional with dynamic
  registration)
- **`clientSecret`** (string): OAuth client secret (optional for public clients)
- **`authorizationUrl`** (string): OAuth authorization endpoint (auto-discovered
  if omitted)
- **`tokenUrl`** (string): OAuth token endpoint (auto-discovered if omitted)
- **`scopes`** (string[]): Required OAuth scopes
- **`redirectUri`** (string): Custom redirect URI (defaults to
  `http://localhost:7777/oauth/callback`)
- **`tokenParamName`** (string): Query parameter name for tokens in SSE URLs
- **`audiences`** (string[]): Audiences the token is valid for

#### Token management

OAuth tokens are automatically:

- **Stored securely** in `~/.gemini/mcp-oauth-tokens.json`
- **Refreshed** when expired (if refresh tokens are available)
- **Validated** before each connection attempt
- **Cleaned up** when invalid or expired

#### Authentication provider type

You can specify the authentication provider type using the `authProviderType`
property:

- **`authProviderType`** (string): Specifies the authentication provider. Can be
  one of the following:
  - **`dynamic_discovery`** (default): The CLI will automatically discover the
    OAuth configuration from the server.
  - **`google_credentials`**: The CLI will use the Google Application Default
    Credentials (ADC) to authenticate with the server. When using this provider,
    you must specify the required scopes.
  - **`service_account_impersonation`**: The CLI will impersonate a Google Cloud
    Service Account to authenticate with the server. This is useful for
    accessing IAP-protected services (this was specifically designed for Cloud
    Run services).

#### Google credentials

```json
{
  "mcpServers": {
    "googleCloudServer": {
      "httpUrl": "https://my-gcp-service.run.app/mcp",
      "authProviderType": "google_credentials",
      "oauth": {
        "scopes": ["https://www.googleapis.com/auth/userinfo.email"]
      }
    }
  }
}
```

#### Service account impersonation

To authenticate with a server using Service Account Impersonation, you must set
the `authProviderType` to `service_account_impersonation` and provide the
following properties:

- **`targetAudience`** (string): The OAuth Client ID allowslisted on the
  IAP-protected application you are trying to access.
- **`targetServiceAccount`** (string): The email address of the Google Cloud
  Service Account to impersonate.

The CLI will use your local Application Default Credentials (ADC) to generate an
OIDC ID token for the specified service account and audience. This token will
then be used to authenticate with the MCP server.

#### Setup instructions

1. **[Create](https://cloud.google.com/iap/docs/oauth-client-creation) or use an
   existing OAuth 2.0 client ID.** To use an existing OAuth 2.0 client ID,
   follow the steps in
   [How to share OAuth Clients](https://cloud.google.com/iap/docs/sharing-oauth-clients).
2. **Add the OAuth ID to the allowlist for
   [programmatic access](https://cloud.google.com/iap/docs/sharing-oauth-clients#programmatic_access)
   for the application.** Since Cloud Run is not yet a supported resource type
   in gcloud iap, you must allowlist the Client ID on the project.
3. **Create a service account.**
   [Documentation](https://cloud.google.com/iam/docs/service-accounts-create#creating),
   [Cloud Console Link](https://console.cloud.google.com/iam-admin/serviceaccounts)
4. **Add both the service account and users to the IAP Policy** in the
   "Security" tab of the Cloud Run service itself or via gcloud.
5. **Grant all users and groups** who will access the MCP Server the necessary
   permissions to
   [impersonate the service account](https://cloud.google.com/docs/authentication/use-service-account-impersonation)
   (i.e., `roles/iam.serviceAccountTokenCreator`).
6. **[Enable](https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com)
   the IAM Credentials API** for your project.

### Example configurations

#### Python MCP server (stdio)

```json
{
  "mcpServers": {
    "pythonTools": {
      "command": "python",
      "args": ["-m", "my_mcp_server", "--port", "8080"],
      "cwd": "./mcp-servers/python",
      "env": {
        "DATABASE_URL": "$DB_CONNECTION_STRING",
        "API_KEY": "${EXTERNAL_API_KEY}"
      },
      "timeout": 15000
    }
  }
}
```

#### Node.js MCP server (stdio)

```json
{
  "mcpServers": {
    "nodeServer": {
      "command": "node",
      "args": ["dist/server.js", "--verbose"],
      "cwd": "./mcp-servers/node",
      "trust": true
    }
  }
}
```

#### Docker-based MCP server

```json
{
  "mcpServers": {
    "dockerizedServer": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "-e",
        "API_KEY",
        "-v",
        "${PWD}:/workspace",
        "my-mcp-server:latest"
      ],
      "env": {
        "API_KEY": "$EXTERNAL_SERVICE_TOKEN"
      }
    }
  }
}
```

#### HTTP-based MCP server

```json
{
  "mcpServers": {
    "httpServer": {
      "httpUrl": "http://localhost:3000/mcp",
      "timeout": 5000
    }
  }
}
```

#### HTTP-based MCP Server with custom headers

```json
{
  "mcpServers": {
    "httpServerWithAuth": {
      "httpUrl": "http://localhost:3000/mcp",
      "headers": {
        "Authorization": "Bearer your-api-token",
        "X-Custom-Header": "custom-value",
        "Content-Type": "application/json"
      },
      "timeout": 5000
    }
  }
}
```

#### MCP server with tool filtering

```json
{
  "mcpServers": {
    "filteredServer": {
      "command": "python",
      "args": ["-m", "my_mcp_server"],
      "includeTools": ["safe_tool", "file_reader", "data_processor"],
      // "excludeTools": ["dangerous_tool", "file_deleter"],
      "timeout": 30000
    }
  }
}
```

### SSE MCP server with SA impersonation

```json
{
  "mcpServers": {
    "myIapProtectedServer": {
      "url": "https://my-iap-service.run.app/sse",
      "authProviderType": "service_account_impersonation",
      "targetAudience": "YOUR_IAP_CLIENT_ID.apps.googleusercontent.com",
      "targetServiceAccount": "your-sa@your-project.iam.gserviceaccount.com"
    }
  }
}
```

## Discovery process deep dive

When the Gemini CLI starts, it performs MCP server discovery through the
following detailed process:

### 1. Server iteration and connection

For each configured server in `mcpServers`:

1. **Status tracking begins:** Server status is set to `CONNECTING`
2. **Transport selection:** Based on configuration properties:
   - `httpUrl` → `StreamableHTTPClientTransport`
   - `url` → `SSEClientTransport`
   - `command` → `StdioClientTransport`
3. **Connection establishment:** The MCP client attempts to connect with the
   configured timeout
4. **Error handling:** Connection failures are logged and the server status is
   set to `DISCONNECTED`

### 2. Tool discovery

Upon successful connection:

1. **Tool listing:** The client calls the MCP server's tool listing endpoint
2. **Schema validation:** Each tool's function declaration is validated
3. **Tool filtering:** Tools are filtered based on `includeTools` and
   `excludeTools` configuration
4. **Name sanitization:** Tool names are cleaned to meet Gemini API
   requirements:
   - Invalid characters (non-alphanumeric, underscore, dot, hyphen) are replaced
     with underscores
   - Names longer than 63 characters are truncated with middle replacement
     (`___`)

### 3. Conflict resolution

When multiple servers expose tools with the same name:

1. **First registration wins:** The first server to register a tool name gets
   the unprefixed name
2. **Automatic prefixing:** Subsequent servers get prefixed names:
   `serverName__toolName`
3. **Registry tracking:** The tool registry maintains mappings between server
   names and their tools

### 4. Schema processing

Tool parameter schemas undergo sanitization for Gemini API compatibility:

- **`$schema` properties** are removed
- **`additionalProperties`** are stripped
- **`anyOf` with `default`** have their default values removed (Vertex AI
  compatibility)
- **Recursive processing** applies to nested schemas

### 5. Connection management

After discovery:

- **Persistent connections:** Servers that successfully register tools maintain
  their connections
- **Cleanup:** Servers that provide no usable tools have their connections
  closed
- **Status updates:** Final server statuses are set to `CONNECTED` or
  `DISCONNECTED`

## Tool execution flow

When the Gemini model decides to use an MCP tool, the following execution flow
occurs:

### 1. Tool invocation

The model generates a `FunctionCall` with:

- **Tool name:** The registered name (potentially prefixed)
- **Arguments:** JSON object matching the tool's parameter schema

### 2. Confirmation process

Each `DiscoveredMCPTool` implements sophisticated confirmation logic:

#### Trust-based bypass

```typescript
if (this.trust) {
  return false; // No confirmation needed
}
```

#### Dynamic allow-listing

The system maintains internal allow-lists for:

- **Server-level:** `serverName` → All tools from this server are trusted
- **Tool-level:** `serverName.toolName` → This specific tool is trusted

#### User choice handling

When confirmation is required, users can choose:

- **Proceed once:** Execute this time only
- **Always allow this tool:** Add to tool-level allow-list
- **Always allow this server:** Add to server-level allow-list
- **Cancel:** Abort execution

### 3. Execution

Upon confirmation (or trust bypass):

1. **Parameter preparation:** Arguments are validated against the tool's schema
2. **MCP call:** The underlying `CallableTool` invokes the server with:

   ```typescript
   const functionCalls = [
     {
       name: this.serverToolName, // Original server tool name
       args: params,
     },
   ];
   ```

3. **Response processing:** Results are formatted for both LLM context and user
   display

### 4. Response handling

The execution result contains:

- **`llmContent`:** Raw response parts for the language model's context
- **`returnDisplay`:** Formatted output for user display (often JSON in markdown
  code blocks)

## How to interact with your MCP server

### Using the `/mcp` command

The `/mcp` command provides comprehensive information about your MCP server
setup:

```bash
/mcp
```

This displays:

- **Server list:** All configured MCP servers
- **Connection status:** `CONNECTED`, `CONNECTING`, or `DISCONNECTED`
- **Server details:** Configuration summary (excluding sensitive data)
- **Available tools:** List of tools from each server with descriptions
- **Discovery state:** Overall discovery process status

### Example `/mcp` output

```
MCP Servers Status:

📡 pythonTools (CONNECTED)
  Command: python -m my_mcp_server --port 8080
  Working Directory: ./mcp-servers/python
  Timeout: 15000ms
  Tools: calculate_sum, file_analyzer, data_processor

🔌 nodeServer (DISCONNECTED)
  Command: node dist/server.js --verbose
  Error: Connection refused

🐳 dockerizedServer (CONNECTED)
  Command: docker run -i --rm -e API_KEY my-mcp-server:latest
  Tools: docker__deploy, docker__status

Discovery State: COMPLETED
```

### Tool usage

Once discovered, MCP tools are available to the Gemini model like built-in
tools. The model will automatically:

1. **Select appropriate tools** based on your requests
2. **Present confirmation dialogs** (unless the server is trusted)
3. **Execute tools** with proper parameters
4. **Display results** in a user-friendly format

## Status monitoring and troubleshooting

### Connection states

The MCP integration tracks several states:

#### Server status (`MCPServerStatus`)

- **`DISCONNECTED`:** Server is not connected or has errors
- **`CONNECTING`:** Connection attempt in progress
- **`CONNECTED`:** Server is connected and ready

#### Discovery state (`MCPDiscoveryState`)

- **`NOT_STARTED`:** Discovery hasn't begun
- **`IN_PROGRESS`:** Currently discovering servers
- **`COMPLETED`:** Discovery finished (with or without errors)

### Common issues and solutions

#### Server won't connect

**Symptoms:** Server shows `DISCONNECTED` status

**Troubleshooting:**

1. **Check configuration:** Verify `command`, `args`, and `cwd` are correct
2. **Test manually:** Run the server command directly to ensure it works
3. **Check dependencies:** Ensure all required packages are installed
4. **Review logs:** Look for error messages in the CLI output
5. **Verify permissions:** Ensure the CLI can execute the server command

#### No tools discovered

**Symptoms:** Server connects but no tools are available

**Troubleshooting:**

1. **Verify tool registration:** Ensure your server actually registers tools
2. **Check MCP protocol:** Confirm your server implements the MCP tool listing
   correctly
3. **Review server logs:** Check stderr output for server-side errors
4. **Test tool listing:** Manually test your server's tool discovery endpoint

#### Tools not executing

**Symptoms:** Tools are discovered but fail during execution

**Troubleshooting:**

1. **Parameter validation:** Ensure your tool accepts the expected parameters
2. **Schema compatibility:** Verify your input schemas are valid JSON Schema
3. **Error handling:** Check if your tool is throwing unhandled exceptions
4. **Timeout issues:** Consider increasing the `timeout` setting

#### Sandbox compatibility

**Symptoms:** MCP servers fail when sandboxing is enabled

**Solutions:**

1. **Docker-based servers:** Use Docker containers that include all dependencies
2. **Path accessibility:** Ensure server executables are available in the
   sandbox
3. **Network access:** Configure sandbox to allow necessary network connections
4. **Environment variables:** Verify required environment variables are passed
   through

### Debugging tips

1. **Enable debug mode:** Run the CLI with `--debug` for verbose output (use F12
   to open debug console in interactive mode)
2. **Check stderr:** MCP server stderr is captured and logged (INFO messages
   filtered)
3. **Test isolation:** Test your MCP server independently before integrating
4. **Incremental setup:** Start with simple tools before adding complex
   functionality
5. **Use `/mcp` frequently:** Monitor server status during development

## Important notes

### Security considerations

- **Trust settings:** The `trust` option bypasses all confirmation dialogs. Use
  cautiously and only for servers you completely control
- **Access tokens:** Be security-aware when configuring environment variables
  containing API keys or tokens
- **Sandbox compatibility:** When using sandboxing, ensure MCP servers are
  available within the sandbox environment
- **Private data:** Using broadly scoped personal access tokens can lead to
  information leakage between repositories

### Performance and resource management

- **Connection persistence:** The CLI maintains persistent connections to
  servers that successfully register tools
- **Automatic cleanup:** Connections to servers providing no tools are
  automatically closed
- **Timeout management:** Configure appropriate timeouts based on your server's
  response characteristics
- **Resource monitoring:** MCP servers run as separate processes and consume
  system resources

### Schema compatibility

- **Property stripping:** The system automatically removes certain schema
  properties (`$schema`, `additionalProperties`) for Gemini API compatibility
- **Name sanitization:** Tool names are automatically sanitized to meet API
  requirements
- **Conflict resolution:** Tool name conflicts between servers are resolved
  through automatic prefixing

This comprehensive integration makes MCP servers a powerful way to extend the
Gemini CLI's capabilities while maintaining security, reliability, and ease of
use.

## Returning rich content from tools

MCP tools are not limited to returning simple text. You can return rich,
multi-part content, including text, images, audio, and other binary data in a
single tool response. This allows you to build powerful tools that can provide
diverse information to the model in a single turn.

All data returned from the tool is processed and sent to the model as context
for its next generation, enabling it to reason about or summarize the provided
information.

### How it works

To return rich content, your tool's response must adhere to the MCP
specification for a
[`CallToolResult`](https://modelcontextprotocol.io/specification/2025-06-18/server/tools#tool-result).
The `content` field of the result should be an array of `ContentBlock` objects.
The Gemini CLI will correctly process this array, separating text from binary
data and packaging it for the model.

You can mix and match different content block types in the `content` array. The
supported block types include:

- `text`
- `image`
- `audio`
- `resource` (embedded content)
- `resource_link`

### Example: Returning text and an image

Here is an example of a valid JSON response from an MCP tool that returns both a
text description and an image:

```json
{
  "content": [
    {
      "type": "text",
      "text": "Here is the logo you requested."
    },
    {
      "type": "image",
      "data": "BASE64_ENCODED_IMAGE_DATA_HERE",
      "mimeType": "image/png"
    },
    {
      "type": "text",
      "text": "The logo was created in 2025."
    }
  ]
}
```

When the Gemini CLI receives this response, it will:

1.  Extract all the text and combine it into a single `functionResponse` part
    for the model.
2.  Present the image data as a separate `inlineData` part.
3.  Provide a clean, user-friendly summary in the CLI, indicating that both text
    and an image were received.

This enables you to build sophisticated tools that can provide rich, multi-modal
context to the Gemini model.

## MCP prompts as slash commands

In addition to tools, MCP servers can expose predefined prompts that can be
executed as slash commands within the Gemini CLI. This allows you to create
shortcuts for common or complex queries that can be easily invoked by name.

### Defining prompts on the server

Here's a small example of a stdio MCP server that defines prompts:

```ts
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';

const server = new McpServer({
  name: 'prompt-server',
  version: '1.0.0',
});

server.registerPrompt(
  'poem-writer',
  {
    title: 'Poem Writer',
    description: 'Write a nice haiku',
    argsSchema: { title: z.string(), mood: z.string().optional() },
  },
  ({ title, mood }) => ({
    messages: [
      {
        role: 'user',
        content: {
          type: 'text',
          text: `Write a haiku${mood ? ` with the mood ${mood}` : ''} called ${title}. Note that a haiku is 5 syllables followed by 7 syllables followed by 5 syllables `,
        },
      },
    ],
  }),
);

const transport = new StdioServerTransport();
await server.connect(transport);
```

This can be included in `settings.json` under `mcpServers` with:

```json
{
  "mcpServers": {
    "nodeServer": {
      "command": "node",
      "args": ["filename.ts"]
    }
  }
}
```

### Invoking prompts

Once a prompt is discovered, you can invoke it using its name as a slash
command. The CLI will automatically handle parsing arguments.

```bash
/poem-writer --title="Gemini CLI" --mood="reverent"
```

or, using positional arguments:

```bash
/poem-writer "Gemini CLI" reverent
```

When you run this command, the Gemini CLI executes the `prompts/get` method on
the MCP server with the provided arguments. The server is responsible for
substituting the arguments into the prompt template and returning the final
prompt text. The CLI then sends this prompt to the model for execution. This
provides a convenient way to automate and share common workflows.

## Managing MCP servers with `gemini mcp`

While you can always configure MCP servers by manually editing your
`settings.json` file, the Gemini CLI provides a convenient set of commands to
manage your server configurations programmatically. These commands streamline
the process of adding, listing, and removing MCP servers without needing to
directly edit JSON files.

### Adding a server (`gemini mcp add`)

The `add` command configures a new MCP server in your `settings.json`. Based on
the scope (`-s, --scope`), it will be added to either the user config
`~/.gemini/settings.json` or the project config `.gemini/settings.json` file.

**Command:**

```bash
gemini mcp add [options] <name> <commandOrUrl> [args...]
```

- `<name>`: A unique name for the server.
- `<commandOrUrl>`: The command to execute (for `stdio`) or the URL (for
  `http`/`sse`).
- `[args...]`: Optional arguments for a `stdio` command.

**Options (flags):**

- `-s, --scope`: Configuration scope (user or project). [default: "project"]
- `-t, --transport`: Transport type (stdio, sse, http). [default: "stdio"]
- `-e, --env`: Set environment variables (e.g. -e KEY=value).
- `-H, --header`: Set HTTP headers for SSE and HTTP transports (e.g. -H
  "X-Api-Key: abc123" -H "Authorization: Bearer abc123").
- `--timeout`: Set connection timeout in milliseconds.
- `--trust`: Trust the server (bypass all tool call confirmation prompts).
- `--description`: Set the description for the server.
- `--include-tools`: A comma-separated list of tools to include.
- `--exclude-tools`: A comma-separated list of tools to exclude.

#### Adding an stdio server

This is the default transport for running local servers.

```bash
# Basic syntax
gemini mcp add [options] <name> <command> [args...]

# Example: Adding a local server
gemini mcp add -e API_KEY=123 -e DEBUG=true my-stdio-server /path/to/server arg1 arg2 arg3

# Example: Adding a local python server
gemini mcp add python-server python server.py -- --server-arg my-value
```

#### Adding an HTTP server

This transport is for servers that use the streamable HTTP transport.

```bash
# Basic syntax
gemini mcp add --transport http <name> <url>

# Example: Adding an HTTP server
gemini mcp add --transport http http-server https://api.example.com/mcp/

# Example: Adding an HTTP server with an authentication header
gemini mcp add --transport http --header "Authorization: Bearer abc123" secure-http https://api.example.com/mcp/
```

#### Adding an SSE server

This transport is for servers that use Server-Sent Events (SSE).

```bash
# Basic syntax
gemini mcp add --transport sse <name> <url>

# Example: Adding an SSE server
gemini mcp add --transport sse sse-server https://api.example.com/sse/

# Example: Adding an SSE server with an authentication header
gemini mcp add --transport sse --header "Authorization: Bearer abc123" secure-sse https://api.example.com/sse/
```

### Listing servers (`gemini mcp list`)

To view all MCP servers currently configured, use the `list` command. It
displays each server's name, configuration details, and connection status. This
command has no flags.

**Command:**

```bash
gemini mcp list
```

**Example output:**

```sh
✓ stdio-server: command: python3 server.py (stdio) - Connected
✓ http-server: https://api.example.com/mcp (http) - Connected
✗ sse-server: https://api.example.com/sse (sse) - Disconnected
```

### Removing a server (`gemini mcp remove`)

To delete a server from your configuration, use the `remove` command with the
server's name.

**Command:**

```bash
gemini mcp remove <name>
```

**Options (flags):**

- `-s, --scope`: Configuration scope (user or project). [default: "project"]

**Example:**

```bash
gemini mcp remove my-server
```

This will find and delete the "my-server" entry from the `mcpServers` object in
the appropriate `settings.json` file based on the scope (`-s, --scope`).

### Enabling/disabling a server (`gemini mcp enable`, `gemini mcp disable`)

Temporarily disable an MCP server without removing its configuration, or
re-enable a previously disabled server.

**Commands:**

```bash
gemini mcp enable <name> [--session]
gemini mcp disable <name> [--session]
```

**Options (flags):**

- `--session`: Apply change only for this session (not persisted to file).

Disabled servers appear in `/mcp` status as "Disabled" but won't connect or
provide tools. Enablement state is stored in
`~/.gemini/mcp-server-enablement.json`.

The same commands are available as slash commands during an active session:
`/mcp enable <name>` and `/mcp disable <name>`.

## Instructions

Gemini CLI supports
[MCP server instructions](https://modelcontextprotocol.io/specification/2025-06-18/schema#initializeresult),
which will be appended to the system instructions.


---


# Gemini CLI extensions

Gemini CLI extensions package prompts, MCP servers, custom commands, hooks, and
agent skills into a familiar and user-friendly format. With extensions, you can
expand the capabilities of Gemini CLI and share those capabilities with others.
They are designed to be easily installable and shareable.

To see examples of extensions, you can browse a gallery of
[Gemini CLI extensions](https://geminicli.com/extensions/browse/).

## Managing extensions

You can verify your installed extensions and their status using the interactive
command:

```bash
/extensions list
```

or in noninteractive mode:

```bash
gemini extensions list
```

## Installation

To install a real extension, you can use the `extensions install` command with a
GitHub repository URL in noninteractive mode. For example:

```bash
gemini extensions install https://github.com/gemini-cli-extensions/workspace
```

## Next steps

- [Writing extensions](/docs/extensions/writing-extensions): Learn how to create your first
  extension.
- [Extensions reference](/docs/extensions/reference): Deeply understand the extension format,
  commands, and configuration.
- [Best practices](/docs/extensions/best-practices): Learn strategies for building great
  extensions.
- [Extensions releasing](/docs/extensions/releasing): Learn how to share your extensions with
  the world.



---


# Getting started with Gemini CLI extensions

This guide will walk you through creating your first Gemini CLI extension.
You'll learn how to set up a new extension, add a custom tool via an MCP server,
create a custom command, and provide context to the model with a `GEMINI.md`
file.

## Prerequisites

Before you start, make sure you have the Gemini CLI installed and a basic
understanding of Node.js.

## When to use what

Extensions offer a variety of ways to customize Gemini CLI.

| Feature                                                        | What it is                                                                                                         | When to use it                                                                                                                                                                                                                                                                                 | Invoked by            |
| :------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------- |
| **[MCP server](/docs/extensions/reference#mcp-servers)**                     | A standard way to expose new tools and data sources to the model.                                                  | Use this when you want the model to be able to _do_ new things, like fetching data from an internal API, querying a database, or controlling a local application. We also support MCP resources (which can replace custom commands) and system instructions (which can replace custom context) | Model                 |
| **[Custom commands](/docs/cli/custom-commands)**               | A shortcut (like `/my-cmd`) that executes a pre-defined prompt or shell command.                                   | Use this for repetitive tasks or to save long, complex prompts that you use frequently. Great for automation.                                                                                                                                                                                  | User                  |
| **[Context file (`GEMINI.md`)](/docs/extensions/reference#contextfilename)** | A markdown file containing instructions that are loaded into the model's context at the start of every session.    | Use this to define the "personality" of your extension, set coding standards, or provide essential knowledge that the model should always have.                                                                                                                                                | CLI provides to model |
| **[Agent skills](/docs/cli/skills)**                           | A specialized set of instructions and workflows that the model activates only when needed.                         | Use this for complex, occasional tasks (like "create a PR" or "audit security") to avoid cluttering the main context window when the skill isn't being used.                                                                                                                                   | Model                 |
| **[Hooks](/docs/hooks)**                                 | A way to intercept and customize the CLI's behavior at specific lifecycle events (e.g., before/after a tool call). | Use this when you want to automate actions based on what the model is doing, like validating tool arguments, logging activity, or modifying the model's input/output.                                                                                                                          | CLI                   |

## Step 1: Create a new extension

The easiest way to start is by using one of the built-in templates. We'll use
the `mcp-server` example as our foundation.

Run the following command to create a new directory called `my-first-extension`
with the template files:

```bash
gemini extensions new my-first-extension mcp-server
```

This will create a new directory with the following structure:

```
my-first-extension/
├── example.js
├── gemini-extension.json
└── package.json
```

## Step 2: Understand the extension files

Let's look at the key files in your new extension.

### `gemini-extension.json`

This is the manifest file for your extension. It tells Gemini CLI how to load
and use your extension.

```json
{
  "name": "mcp-server-example",
  "version": "1.0.0",
  "mcpServers": {
    "nodeServer": {
      "command": "node",
      "args": ["${extensionPath}${/}example.js"],
      "cwd": "${extensionPath}"
    }
  }
}
```

- `name`: The unique name for your extension.
- `version`: The version of your extension.
- `mcpServers`: This section defines one or more Model Context Protocol (MCP)
  servers. MCP servers are how you can add new tools for the model to use.
  - `command`, `args`, `cwd`: These fields specify how to start your server.
    Notice the use of the `${extensionPath}` variable, which Gemini CLI replaces
    with the absolute path to your extension's installation directory. This
    allows your extension to work regardless of where it's installed.

### `example.js`

This file contains the source code for your MCP server. It's a simple Node.js
server that uses the `@modelcontextprotocol/sdk`.

```javascript
/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';

const server = new McpServer({
  name: 'prompt-server',
  version: '1.0.0',
});

// Registers a new tool named 'fetch_posts'
server.registerTool(
  'fetch_posts',
  {
    description: 'Fetches a list of posts from a public API.',
    inputSchema: z.object({}).shape,
  },
  async () => {
    const apiResponse = await fetch(
      'https://jsonplaceholder.typicode.com/posts',
    );
    const posts = await apiResponse.json();
    const response = { posts: posts.slice(0, 5) };
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(response),
        },
      ],
    };
  },
);

// ... (prompt registration omitted for brevity)

const transport = new StdioServerTransport();
await server.connect(transport);
```

This server defines a single tool called `fetch_posts` that fetches data from a
public API.

### `package.json`

This is the standard configuration file for a Node.js project. It defines
dependencies and scripts.

## Step 3: Link your extension

Before you can use the extension, you need to link it to your Gemini CLI
installation for local development.

1.  **Install dependencies:**

    ```bash
    cd my-first-extension
    npm install
    ```

2.  **Link the extension:**

    The `link` command creates a symbolic link from the Gemini CLI extensions
    directory to your development directory. This means any changes you make
    will be reflected immediately without needing to reinstall.

    ```bash
    gemini extensions link .
    ```

Now, restart your Gemini CLI session. The new `fetch_posts` tool will be
available. You can test it by asking: "fetch posts".

## Step 4: Add a custom command

Custom commands provide a way to create shortcuts for complex prompts. Let's add
a command that searches for a pattern in your code.

1.  Create a `commands` directory and a subdirectory for your command group:

    ```bash
    mkdir -p commands/fs
    ```

2.  Create a file named `commands/fs/grep-code.toml`:

    ```toml
    prompt = """
    Please summarize the findings for the pattern `{{args}}`.

    Search Results:
    !{grep -r {{args}} .}
    """
    ```

    This command, `/fs:grep-code`, will take an argument, run the `grep` shell
    command with it, and pipe the results into a prompt for summarization.

After saving the file, restart the Gemini CLI. You can now run
`/fs:grep-code "some pattern"` to use your new command.

## Step 5: Add a custom `GEMINI.md`

You can provide persistent context to the model by adding a `GEMINI.md` file to
your extension. This is useful for giving the model instructions on how to
behave or information about your extension's tools. Note that you may not always
need this for extensions built to expose commands and prompts.

1.  Create a file named `GEMINI.md` in the root of your extension directory:

    ```markdown
    # My First Extension Instructions

    You are an expert developer assistant. When the user asks you to fetch
    posts, use the `fetch_posts` tool. Be concise in your responses.
    ```

2.  Update your `gemini-extension.json` to tell the CLI to load this file:

    ```json
    {
      "name": "my-first-extension",
      "version": "1.0.0",
      "contextFileName": "GEMINI.md",
      "mcpServers": {
        "nodeServer": {
          "command": "node",
          "args": ["${extensionPath}${/}example.js"],
          "cwd": "${extensionPath}"
        }
      }
    }
    ```

Restart the CLI again. The model will now have the context from your `GEMINI.md`
file in every session where the extension is active.

## (Optional) Step 6: Add an Agent Skill

_Note: This is an experimental feature enabled via `experimental.skills`._

[Agent Skills](/docs/cli/skills) let you bundle specialized expertise and
procedural workflows. Unlike `GEMINI.md`, which provides persistent context,
skills are activated only when needed, saving context tokens.

1.  Create a `skills` directory and a subdirectory for your skill:

    ```bash
    mkdir -p skills/security-audit
    ```

2.  Create a `skills/security-audit/SKILL.md` file:

    ```markdown
    ---
    name: security-audit
    description:
      Expertise in auditing code for security vulnerabilities. Use when the user
      asks to "check for security issues" or "audit" their changes.
    ---

    # Security Auditor

    You are an expert security researcher. When auditing code:

    1. Look for common vulnerabilities (OWASP Top 10).
    2. Check for hardcoded secrets or API keys.
    3. Suggest remediation steps for any findings.
    ```

Skills bundled with your extension are automatically discovered and can be
activated by the model during a session when it identifies a relevant task.

## Step 7: Release your extension

Once you're happy with your extension, you can share it with others. The two
primary ways of releasing extensions are via a Git repository or through GitHub
Releases. Using a public Git repository is the simplest method.

For detailed instructions on both methods, please refer to the
[Extension Releasing Guide](/docs/extensions/releasing).

## Conclusion

You've successfully created a Gemini CLI extension! You learned how to:

- Bootstrap a new extension from a template.
- Add custom tools with an MCP server.
- Create convenient custom commands.
- Provide persistent context to the model.
- Bundle specialized Agent Skills.
- Link your extension for local development.

From here, you can explore more advanced features and build powerful new
capabilities into the Gemini CLI.


---


# Extensions reference

This guide covers the `gemini extensions` commands and the structure of the
`gemini-extension.json` configuration file.

## Extension management

We offer a suite of extension management tools using `gemini extensions`
commands.

Note that these commands (e.g. `gemini extensions install`) are not supported
from within the CLI's **interactive mode**, although you can list installed
extensions using the `/extensions list` slash command.

Note that all of these management operations (including updates to slash
commands) will only be reflected in active CLI sessions on **restart**.

### Installing an extension

You can install an extension using `gemini extensions install` with either a
GitHub URL or a local path.

Note that we create a copy of the installed extension, so you will need to run
`gemini extensions update` to pull in changes from both locally-defined
extensions and those on GitHub.

NOTE: If you are installing an extension from GitHub, you'll need to have `git`
installed on your machine. See
[git installation instructions](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
for help.

```
gemini extensions install <source> [--ref <ref>] [--auto-update] [--pre-release] [--consent]
```

- `<source>`: The github URL or local path of the extension to install.
- `--ref`: The git ref to install from.
- `--auto-update`: Enable auto-update for this extension.
- `--pre-release`: Enable pre-release versions for this extension.
- `--consent`: Acknowledge the security risks of installing an extension and
  skip the confirmation prompt.

### Uninstalling an extension

To uninstall one or more extensions, run
`gemini extensions uninstall <name...>`:

```
gemini extensions uninstall gemini-cli-security gemini-cli-another-extension
```

### Disabling an extension

Extensions are, by default, enabled across all workspaces. You can disable an
extension entirely or for specific workspace.

```
gemini extensions disable <name> [--scope <scope>]
```

- `<name>`: The name of the extension to disable.
- `--scope`: The scope to disable the extension in (`user` or `workspace`).

### Enabling an extension

You can enable extensions using `gemini extensions enable <name>`. You can also
enable an extension for a specific workspace using
`gemini extensions enable <name> --scope=workspace` from within that workspace.

```
gemini extensions enable <name> [--scope <scope>]
```

- `<name>`: The name of the extension to enable.
- `--scope`: The scope to enable the extension in (`user` or `workspace`).

### Updating an extension

For extensions installed from a local path or a git repository, you can
explicitly update to the latest version (as reflected in the
`gemini-extension.json` `version` field) with `gemini extensions update <name>`.

You can update all extensions with:

```
gemini extensions update --all
```

### Create a boilerplate extension

We offer several example extensions `context`, `custom-commands`,
`exclude-tools` and `mcp-server`. You can view these examples
[here](https://github.com/google-gemini/gemini-cli/tree/main/packages/cli/src/commands/extensions/examples).

To copy one of these examples into a development directory using the type of
your choosing, run:

```
gemini extensions new <path> [template]
```

- `<path>`: The path to create the extension in.
- `[template]`: The boilerplate template to use.

### Link a local extension

The `gemini extensions link` command will create a symbolic link from the
extension installation directory to the development path.

This is useful so you don't have to run `gemini extensions update` every time
you make changes you'd like to test.

```
gemini extensions link <path>
```

- `<path>`: The path of the extension to link.

## Extension format

On startup, Gemini CLI looks for extensions in `<home>/.gemini/extensions`

Extensions exist as a directory that contains a `gemini-extension.json` file.
For example:

`<home>/.gemini/extensions/my-extension/gemini-extension.json`

### `gemini-extension.json`

The `gemini-extension.json` file contains the configuration for the extension.
The file has the following structure:

```json
{
  "name": "my-extension",
  "version": "1.0.0",
  "description": "My awesome extension",
  "mcpServers": {
    "my-server": {
      "command": "node my-server.js"
    }
  },
  "contextFileName": "GEMINI.md",
  "excludeTools": ["run_shell_command"]
}
```

- `name`: The name of the extension. This is used to uniquely identify the
  extension and for conflict resolution when extension commands have the same
  name as user or project commands. The name should be lowercase or numbers and
  use dashes instead of underscores or spaces. This is how users will refer to
  your extension in the CLI. Note that we expect this name to match the
  extension directory name.
- `version`: The version of the extension.
- `description`: A short description of the extension. This will be displayed on
  [geminicli.com/extensions](https://geminicli.com/extensions).
- `mcpServers`: A map of MCP servers to settings. The key is the name of the
  server, and the value is the server configuration. These servers will be
  loaded on startup just like MCP servers settingsd in a
  [`settings.json` file](/docs/get-started/configuration). If both an extension
  and a `settings.json` file settings an MCP server with the same name, the
  server defined in the `settings.json` file takes precedence.
  - Note that all MCP server configuration options are supported except for
    `trust`.
- `contextFileName`: The name of the file that contains the context for the
  extension. This will be used to load the context from the extension directory.
  If this property is not used but a `GEMINI.md` file is present in your
  extension directory, then that file will be loaded.
- `excludeTools`: An array of tool names to exclude from the model. You can also
  specify command-specific restrictions for tools that support it, like the
  `run_shell_command` tool. For example,
  `"excludeTools": ["run_shell_command(rm -rf)"]` will block the `rm -rf`
  command. Note that this differs from the MCP server `excludeTools`
  functionality, which can be listed in the MCP server config.

When Gemini CLI starts, it loads all the extensions and merges their
configurations. If there are any conflicts, the workspace configuration takes
precedence.

### Settings

_Note: This is an experimental feature. We do not yet recommend extension
authors introduce settings as part of their core flows._

Extensions can define settings that the user will be prompted to provide upon
installation. This is useful for things like API keys, URLs, or other
configuration that the extension needs to function.

To define settings, add a `settings` array to your `gemini-extension.json` file.
Each object in the array should have the following properties:

- `name`: A user-friendly name for the setting.
- `description`: A description of the setting and what it's used for.
- `envVar`: The name of the environment variable that the setting will be stored
  as.
- `sensitive`: Optional boolean. If true, obfuscates the input the user provides
  and stores the secret in keychain storage. **Example**

```json
{
  "name": "my-api-extension",
  "version": "1.0.0",
  "settings": [
    {
      "name": "API Key",
      "description": "Your API key for the service.",
      "envVar": "MY_API_KEY"
    }
  ]
}
```

When a user installs this extension, they will be prompted to enter their API
key. The value will be saved to a `.env` file in the extension's directory
(e.g., `<home>/.gemini/extensions/my-api-extension/.env`).

You can view a list of an extension's settings by running:

```
gemini extensions list
```

and you can update a given setting using:

```
gemini extensions config <extension name> [setting name] [--scope <scope>]
```

- `--scope`: The scope to set the setting in (`user` or `workspace`). This is
  optional and will default to `user`.

### Custom commands

Extensions can provide [custom commands](/docs/cli/custom-commands) by placing
TOML files in a `commands/` subdirectory within the extension directory. These
commands follow the same format as user and project custom commands and use
standard naming conventions.

**Example**

An extension named `gcp` with the following structure:

```
.gemini/extensions/gcp/
├── gemini-extension.json
└── commands/
    ├── deploy.toml
    └── gcs/
        └── sync.toml
```

Would provide these commands:

- `/deploy` - Shows as `[gcp] Custom command from deploy.toml` in help
- `/gcs:sync` - Shows as `[gcp] Custom command from sync.toml` in help

### Hooks

Extensions can provide [hooks](/docs/hooks) to intercept and customize
Gemini CLI behavior at specific lifecycle events. Hooks provided by an extension
must be defined in a `hooks/hooks.json` file within the extension directory.

> [!IMPORTANT] Hooks are not defined directly in `gemini-extension.json`. The
> CLI specifically looks for the `hooks/hooks.json` file.

### Agent Skills

Extensions can bundle [Agent Skills](/docs/cli/skills) to provide specialized
workflows. Skills must be placed in a `skills/` directory within the extension.

**Example**

An extension with the following structure:

```
.gemini/extensions/my-extension/
├── gemini-extension.json
└── skills/
    └── security-audit/
        └── SKILL.md
```

Will expose a `security-audit` skill that the model can activate.

### Conflict resolution

Extension commands have the lowest precedence. When a conflict occurs with user
or project commands:

1. **No conflict**: Extension command uses its natural name (e.g., `/deploy`)
2. **With conflict**: Extension command is renamed with the extension prefix
   (e.g., `/gcp.deploy`)

For example, if both a user and the `gcp` extension define a `deploy` command:

- `/deploy` - Executes the user's deploy command
- `/gcp.deploy` - Executes the extension's deploy command (marked with `[gcp]`
  tag)

## Variables

Gemini CLI extensions allow variable substitution in `gemini-extension.json`.
This can be useful if e.g., you need the current directory to run an MCP server
using an argument like `"args": ["${extensionPath}${/}dist${/}server.js"]`.

**Supported variables:**

| variable                   | description                                                                                                                                                     |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `${extensionPath}`         | The fully-qualified path of the extension in the user's filesystem e.g., '/Users/username/.gemini/extensions/example-extension'. This will not unwrap symlinks. |
| `${workspacePath}`         | The fully-qualified path of the current workspace.                                                                                                              |
| `${/} or ${pathSeparator}` | The path separator (differs per OS).                                                                                                                            |



---


# Extensions on Gemini CLI: Best practices

This guide covers best practices for developing, securing, and maintaining
Gemini CLI extensions.

## Development

Developing extensions for Gemini CLI is intended to be a lightweight, iterative
process.

### Structure your extension

While simple extensions can just be a few files, we recommend a robust structure
for complex extensions:

```
my-extension/
├── package.json
├── tsconfig.json
├── gemini-extension.json
├── src/
│   ├── index.ts
│   └── tools/
└── dist/
```

- **Use TypeScript**: We strongly recommend using TypeScript for type safety and
  better tooling.
- **Separate source and build**: Keep your source code in `src` and build to
  `dist`.
- **Bundle dependencies**: If your extension has many dependencies, consider
  bundling them (e.g., with `esbuild` or `webpack`) to reduce install time and
  potential conflicts.

### Iterate with `link`

Use `gemini extensions link` to develop locally without constantly reinstalling:

```bash
cd my-extension
gemini extensions link .
```

Changes to your code (after rebuilding) will be immediately available in the CLI
on restart.

### Use `GEMINI.md` effectively

Your `GEMINI.md` file provides context to the model. Keep it focused:

- **Do:** Explain high-level goals and how to use the provided tools.
- **Don't:** Dump your entire documentation.
- **Do:** Use clear, concise language.

## Security

When building a Gemini CLI extension, follow general security best practices
(such as least privilege and input validation) to reduce risk.

### Minimal permissions

When defining tools in your MCP server, only request the permissions necessary.
Avoid giving the model broad access (like full shell access) if a more
restricted set of tools will suffice.

If you must use powerful tools like `run_shell_command`, consider restricting
them to specific commands in your `gemini-extension.json`:

```json
{
  "name": "my-safe-extension",
  "excludeTools": ["run_shell_command(rm -rf *)"]
}
```

This ensures that even if the model tries to execute a dangerous command, it
will be blocked at the CLI level.

### Validate inputs

Your MCP server is running on the user's machine. Always validate inputs to your
tools to prevent arbitrary code execution or filesystem access outside the
intended scope.

```typescript
// Good: Validating paths
if (!path.resolve(inputPath).startsWith(path.resolve(allowedDir) + path.sep)) {
  throw new Error('Access denied');
}
```

### Sensitive settings

If your extension requires API keys, use the `sensitive: true` option in
`gemini-extension.json`. This ensures keys are stored securely in the system
keychain and obfuscated in the UI.

```json
"settings": [
  {
    "name": "API Key",
    "envVar": "MY_API_KEY",
    "sensitive": true
  }
]
```

## Releasing

You can upload your extension directly to GitHub to list it in the gallery.
Gemini CLI extensions also offers support for more complicated
[releases](/docs/extensions/releasing).

### Semantic versioning

Follow [Semantic Versioning](https://semver.org/).

- **Major**: Breaking changes (renaming tools, changing arguments).
- **Minor**: New features (new tools, commands).
- **Patch**: Bug fixes.

### Release Channels

Use git branches to manage release channels (e.g., `main` for stable, `dev` for
bleeding edge). This allows users to choose their stability level:

```bash
# Stable
gemini extensions install github.com/user/repo

# Dev
gemini extensions install github.com/user/repo --ref dev
```

### Clean artifacts

If you are using GitHub Releases, ensure your release artifacts only contain the
necessary files (`dist/`, `gemini-extension.json`, `package.json`). Exclude
`node_modules` (users will install them) and `src/` to keep downloads small.


---


# Extension releasing

There are two primary ways of releasing extensions to users:

- [Git repository](#releasing-through-a-git-repository)
- [Github Releases](#releasing-through-github-releases)

Git repository releases tend to be the simplest and most flexible approach,
while GitHub releases can be more efficient on initial install as they are
shipped as single archives instead of requiring a git clone which downloads each
file individually. Github releases may also contain platform specific archives
if you need to ship platform specific binary files.

## Releasing through a git repository

This is the most flexible and simple option. All you need to do is create a
publicly accessible git repo (such as a public github repository) and then users
can install your extension using `gemini extensions install <your-repo-uri>`.
They can optionally depend on a specific ref (branch/tag/commit) using the
`--ref=<some-ref>` argument, this defaults to the default branch.

Whenever commits are pushed to the ref that a user depends on, they will be
prompted to update the extension. Note that this also allows for easy rollbacks,
the HEAD commit is always treated as the latest version regardless of the actual
version in the `gemini-extension.json` file.

### Managing release channels using a git repository

Users can depend on any ref from your git repo, such as a branch or tag, which
allows you to manage multiple release channels.

For instance, you can maintain a `stable` branch, which users can install this
way `gemini extensions install <your-repo-uri> --ref=stable`. Or, you could make
this the default by treating your default branch as your stable release branch,
and doing development in a different branch (for instance called `dev`). You can
maintain as many branches or tags as you like, providing maximum flexibility for
you and your users.

Note that these `ref` arguments can be tags, branches, or even specific commits,
which allows users to depend on a specific version of your extension. It is up
to you how you want to manage your tags and branches.

### Example releasing flow using a git repo

While there are many options for how you want to manage releases using a git
flow, we recommend treating your default branch as your "stable" release branch.
This means that the default behavior for
`gemini extensions install <your-repo-uri>` is to be on the stable release
branch.

Lets say you want to maintain three standard release channels, `stable`,
`preview`, and `dev`. You would do all your standard development in the `dev`
branch. When you are ready to do a preview release, you merge that branch into
your `preview` branch. When you are ready to promote your preview branch to
stable, you merge `preview` into your stable branch (which might be your default
branch or a different branch).

You can also cherry pick changes from one branch into another using
`git cherry-pick`, but do note that this will result in your branches having a
slightly divergent history from each other, unless you force push changes to
your branches on each release to restore the history to a clean slate (which may
not be possible for the default branch depending on your repository settings).
If you plan on doing cherry picks, you may want to avoid having your default
branch be the stable branch to avoid force-pushing to the default branch which
should generally be avoided.

## Releasing through GitHub releases

Gemini CLI extensions can be distributed through
[GitHub Releases](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases).
This provides a faster and more reliable initial installation experience for
users, as it avoids the need to clone the repository.

Each release includes at least one archive file, which contains the full
contents of the repo at the tag that it was linked to. Releases may also include
[pre-built archives](#custom-pre-built-archives) if your extension requires some
build step or has platform specific binaries attached to it.

When checking for updates, gemini will just look for the "latest" release on
github (you must mark it as such when creating the release), unless the user
installed a specific release by passing `--ref=<some-release-tag>`.

You may also install extensions with the `--pre-release` flag in order to get
the latest release regardless of whether it has been marked as "latest". This
allows you to test that your release works before actually pushing it to all
users.

### Custom pre-built archives

Custom archives must be attached directly to the github release as assets and
must be fully self-contained. This means they should include the entire
extension, see [archive structure](#archive-structure).

If your extension is platform-independent, you can provide a single generic
asset. In this case, there should be only one asset attached to the release.

Custom archives may also be used if you want to develop your extension within a
larger repository, you can build an archive which has a different layout from
the repo itself (for instance it might just be an archive of a subdirectory
containing the extension).

#### Platform specific archives

To ensure Gemini CLI can automatically find the correct release asset for each
platform, you must follow this naming convention. The CLI will search for assets
in the following order:

1.  **Platform and architecture-Specific:**
    `{platform}.{arch}.{name}.{extension}`
2.  **Platform-specific:** `{platform}.{name}.{extension}`
3.  **Generic:** If only one asset is provided, it will be used as a generic
    fallback.

- `{name}`: The name of your extension.
- `{platform}`: The operating system. Supported values are:
  - `darwin` (macOS)
  - `linux`
  - `win32` (Windows)
- `{arch}`: The architecture. Supported values are:
  - `x64`
  - `arm64`
- `{extension}`: The file extension of the archive (e.g., `.tar.gz` or `.zip`).

**Examples:**

- `darwin.arm64.my-tool.tar.gz` (specific to Apple Silicon Macs)
- `darwin.my-tool.tar.gz` (for all Macs)
- `linux.x64.my-tool.tar.gz`
- `win32.my-tool.zip`

#### Archive structure

Archives must be fully contained extensions and have all the standard
requirements - specifically the `gemini-extension.json` file must be at the root
of the archive.

The rest of the layout should look exactly the same as a typical extension, see
[extensions.md](/docs/extensions).

#### Example GitHub Actions workflow

Here is an example of a GitHub Actions workflow that builds and releases a
Gemini CLI extension for multiple platforms:

```yaml
name: Release Extension

on:
  push:
    tags:
      - 'v*'

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Node.js
        uses: actions/setup-node@v3
        with:
          node-version: '20'

      - name: Install dependencies
        run: npm ci

      - name: Build extension
        run: npm run build

      - name: Create release assets
        run: |
          npm run package -- --platform=darwin --arch=arm64
          npm run package -- --platform=linux --arch=x64
          npm run package -- --platform=win32 --arch=x64

      - name: Create GitHub Release
        uses: softprops/action-gh-release@v1
        with:
          files: |
            release/darwin.arm64.my-tool.tar.gz
            release/linux.arm64.my-tool.tar.gz
            release/win32.arm64.my-tool.zip
```



---


# Gemini CLI hooks
(experimental)

Hooks are scripts or programs that Gemini CLI executes at specific points in the
agentic loop, allowing you to intercept and customize behavior without modifying
the CLI's source code.

## Availability

> **Experimental Feature**: Hooks are currently enabled by default only in the
> **Preview** and **Nightly** release channels.

If you are on the Stable channel, you must explicitly enable the hooks system in
your `settings.json`:

```json
{
  "hooksConfig": {
    "enabled": true
  }
}
```

- **[Writing hooks guide](/docs/hooks/writing-hooks)**: A tutorial on creating
  your first hook with comprehensive examples.
- **[Hooks reference](/docs/hooks/reference)**: The definitive technical
  specification of I/O schemas and exit codes.
- **[Best practices](/docs/hooks/best-practices)**: Guidelines on security,
  performance, and debugging.

## What are hooks?

Hooks run synchronously as part of the agent loop—when a hook event fires,
Gemini CLI waits for all matching hooks to complete before continuing.

With hooks, you can:

- **Add context:** Inject relevant information (like git history) before the
  model processes a request.
- **Validate actions:** Review tool arguments and block potentially dangerous
  operations.
- **Enforce policies:** Implement security scanners and compliance checks.
- **Log interactions:** Track tool usage and model responses for auditing.
- **Optimize behavior:** Dynamically filter available tools or adjust model
  parameters.

## Core concepts

### Hook events

Hooks are triggered by specific events in Gemini CLI's lifecycle.

| Event                 | When It Fires                                  | Impact                 | Common Use Cases                             |
| --------------------- | ---------------------------------------------- | ---------------------- | -------------------------------------------- |
| `SessionStart`        | When a session begins (startup, resume, clear) | Inject Context         | Initialize resources, load context           |
| `SessionEnd`          | When a session ends (exit, clear)              | Advisory               | Clean up, save state                         |
| `BeforeAgent`         | After user submits prompt, before planning     | Block Turn / Context   | Add context, validate prompts, block turns   |
| `AfterAgent`          | When agent loop ends                           | Retry / Halt           | Review output, force retry or halt execution |
| `BeforeModel`         | Before sending request to LLM                  | Block Turn / Mock      | Modify prompts, swap models, mock responses  |
| `AfterModel`          | After receiving LLM response                   | Block Turn / Redact    | Filter/redact responses, log interactions    |
| `BeforeToolSelection` | Before LLM selects tools                       | Filter Tools           | Filter available tools, optimize selection   |
| `BeforeTool`          | Before a tool executes                         | Block Tool / Rewrite   | Validate arguments, block dangerous ops      |
| `AfterTool`           | After a tool executes                          | Block Result / Context | Process results, run tests, hide results     |
| `PreCompress`         | Before context compression                     | Advisory               | Save state, notify user                      |
| `Notification`        | When a system notification occurs              | Advisory               | Forward to desktop alerts, logging           |

### Global mechanics

Understanding these core principles is essential for building robust hooks.

#### Strict JSON requirements (The "Golden Rule")

Hooks communicate via `stdin` (Input) and `stdout` (Output).

1. **Silence is Mandatory**: Your script **must not** print any plain text to
   `stdout` other than the final JSON object. **Even a single `echo` or `print`
   call before the JSON will break parsing.**
2. **Pollution = Failure**: If `stdout` contains non-JSON text, parsing will
   fail. The CLI will default to "Allow" and treat the entire output as a
   `systemMessage`.
3. **Debug via Stderr**: Use `stderr` for **all** logging and debugging (e.g.,
   `echo "debug" >&2`). Gemini CLI captures `stderr` but never attempts to parse
   it as JSON.

#### Exit codes

Gemini CLI uses exit codes to determine the high-level outcome of a hook
execution:

| Exit Code | Label            | Behavioral Impact                                                                                                                                                            |
| --------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **0**     | **Success**      | The `stdout` is parsed as JSON. **Preferred code** for all logic, including intentional blocks (e.g., `{"decision": "deny"}`).                                               |
| **2**     | **System Block** | **Critical Block**. The target action (tool, turn, or stop) is aborted. `stderr` is used as the rejection reason. High severity; used for security stops or script failures. |
| **Other** | **Warning**      | Non-fatal failure. A warning is shown, but the interaction proceeds using original parameters.                                                                               |

#### Matchers

You can filter which specific tools or triggers fire your hook using the
`matcher` field.

- **Tool events** (`BeforeTool`, `AfterTool`): Matchers are **Regular
  Expressions**. (e.g., `"write_.*"`).
- **Lifecycle events**: Matchers are **Exact Strings**. (e.g., `"startup"`).
- **Wildcards**: `"*"` or `""` (empty string) matches all occurrences.

## Configuration

Hooks are configured in `settings.json`. Gemini CLI merges configurations from
multiple layers in the following order of precedence (highest to lowest):

1.  **Project settings**: `.gemini/settings.json` in the current directory.
2.  **User settings**: `~/.gemini/settings.json`.
3.  **System settings**: `/etc/gemini-cli/settings.json`.
4.  **Extensions**: Hooks defined by installed extensions.

### Configuration schema

```json
{
  "hooks": {
    "BeforeTool": [
      {
        "matcher": "write_file|replace",
        "hooks": [
          {
            "name": "security-check",
            "type": "command",
            "command": "$GEMINI_PROJECT_DIR/.gemini/hooks/security.sh",
            "timeout": 5000
          }
        ]
      }
    ]
  }
}
```

#### Hook configuration fields

| Field         | Type   | Required  | Description                                                          |
| :------------ | :----- | :-------- | :------------------------------------------------------------------- |
| `type`        | string | **Yes**   | The execution engine. Currently only `"command"` is supported.       |
| `command`     | string | **Yes\*** | The shell command to execute. (Required when `type` is `"command"`). |
| `name`        | string | No        | A friendly name for identifying the hook in logs and CLI commands.   |
| `timeout`     | number | No        | Execution timeout in milliseconds (default: 60000).                  |
| `description` | string | No        | A brief explanation of the hook's purpose.                           |

---

### Environment variables

Hooks are executed with a sanitized environment.

- `GEMINI_PROJECT_DIR`: The absolute path to the project root.
- `GEMINI_SESSION_ID`: The unique ID for the current session.
- `GEMINI_CWD`: The current working directory.
- `CLAUDE_PROJECT_DIR`: (Alias) Provided for compatibility.

## Security and risks

> **Warning: Hooks execute arbitrary code with your user privileges.** By
> configuring hooks, you are allowing scripts to run shell commands on your
> machine.

**Project-level hooks** are particularly risky when opening untrusted projects.
Gemini CLI **fingerprints** project hooks. If a hook's name or command changes
(e.g., via `git pull`), it is treated as a **new, untrusted hook** and you will
be warned before it executes.

See [Security Considerations](/docs/hooks/best-practices#using-hooks-securely)
for a detailed threat model.

## Managing hooks

Use the CLI commands to manage hooks without editing JSON manually:

- **View hooks:** `/hooks panel`
- **Enable/Disable all:** `/hooks enable-all` or `/hooks disable-all`
- **Toggle individual:** `/hooks enable <name>` or `/hooks disable <name>`


---


# Writing hooks for Gemini CLI

This guide will walk you through creating hooks for Gemini CLI, from a simple
logging hook to a comprehensive workflow assistant.

## Prerequisites

Before you start, make sure you have:

- Gemini CLI installed and configured
- Basic understanding of shell scripting or JavaScript/Node.js
- Familiarity with JSON for hook input/output

## Quick start

Let's create a simple hook that logs all tool executions to understand the
basics.

**Crucial Rule:** Always write logs to `stderr`. Write only the final JSON to
`stdout`.

### Step 1: Create your hook script

Create a directory for hooks and a simple logging script.

> **Note**:
>
> This example uses `jq` to parse JSON. If you don't have it installed, you can
> perform similar logic using Node.js or Python.

```bash
mkdir -p .gemini/hooks
cat > .gemini/hooks/log-tools.sh << 'EOF'
#!/usr/bin/env bash
# Read hook input from stdin
input=$(cat)

# Extract tool name (requires jq)
tool_name=$(echo "$input" | jq -r '.tool_name')

# Log to stderr (visible in terminal if hook fails, or captured in logs)
echo "Logging tool: $tool_name" >&2

# Log to file
echo "[$(date)] Tool executed: $tool_name" >> .gemini/tool-log.txt

# Return success (exit 0) with empty JSON
echo "{}"
exit 0
EOF

chmod +x .gemini/hooks/log-tools.sh
```

## Exit Code Strategies

There are two ways to control or block an action in Gemini CLI:

| Strategy                   | Exit Code | Implementation                                                     | Best For                                                    |
| :------------------------- | :-------- | :----------------------------------------------------------------- | :---------------------------------------------------------- |
| **Structured (Idiomatic)** | `0`       | Return a JSON object like `{"decision": "deny", "reason": "..."}`. | Production hooks, custom user feedback, and complex logic.  |
| **Emergency Brake**        | `2`       | Print the error message to `stderr` and exit.                      | Simple security gates, script errors, or rapid prototyping. |

## Practical examples

### Security: Block secrets in commits

Prevent committing files containing API keys or passwords. Note that we use
**Exit Code 0** to provide a structured denial message to the agent.

**`.gemini/hooks/block-secrets.sh`:**

```bash
#!/usr/bin/env bash
input=$(cat)

# Extract content being written
content=$(echo "$input" | jq -r '.tool_input.content // .tool_input.new_string // ""')

# Check for secrets
if echo "$content" | grep -qE 'api[_-]?key|password|secret'; then
  # Log to stderr
  echo "Blocked potential secret" >&2

  # Return structured denial to stdout
  cat <<EOF
{
  "decision": "deny",
  "reason": "Security Policy: Potential secret detected in content.",
  "systemMessage": "🔒 Security scanner blocked operation"
}
EOF
  exit 0
fi

# Allow
echo '{"decision": "allow"}'
exit 0
```

### Dynamic context injection (Git History)

Add relevant project context before each agent interaction.

**`.gemini/hooks/inject-context.sh`:**

```bash
#!/usr/bin/env bash

# Get recent git commits for context
context=$(git log -5 --oneline 2>/dev/null || echo "No git history")

# Return as JSON
cat <<EOF
{
  "hookSpecificOutput": {
    "hookEventName": "BeforeAgent",
    "additionalContext": "Recent commits:\n$context"
  }
}
EOF
```

### RAG-based Tool Filtering (BeforeToolSelection)

Use `BeforeToolSelection` to intelligently reduce the tool space. This example
uses a Node.js script to check the user's prompt and allow only relevant tools.

**`.gemini/hooks/filter-tools.js`:**

```javascript
#!/usr/bin/env node
const fs = require('fs');

async function main() {
  const input = JSON.parse(fs.readFileSync(0, 'utf-8'));
  const { llm_request } = input;

  // Decoupled API: Access messages from llm_request
  const messages = llm_request.messages || [];
  const lastUserMessage = messages
    .slice()
    .reverse()
    .find((m) => m.role === 'user');

  if (!lastUserMessage) {
    console.log(JSON.stringify({})); // Do nothing
    return;
  }

  const text = lastUserMessage.content;
  const allowed = ['write_todos']; // Always allow memory

  // Simple keyword matching
  if (text.includes('read') || text.includes('check')) {
    allowed.push('read_file', 'list_directory');
  }
  if (text.includes('test')) {
    allowed.push('run_shell_command');
  }

  // If we found specific intent, filter tools. Otherwise allow all.
  if (allowed.length > 1) {
    console.log(
      JSON.stringify({
        hookSpecificOutput: {
          hookEventName: 'BeforeToolSelection',
          toolConfig: {
            mode: 'ANY', // Force usage of one of these tools (or AUTO)
            allowedFunctionNames: allowed,
          },
        },
      }),
    );
  } else {
    console.log(JSON.stringify({}));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

**`.gemini/settings.json`:**

```json
{
  "hooks": {
    "BeforeToolSelection": [
      {
        "matcher": "*",
        "hooks": [
          {
            "name": "intent-filter",
            "type": "command",
            "command": "node .gemini/hooks/filter-tools.js"
          }
        ]
      }
    ]
  }
}
```

> **TIP**
>
> **Union Aggregation Strategy**: `BeforeToolSelection` is unique in that it
> combines the results of all matching hooks. If you have multiple filtering
> hooks, the agent will receive the **union** of all whitelisted tools. Only
> using `mode: "NONE"` will override other hooks to disable all tools.

## Complete example: Smart Development Workflow Assistant

This comprehensive example demonstrates all hook events working together. We
will build a system that maintains memory, filters tools, and checks for
security.

### Architecture

1. **SessionStart**: Load project memories.
2. **BeforeAgent**: Inject memories into context.
3. **BeforeToolSelection**: Filter tools based on intent.
4. **BeforeTool**: Scan for secrets.
5. **AfterModel**: Record interactions.
6. **AfterAgent**: Validate final response quality (Retry).
7. **SessionEnd**: Consolidate memories.

### Configuration (`.gemini/settings.json`)

```json
{
  "hooks": {
    "SessionStart": [
      {
        "matcher": "startup",
        "hooks": [
          {
            "name": "init",
            "type": "command",
            "command": "node .gemini/hooks/init.js"
          }
        ]
      }
    ],
    "BeforeAgent": [
      {
        "matcher": "*",
        "hooks": [
          {
            "name": "memory",
            "type": "command",
            "command": "node .gemini/hooks/inject-memories.js"
          }
        ]
      }
    ],
    "BeforeToolSelection": [
      {
        "matcher": "*",
        "hooks": [
          {
            "name": "filter",
            "type": "command",
            "command": "node .gemini/hooks/rag-filter.js"
          }
        ]
      }
    ],
    "BeforeTool": [
      {
        "matcher": "write_file",
        "hooks": [
          {
            "name": "security",
            "type": "command",
            "command": "node .gemini/hooks/security.js"
          }
        ]
      }
    ],
    "AfterModel": [
      {
        "matcher": "*",
        "hooks": [
          {
            "name": "record",
            "type": "command",
            "command": "node .gemini/hooks/record.js"
          }
        ]
      }
    ],
    "AfterAgent": [
      {
        "matcher": "*",
        "hooks": [
          {
            "name": "validate",
            "type": "command",
            "command": "node .gemini/hooks/validate.js"
          }
        ]
      }
    ],
    "SessionEnd": [
      {
        "matcher": "exit",
        "hooks": [
          {
            "name": "save",
            "type": "command",
            "command": "node .gemini/hooks/consolidate.js"
          }
        ]
      }
    ]
  }
}
```

### Hook Scripts

> **Note**: For brevity, these scripts use `console.error` for logging and
> standard `console.log` for JSON output.

#### 1. Initialize (`init.js`)

```javascript
#!/usr/bin/env node
// Initialize DB or resources
console.error('Initializing assistant...');

// Output to user
console.log(
  JSON.stringify({
    systemMessage: '🧠 Smart Assistant Loaded',
  }),
);
```

#### 2. Inject Memories (`inject-memories.js`)

```javascript
#!/usr/bin/env node
const fs = require('fs');

async function main() {
  const input = JSON.parse(fs.readFileSync(0, 'utf-8'));
  // Assume we fetch memories from a DB here
  const memories = '- [Memory] Always use TypeScript for this project.';

  console.log(
    JSON.stringify({
      hookSpecificOutput: {
        hookEventName: 'BeforeAgent',
        additionalContext: `\n## Relevant Memories\n${memories}`,
      },
    }),
  );
}
main();
```

#### 3. Security Check (`security.js`)

```javascript
#!/usr/bin/env node
const fs = require('fs');
const input = JSON.parse(fs.readFileSync(0));
const content = input.tool_input.content || '';

if (content.includes('SECRET_KEY')) {
  console.log(
    JSON.stringify({
      decision: 'deny',
      reason: 'Found SECRET_KEY in content',
      systemMessage: '🚨 Blocked sensitive commit',
    }),
  );
  process.exit(0);
}

console.log(JSON.stringify({ decision: 'allow' }));
```

#### 4. Record Interaction (`record.js`)

```javascript
#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const input = JSON.parse(fs.readFileSync(0));
const { llm_request, llm_response } = input;
const logFile = path.join(
  process.env.GEMINI_PROJECT_DIR,
  '.gemini/memory/session.jsonl',
);

fs.appendFileSync(
  logFile,
  JSON.stringify({
    request: llm_request,
    response: llm_response,
    timestamp: new Date().toISOString(),
  }) + '\n',
);

console.log(JSON.stringify({}));
```

#### 5. Validate Response (`validate.js`)

```javascript
#!/usr/bin/env node
const fs = require('fs');
const input = JSON.parse(fs.readFileSync(0));
const response = input.prompt_response;

// Example: Check if the agent forgot to include a summary
if (!response.includes('Summary:')) {
  console.log(
    JSON.stringify({
      decision: 'block', // Triggers an automatic retry turn
      reason: 'Your response is missing a Summary section. Please add one.',
      systemMessage: '🔄 Requesting missing summary...',
    }),
  );
  process.exit(0);
}

console.log(JSON.stringify({ decision: 'allow' }));
```

#### 6. Consolidate Memories (`consolidate.js`)

```javascript
#!/usr/bin/env node
// Logic to save final session state
console.error('Consolidating memories for session end...');
```

## Packaging as an extension

While project-level hooks are great for specific repositories, you can share
your hooks across multiple projects by packaging them as a
[Gemini CLI extension](https://www.google.com/search?q=../extensions/index.md).
This provides version control, easy distribution, and centralized management.



---



# Hooks reference

This document provides the technical specification for Gemini CLI hooks,
including JSON schemas and API details.

## Global hook mechanics

- **Communication**: `stdin` for Input (JSON), `stdout` for Output (JSON), and
  `stderr` for logs and feedback.
- **Exit codes**:
  - `0`: Success. `stdout` is parsed as JSON. **Preferred for all logic.**
  - `2`: System Block. The action is blocked; `stderr` is used as the rejection
    reason.
  - `Other`: Warning. A non-fatal failure occurred; the CLI continues with a
    warning.
- **Silence is Mandatory**: Your script **must not** print any plain text to
  `stdout` other than the final JSON.

---

## Configuration schema

Hooks are defined in `settings.json` within the `hooks` object. Each event
(e.g., `BeforeTool`) contains an array of **hook definitions**.

### Hook definition

| Field        | Type      | Required | Description                                                                             |
| :----------- | :-------- | :------- | :-------------------------------------------------------------------------------------- |
| `matcher`    | `string`  | No       | A regex (for tools) or exact string (for lifecycle) to filter when the hook runs.       |
| `sequential` | `boolean` | No       | If `true`, hooks in this group run one after another. If `false`, they run in parallel. |
| `hooks`      | `array`   | **Yes**  | An array of **hook configurations**.                                                    |

### Hook configuration

| Field         | Type     | Required  | Description                                                          |
| :------------ | :------- | :-------- | :------------------------------------------------------------------- |
| `type`        | `string` | **Yes**   | The execution engine. Currently only `"command"` is supported.       |
| `command`     | `string` | **Yes\*** | The shell command to execute. (Required when `type` is `"command"`). |
| `name`        | `string` | No        | A friendly name for identifying the hook in logs and CLI commands.   |
| `timeout`     | `number` | No        | Execution timeout in milliseconds (default: 60000).                  |
| `description` | `string` | No        | A brief explanation of the hook's purpose.                           |

---

## Base input schema

All hooks receive these common fields via `stdin`:

```typescript
{
  "session_id": string,      // Unique ID for the current session
  "transcript_path": string, // Absolute path to session transcript JSON
  "cwd": string,             // Current working directory
  "hook_event_name": string, // The firing event (e.g. "BeforeTool")
  "timestamp": string        // ISO 8601 execution time
}
```

---

## Common output fields

Most hooks support these fields in their `stdout` JSON:

| Field            | Type      | Description                                                                    |
| :--------------- | :-------- | :----------------------------------------------------------------------------- |
| `systemMessage`  | `string`  | Displayed immediately to the user in the terminal.                             |
| `suppressOutput` | `boolean` | If `true`, hides internal hook metadata from logs/telemetry.                   |
| `continue`       | `boolean` | If `false`, stops the entire agent loop immediately.                           |
| `stopReason`     | `string`  | Displayed to the user when `continue` is `false`.                              |
| `decision`       | `string`  | `"allow"` or `"deny"` (alias `"block"`). Specific impact depends on the event. |
| `reason`         | `string`  | The feedback/error message provided when a `decision` is `"deny"`.             |

---

## Tool hooks

### Matchers and tool names

For `BeforeTool` and `AfterTool` events, the `matcher` field in your settings is
compared against the name of the tool being executed.

- **Built-in Tools**: You can match any built-in tool (e.g., `read_file`,
  `run_shell_command`). See the [Tools Reference](/docs/tools) for a full list
  of available tool names.
- **MCP Tools**: Tools from MCP servers follow the naming pattern
  `mcp__<server_name>__<tool_name>`.
- **Regex Support**: Matchers support regular expressions (e.g.,
  `matcher: "read_.*"` matches all file reading tools).

### `BeforeTool`

Fires before a tool is invoked. Used for argument validation, security checks,
and parameter rewriting.

- **Input Fields**:
  - `tool_name`: (`string`) The name of the tool being called.
  - `tool_input`: (`object`) The raw arguments generated by the model.
  - `mcp_context`: (`object`) Optional metadata for MCP-based tools.
- **Relevant Output Fields**:
  - `decision`: Set to `"deny"` (or `"block"`) to prevent the tool from
    executing.
  - `reason`: Required if denied. This text is sent **to the agent** as a tool
    error, allowing it to respond or retry.
  - `hookSpecificOutput.tool_input`: An object that **merges with and
    overrides** the model's arguments before execution.
  - `continue`: Set to `false` to **kill the entire agent loop** immediately.
- **Exit Code 2 (Block Tool)**: Prevents execution. Uses `stderr` as the
  `reason` sent to the agent. **The turn continues.**

### `AfterTool`

Fires after a tool executes. Used for result auditing, context injection, or
hiding sensitive output from the agent.

- **Input Fields**:
  - `tool_name`: (`string`)
  - `tool_input`: (`object`) The original arguments.
  - `tool_response`: (`object`) The result containing `llmContent`,
    `returnDisplay`, and optional `error`.
  - `mcp_context`: (`object`)
- **Relevant Output Fields**:
  - `decision`: Set to `"deny"` to hide the real tool output from the agent.
  - `reason`: Required if denied. This text **replaces** the tool result sent
    back to the model.
  - `hookSpecificOutput.additionalContext`: Text that is **appended** to the
    tool result for the agent.
  - `continue`: Set to `false` to **kill the entire agent loop** immediately.
- **Exit Code 2 (Block Result)**: Hides the tool result. Uses `stderr` as the
  replacement content sent to the agent. **The turn continues.**

---

## Agent hooks

### `BeforeAgent`

Fires after a user submits a prompt, but before the agent begins planning. Used
for prompt validation or injecting dynamic context.

- **Input Fields**:
  - `prompt`: (`string`) The original text submitted by the user.
- **Relevant Output Fields**:
  - `hookSpecificOutput.additionalContext`: Text that is **appended** to the
    prompt for this turn only.
  - `decision`: Set to `"deny"` to block the turn and **discard the user's
    message** (it will not appear in history).
  - `continue`: Set to `false` to block the turn but **save the message to
    history**.
  - `reason`: Required if denied or stopped.
- **Exit Code 2 (Block Turn)**: Aborts the turn and erases the prompt from
  context. Same as `decision: "deny"`.

### `AfterAgent`

Fires once per turn after the model generates its final response. Primary use
case is response validation and automatic retries.

- **Input Fields**:
  - `prompt`: (`string`) The user's original request.
  - `prompt_response`: (`string`) The final text generated by the agent.
  - `stop_hook_active`: (`boolean`) Indicates if this hook is already running as
    part of a retry sequence.
- **Relevant Output Fields**:
  - `decision`: Set to `"deny"` to **reject the response** and force a retry.
  - `reason`: Required if denied. This text is sent **to the agent as a new
    prompt** to request a correction.
  - `continue`: Set to `false` to **stop the session** without retrying.
  - `clearContext`: If `true`, clears conversation history (LLM memory) while
    preserving UI display.
- **Exit Code 2 (Retry)**: Rejects the response and triggers an automatic retry
  turn using `stderr` as the feedback prompt.

---

## Model hooks

### `BeforeModel`

Fires before sending a request to the LLM. Operates on a stable, SDK-agnostic
request format.

- **Input Fields**:
  - `llm_request`: (`object`) Contains `model`, `messages`, and `config`
    (generation params).
- **Relevant Output Fields**:
  - `hookSpecificOutput.llm_request`: An object that **overrides** parts of the
    outgoing request (e.g., changing models or temperature).
  - `hookSpecificOutput.llm_response`: A **Synthetic Response** object. If
    provided, the CLI skips the LLM call entirely and uses this as the response.
  - `decision`: Set to `"deny"` to block the request and abort the turn.
- **Exit Code 2 (Block Turn)**: Aborts the turn and skips the LLM call. Uses
  `stderr` as the error message.

### `BeforeToolSelection`

Fires before the LLM decides which tools to call. Used to filter the available
toolset or force specific tool modes.

- **Input Fields**:
  - `llm_request`: (`object`) Same format as `BeforeModel`.
- **Relevant Output Fields**:
  - `hookSpecificOutput.toolConfig.mode`: (`"AUTO" | "ANY" | "NONE"`)
    - `"NONE"`: Disables all tools (Wins over other hooks).
    - `"ANY"`: Forces at least one tool call.
  - `hookSpecificOutput.toolConfig.allowedFunctionNames`: (`string[]`) Whitelist
    of tool names.
- **Union Strategy**: Multiple hooks' whitelists are **combined**.
- **Limitations**: Does **not** support `decision`, `continue`, or
  `systemMessage`.

### `AfterModel`

Fires immediately after an LLM response chunk is received. Used for real-time
redaction or PII filtering.

- **Input Fields**:
  - `llm_request`: (`object`) The original request.
  - `llm_response`: (`object`) The model's response (or a single chunk during
    streaming).
- **Relevant Output Fields**:
  - `hookSpecificOutput.llm_response`: An object that **replaces** the model's
    response chunk.
  - `decision`: Set to `"deny"` to discard the response chunk and block the
    turn.
  - `continue`: Set to `false` to **kill the entire agent loop** immediately.
- **Note on Streaming**: Fired for **every chunk** generated by the model.
  Modifying the response only affects the current chunk.
- **Exit Code 2 (Block Response)**: Aborts the turn and discards the model's
  output. Uses `stderr` as the error message.

---

## Lifecycle & system hooks

### `SessionStart`

Fires on application startup, resuming a session, or after a `/clear` command.
Used for loading initial context.

- **Input fields**:
  - `source`: (`"startup" | "resume" | "clear"`)
- **Relevant output fields**:
  - `hookSpecificOutput.additionalContext`: (`string`)
    - **Interactive**: Injected as the first turn in history.
    - **Non-interactive**: Prepended to the user's prompt.
  - `systemMessage`: Shown at the start of the session.
- **Advisory only**: `continue` and `decision` fields are **ignored**. Startup
  is never blocked.

### `SessionEnd`

Fires when the CLI exits or a session is cleared. Used for cleanup or final
telemetry.

- **Input Fields**:
  - `reason`: (`"exit" | "clear" | "logout" | "prompt_input_exit" | "other"`)
- **Relevant Output Fields**:
  - `systemMessage`: Displayed to the user during shutdown.
- **Best Effort**: The CLI **will not wait** for this hook to complete and
  ignores all flow-control fields (`continue`, `decision`).

### `Notification`

Fires when the CLI emits a system alert (e.g., Tool Permissions). Used for
external logging or cross-platform alerts.

- **Input Fields**:
  - `notification_type`: (`"ToolPermission"`)
  - `message`: Summary of the alert.
  - `details`: JSON object with alert-specific metadata (e.g., tool name, file
    path).
- **Relevant Output Fields**:
  - `systemMessage`: Displayed alongside the system alert.
- **Observability Only**: This hook **cannot** block alerts or grant permissions
  automatically. Flow-control fields are ignored.

### `PreCompress`

Fires before the CLI summarizes history to save tokens. Used for logging or
state saving.

- **Input Fields**:
  - `trigger`: (`"auto" | "manual"`)
- **Relevant Output Fields**:
  - `systemMessage`: Displayed to the user before compression.
- **Advisory Only**: Fired asynchronously. It **cannot** block or modify the
  compression process. Flow-control fields are ignored.

---

## Stable Model API

Gemini CLI uses these structures to ensure hooks don't break across SDK updates.

**LLMRequest**:

```typescript
{
  "model": string,
  "messages": Array<{
    "role": "user" | "model" | "system",
    "content": string // Non-text parts are filtered out for hooks
  }>,
  "config": { "temperature": number, ... },
  "toolConfig": { "mode": string, "allowedFunctionNames": string[] }
}

```

**LLMResponse**:

```typescript
{
  "candidates": Array<{
    "content": { "role": "model", "parts": string[] },
    "finishReason": string
  }>,
  "usageMetadata": { "totalTokenCount": number }
}
```



---



# Hooks Best Practices

This guide covers security considerations, performance optimization, debugging
techniques, and privacy considerations for developing and deploying hooks in
Gemini CLI.

## Performance

### Keep hooks fast

Hooks run synchronously—slow hooks delay the agent loop. Optimize for speed by
using parallel operations:

```javascript
// Sequential operations are slower
const data1 = await fetch(url1).then((r) => r.json());
const data2 = await fetch(url2).then((r) => r.json());

// Prefer parallel operations for better performance
// Start requests concurrently
const p1 = fetch(url1).then((r) => r.json());
const p2 = fetch(url2).then((r) => r.json());

// Wait for all results
const [data1, data2] = await Promise.all([p1, p2]);
```

### Cache expensive operations

Store results between invocations to avoid repeated computation, especially for
hooks that run frequently (like `BeforeTool` or `AfterModel`).

```javascript
const fs = require('fs');
const path = require('path');

const CACHE_FILE = '.gemini/hook-cache.json';

function readCache() {
  try {
    return JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
  } catch {
    return {};
  }
}

function writeCache(data) {
  fs.writeFileSync(CACHE_FILE, JSON.stringify(data, null, 2));
}

async function main() {
  const cache = readCache();
  const cacheKey = `tool-list-${(Date.now() / 3600000) | 0}`; // Hourly cache

  if (cache[cacheKey]) {
    // Write JSON to stdout
    console.log(JSON.stringify(cache[cacheKey]));
    return;
  }

  // Expensive operation
  const result = await computeExpensiveResult();
  cache[cacheKey] = result;
  writeCache(cache);

  console.log(JSON.stringify(result));
}
```

### Use appropriate events

Choose hook events that match your use case to avoid unnecessary execution.

- **`AfterAgent`**: Fires **once** per turn after the model finishes its final
  response. Use this for quality validation (Retries) or final logging.
- **`AfterModel`**: Fires after **every chunk** of LLM output. Use this for
  real-time redaction, PII filtering, or monitoring output as it streams.

If you only need to check the final completion, use `AfterAgent` to save
performance.

### Filter with matchers

Use specific matchers to avoid unnecessary hook execution. Instead of matching
all tools with `*`, specify only the tools you need. This saves the overhead of
spawning a process for irrelevant events.

```json
{
  "matcher": "write_file|replace",
  "hooks": [
    {
      "name": "validate-writes",
      "type": "command",
      "command": "./validate.sh"
    }
  ]
}
```

### Optimize JSON parsing

For large inputs (like `AfterModel` receiving a large context), standard JSON
parsing can be slow. If you only need one field, consider streaming parsers or
lightweight extraction logic, though for most shell scripts `jq` is sufficient.

## Debugging

### The "Strict JSON" rule

The most common cause of hook failure is "polluting" the standard output.

- **stdout** is for **JSON only**.
- **stderr** is for **logs and text**.

**Good:**

```bash
#!/bin/bash
echo "Starting check..." >&2  # <--- Redirect to stderr
echo '{"decision": "allow"}'

```

### Log to files

Since hooks run in the background, writing to a dedicated log file is often the
easiest way to debug complex logic.

```bash
#!/usr/bin/env bash
LOG_FILE=".gemini/hooks/debug.log"

# Log with timestamp
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"
}

input=$(cat)
log "Received input: ${input:0:100}..."

# Hook logic here

log "Hook completed successfully"
# Always output valid JSON to stdout at the end, even if just empty
echo "{}"

```

### Use stderr for errors

Error messages on stderr are surfaced appropriately based on exit codes:

```javascript
try {
  const result = dangerousOperation();
  console.log(JSON.stringify({ result }));
} catch (error) {
  // Write the error description to stderr so the user/agent sees it
  console.error(`Hook error: ${error.message}`);
  process.exit(2); // Blocking error
}
```

### Test hooks independently

Run hook scripts manually with sample JSON input to verify they behave as
expected before hooking them up to the CLI.

```bash
# Create test input
cat > test-input.json << 'EOF'
{
  "session_id": "test-123",
  "cwd": "/tmp/test",
  "hook_event_name": "BeforeTool",
  "tool_name": "write_file",
  "tool_input": {
    "file_path": "test.txt",
    "content": "Test content"
  }
}
EOF

# Test the hook
cat test-input.json | .gemini/hooks/my-hook.sh

# Check exit code
echo "Exit code: $?"

```

### Check exit codes

Gemini CLI uses exit codes for high-level flow control:

- **Exit 0 (Success)**: The hook ran successfully. The CLI parses `stdout` for
  JSON decisions.
- **Exit 2 (System Block)**: A critical block occurred. `stderr` is used as the
  reason.
  - For **Agent/Model** events, this aborts the turn.
  - For **Tool** events, this blocks the tool but allows the agent to continue.
  - For **AfterAgent**, this triggers an automatic retry turn.

> **TIP**
>
> **Blocking vs. Stopping**: Use `decision: "deny"` (or Exit Code 2) to block a
> **specific action**. Use `{"continue": false}` in your JSON output to **kill
> the entire agent loop** immediately.

```bash
#!/usr/bin/env bash
set -e

# Hook logic
if process_input; then
  echo '{"decision": "allow"}'
  exit 0
else
  echo "Critical validation failure" >&2
  exit 2
fi

```

### Enable telemetry

Hook execution is logged when `telemetry.logPrompts` is enabled. You can view
these logs to debug execution flow.

```json
{
  "telemetry": {
    "logPrompts": true
  }
}
```

### Use hook panel

The `/hooks panel` command inside the CLI shows execution status and recent
output:

```bash
/hooks panel
```

Check for:

- Hook execution counts
- Recent successes/failures
- Error messages
- Execution timing

## Development

### Start simple

Begin with basic logging hooks before implementing complex logic:

```bash
#!/usr/bin/env bash
# Simple logging hook to understand input structure
input=$(cat)
echo "$input" >> .gemini/hook-inputs.log
# Always return valid JSON
echo "{}"

```

### Documenting your hooks

Maintainability is critical for complex hook systems. Use descriptions and
comments to help yourself and others understand why a hook exists.

**Use the `description` field**: This text is displayed in the `/hooks panel` UI
and helps diagnose issues.

```json
{
  "hooks": {
    "BeforeTool": [
      {
        "matcher": "write_file|replace",
        "hooks": [
          {
            "name": "secret-scanner",
            "type": "command",
            "command": "$GEMINI_PROJECT_DIR/.gemini/hooks/block-secrets.sh",
            "description": "Scans code changes for API keys and secrets before writing"
          }
        ]
      }
    ]
  }
}
```

**Add comments in hook scripts**: Explain performance expectations and
dependencies.

```javascript
#!/usr/bin/env node
/**
 * RAG Tool Filter Hook
 *
 * Reduces the tool space by extracting keywords from the user's request.
 *
 * Performance: ~500ms average
 * Dependencies: @google/generative-ai
 */
```

### Use JSON libraries

Parse JSON with proper libraries instead of text processing.

**Bad:**

```bash
# Fragile text parsing
tool_name=$(echo "$input" | grep -oP '"tool_name":\s*"\K[^"]+')

```

**Good:**

```bash
# Robust JSON parsing
tool_name=$(echo "$input" | jq -r '.tool_name')

```

### Make scripts executable

Always make hook scripts executable:

```bash
chmod +x .gemini/hooks/*.sh
chmod +x .gemini/hooks/*.js

```

### Version control

Commit hooks to share with your team:

```bash
git add .gemini/hooks/
git add .gemini/settings.json

```

**`.gitignore` considerations:**

```gitignore
# Ignore hook cache and logs
.gemini/hook-cache.json
.gemini/hook-debug.log
.gemini/memory/session-*.jsonl

# Keep hook scripts
!.gemini/hooks/*.sh
!.gemini/hooks/*.js

```

## Hook security

### Threat Model

Understanding where hooks come from and what they can do is critical for secure
usage.

| Hook Source                   | Description                                                                                                                |
| :---------------------------- | :------------------------------------------------------------------------------------------------------------------------- |
| **System**                    | Configured by system administrators (e.g., `/etc/gemini-cli/settings.json`, `/Library/...`). Assumed to be the **safest**. |
| **User** (`~/.gemini/...`)    | Configured by you. You are responsible for ensuring they are safe.                                                         |
| **Extensions**                | You explicitly approve and install these. Security depends on the extension source (integrity).                            |
| **Project** (`./.gemini/...`) | **Untrusted by default.** Safest in trusted internal repos; higher risk in third-party/public repos.                       |

#### Project Hook Security

When you open a project with hooks defined in `.gemini/settings.json`:

1. **Detection**: Gemini CLI detects the hooks.
2. **Identification**: A unique identity is generated for each hook based on its
   `name` and `command`.
3. **Warning**: If this specific hook identity has not been seen before, a
   **warning** is displayed.
4. **Execution**: The hook is executed (unless specific security settings block
   it).
5. **Trust**: The hook is marked as "trusted" for this project.

> **Modification detection**: If the `command` string of a project hook is
> changed (e.g., by a `git pull`), its identity changes. Gemini CLI will treat
> it as a **new, untrusted hook** and warn you again. This prevents malicious
> actors from silently swapping a verified command for a malicious one.

### Risks

| Risk                         | Description                                                                                                                          |
| :--------------------------- | :----------------------------------------------------------------------------------------------------------------------------------- |
| **Arbitrary Code Execution** | Hooks run as your user. They can do anything you can do (delete files, install software).                                            |
| **Data Exfiltration**        | A hook could read your input (prompts), output (code), or environment variables (`GEMINI_API_KEY`) and send them to a remote server. |
| **Prompt Injection**         | Malicious content in a file or web page could trick an LLM into running a tool that triggers a hook in an unexpected way.            |

### Mitigation Strategies

#### Verify the source

**Verify the source** of any project hooks or extensions before enabling them.

- For open-source projects, a quick review of the hook scripts is recommended.
- For extensions, ensure you trust the author or publisher (e.g., verified
  publishers, well-known community members).
- Be cautious with obfuscated scripts or compiled binaries from unknown sources.

#### Sanitize environment

Hooks inherit the environment of the Gemini CLI process, which may include
sensitive API keys. Gemini CLI provides a
[redaction system](/docs/get-started/configuration#environment-variable-redaction)
that automatically filters variables matching sensitive patterns (e.g., `KEY`,
`TOKEN`).

> **Disabled by Default**: Environment redaction is currently **OFF by
> default**. We strongly recommend enabling it if you are running third-party
> hooks or working in sensitive environments.

**Impact on hooks:**

- **Security**: Prevents your hook scripts from accidentally leaking secrets.
- **Troubleshooting**: If your hook depends on a specific environment variable
  that is being blocked, you must explicitly allow it in `settings.json`.

```json
{
  "security": {
    "environmentVariableRedaction": {
      "enabled": true,
      "allowed": ["MY_REQUIRED_TOOL_KEY"]
    }
  }
}
```

**System administrators:** You can enforce redaction for all users in the system
configuration.

## Troubleshooting

### Hook not executing

**Check hook name in `/hooks panel`:** Verify the hook appears in the list and
is enabled.

**Verify matcher pattern:**

```bash
# Test regex pattern
echo "write_file|replace" | grep -E "write_.*|replace"

```

**Check disabled list:** Verify the hook is not listed in your `settings.json`:

```json
{
  "hooks": {
    "disabled": ["my-hook-name"]
  }
}
```

**Ensure script is executable**: For macOS and Linux users, verify the script
has execution permissions:

```bash
ls -la .gemini/hooks/my-hook.sh
chmod +x .gemini/hooks/my-hook.sh
```

**Verify script path:** Ensure the path in `settings.json` resolves correctly.

```bash
# Check path expansion
echo "$GEMINI_PROJECT_DIR/.gemini/hooks/my-hook.sh"

# Verify file exists
test -f "$GEMINI_PROJECT_DIR/.gemini/hooks/my-hook.sh" && echo "File exists"
```

### Hook timing out

**Check configured timeout:** The default is 60000ms (1 minute). You can
increase this in `settings.json`:

```json
{
  "name": "slow-hook",
  "timeout": 120000
}
```

**Optimize slow operations:** Move heavy processing to background tasks or use
caching.

### Invalid JSON output

**Validate JSON before outputting:**

```bash
#!/usr/bin/env bash
output='{"decision": "allow"}'

# Validate JSON
if echo "$output" | jq empty 2>/dev/null; then
  echo "$output"
else
  echo "Invalid JSON generated" >&2
  exit 1
fi

```

### Environment variables not available

**Check if variable is set:**

```bash
#!/usr/bin/env bash
if [ -z "$GEMINI_PROJECT_DIR" ]; then
  echo "GEMINI_PROJECT_DIR not set" >&2
  exit 1
fi

```

**Debug available variables:**

```bash
env > .gemini/hook-env.log
```

## Authoring secure hooks

When writing your own hooks, follow these practices to ensure they are robust
and secure.

### Validate all inputs

Never trust data from hooks without validation. Hook inputs often come from the
LLM or user prompts, which can be manipulated.

```bash
#!/usr/bin/env bash
input=$(cat)

# Validate JSON structure
if ! echo "$input" | jq empty 2>/dev/null; then
  echo "Invalid JSON input" >&2
  exit 1
fi

# Validate tool_name explicitly
tool_name=$(echo "$input" | jq -r '.tool_name // empty')
if [[ "$tool_name" != "write_file" && "$tool_name" != "read_file" ]]; then
  echo "Unexpected tool: $tool_name" >&2
  exit 1
fi
```

### Use timeouts

Prevent denial-of-service (hanging agents) by enforcing timeouts. Gemini CLI
defaults to 60 seconds, but you should set stricter limits for fast hooks.

```json
{
  "hooks": {
    "BeforeTool": [
      {
        "matcher": "*",
        "hooks": [
          {
            "name": "fast-validator",
            "type": "command",
            "command": "./hooks/validate.sh",
            "timeout": 5000 // 5 seconds
          }
        ]
      }
    ]
  }
}
```

### Limit permissions

Run hooks with minimal required permissions:

```bash
#!/usr/bin/env bash
# Don't run as root
if [ "$EUID" -eq 0 ]; then
  echo "Hook should not run as root" >&2
  exit 1
fi

# Check file permissions before writing
if [ -w "$file_path" ]; then
  # Safe to write
else
  echo "Insufficient permissions" >&2
  exit 1
fi
```

### Example: Secret Scanner

Use `BeforeTool` hooks to prevent committing sensitive data. This is a powerful
pattern for enhancing security in your workflow.

```javascript
const SECRET_PATTERNS = [
  /api[_-]?key\s*[:=]\s*['"]?[a-zA-Z0-9_-]{20,}['"]?/i,
  /password\s*[:=]\s*['"]?[^\s'"]{8,}['"]?/i,
  /secret\s*[:=]\s*['"]?[a-zA-Z0-9_-]{20,}['"]?/i,
  /AKIA[0-9A-Z]{16}/, // AWS access key
  /ghp_[a-zA-Z0-9]{36}/, // GitHub personal access token
  /sk-[a-zA-Z0-9]{48}/, // OpenAI API key
];

function containsSecret(content) {
  return SECRET_PATTERNS.some((pattern) => pattern.test(content));
}
```

## Privacy considerations

Hook inputs and outputs may contain sensitive information.

### What data is collected

Hook telemetry may include inputs (prompts, code) and outputs (decisions,
reasons) unless disabled.

### Privacy settings

**Disable PII logging:** If you are working with sensitive data, disable prompt
logging in your settings:

```json
{
  "telemetry": {
    "logPrompts": false
  }
}
```

**Suppress Output:** Individual hooks can request their metadata be hidden from
logs and telemetry by returning `"suppressOutput": true` in their JSON response.

> **Note**

> `suppressOutput` only affects background logging. Any `systemMessage` or
> `reason` included in the JSON will still be displayed to the user in the
> terminal.

### Sensitive data in hooks

If your hooks process sensitive data:

1. **Minimize logging:** Don't write sensitive data to log files.
2. **Sanitize outputs:** Remove sensitive data before outputting JSON or writing
   to stderr.



---



# IDE integration

Gemini CLI can integrate with your IDE to provide a more seamless and
context-aware experience. This integration allows the CLI to understand your
workspace better and enables powerful features like native in-editor diffing.

Currently, the supported IDEs are [Antigravity](https://antigravity.google),
[Visual Studio Code](https://code.visualstudio.com/), and other editors that
support VS Code extensions. To build support for other editors, see the
[IDE Companion Extension Spec](/docs/ide-integration/ide-companion-spec).

## Features

- **Workspace context:** The CLI automatically gains awareness of your workspace
  to provide more relevant and accurate responses. This context includes:
  - The **10 most recently accessed files** in your workspace.
  - Your active cursor position.
  - Any text you have selected (up to a 16KB limit; longer selections will be
    truncated).

- **Native diffing:** When Gemini suggests code modifications, you can view the
  changes directly within your IDE's native diff viewer. This allows you to
  review, edit, and accept or reject the suggested changes seamlessly.

- **VS Code commands:** You can access Gemini CLI features directly from the VS
  Code Command Palette (`Cmd+Shift+P` or `Ctrl+Shift+P`):
  - `Gemini CLI: Run`: Starts a new Gemini CLI session in the integrated
    terminal.
  - `Gemini CLI: Accept Diff`: Accepts the changes in the active diff editor.
  - `Gemini CLI: Close Diff Editor`: Rejects the changes and closes the active
    diff editor.
  - `Gemini CLI: View Third-Party Notices`: Displays the third-party notices for
    the extension.

## Installation and setup

There are three ways to set up the IDE integration:

### 1. Automatic nudge (recommended)

When you run Gemini CLI inside a supported editor, it will automatically detect
your environment and prompt you to connect. Answering "Yes" will automatically
run the necessary setup, which includes installing the companion extension and
enabling the connection.

### 2. Manual installation from CLI

If you previously dismissed the prompt or want to install the extension
manually, you can run the following command inside Gemini CLI:

```
/ide install
```

This will find the correct extension for your IDE and install it.

### 3. Manual installation from a marketplace

You can also install the extension directly from a marketplace.

- **For Visual Studio Code:** Install from the
  [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=google.gemini-cli-vscode-ide-companion).
- **For VS Code forks:** To support forks of VS Code, the extension is also
  published on the
  [Open VSX Registry](https://open-vsx.org/extension/google/gemini-cli-vscode-ide-companion).
  Follow your editor's instructions for installing extensions from this
  registry.

> NOTE: The "Gemini CLI Companion" extension may appear towards the bottom of
> search results. If you don't see it immediately, try scrolling down or sorting
> by "Newly Published".
>
> After manually installing the extension, you must run `/ide enable` in the CLI
> to activate the integration.

## Usage

### Enabling and disabling

You can control the IDE integration from within the CLI:

- To enable the connection to the IDE, run:
  ```
  /ide enable
  ```
- To disable the connection, run:
  ```
  /ide disable
  ```

When enabled, Gemini CLI will automatically attempt to connect to the IDE
companion extension.

### Checking the status

To check the connection status and see the context the CLI has received from the
IDE, run:

```
/ide status
```

If connected, this command will show the IDE it's connected to and a list of
recently opened files it is aware of.

> [!NOTE] The file list is limited to 10 recently accessed files within your
> workspace and only includes local files on disk.)

### Working with diffs

When you ask Gemini to modify a file, it can open a diff view directly in your
editor.

**To accept a diff**, you can perform any of the following actions:

- Click the **checkmark icon** in the diff editor's title bar.
- Save the file (e.g., with `Cmd+S` or `Ctrl+S`).
- Open the Command Palette and run **Gemini CLI: Accept Diff**.
- Respond with `yes` in the CLI when prompted.

**To reject a diff**, you can:

- Click the **'x' icon** in the diff editor's title bar.
- Close the diff editor tab.
- Open the Command Palette and run **Gemini CLI: Close Diff Editor**.
- Respond with `no` in the CLI when prompted.

You can also **modify the suggested changes** directly in the diff view before
accepting them.

If you select ‘Allow for this session’ in the CLI, changes will no longer show
up in the IDE as they will be auto-accepted.

## Using with sandboxing

If you are using Gemini CLI within a sandbox, please be aware of the following:

- **On macOS:** The IDE integration requires network access to communicate with
  the IDE companion extension. You must use a Seatbelt profile that allows
  network access.
- **In a Docker container:** If you run Gemini CLI inside a Docker (or Podman)
  container, the IDE integration can still connect to the VS Code extension
  running on your host machine. The CLI is configured to automatically find the
  IDE server on `host.docker.internal`. No special configuration is usually
  required, but you may need to ensure your Docker networking setup allows
  connections from the container to the host.

## Troubleshooting

If you encounter issues with IDE integration, here are some common error
messages and how to resolve them.

### Connection errors

- **Message:**
  `🔴 Disconnected: Failed to connect to IDE companion extension in [IDE Name]. Please ensure the extension is running. To install the extension, run /ide install.`
  - **Cause:** Gemini CLI could not find the necessary environment variables
    (`GEMINI_CLI_IDE_WORKSPACE_PATH` or `GEMINI_CLI_IDE_SERVER_PORT`) to connect
    to the IDE. This usually means the IDE companion extension is not running or
    did not initialize correctly.
  - **Solution:**
    1.  Make sure you have installed the **Gemini CLI Companion** extension in
        your IDE and that it is enabled.
    2.  Open a new terminal window in your IDE to ensure it picks up the correct
        environment.

- **Message:**
  `🔴 Disconnected: IDE connection error. The connection was lost unexpectedly. Please try reconnecting by running /ide enable`
  - **Cause:** The connection to the IDE companion was lost.
  - **Solution:** Run `/ide enable` to try and reconnect. If the issue
    continues, open a new terminal window or restart your IDE.

### Configuration errors

- **Message:**
  `🔴 Disconnected: Directory mismatch. Gemini CLI is running in a different location than the open workspace in [IDE Name]. Please run the CLI from one of the following directories: [List of directories]`
  - **Cause:** The CLI's current working directory is outside the workspace you
    have open in your IDE.
  - **Solution:** `cd` into the same directory that is open in your IDE and
    restart the CLI.

- **Message:**
  `🔴 Disconnected: To use this feature, please open a workspace folder in [IDE Name] and try again.`
  - **Cause:** You have no workspace open in your IDE.
  - **Solution:** Open a workspace in your IDE and restart the CLI.

### General errors

- **Message:**
  `IDE integration is not supported in your current environment. To use this feature, run Gemini CLI in one of these supported IDEs: [List of IDEs]`
  - **Cause:** You are running Gemini CLI in a terminal or environment that is
    not a supported IDE.
  - **Solution:** Run Gemini CLI from the integrated terminal of a supported
    IDE, like Antigravity or VS Code.

- **Message:**
  `No installer is available for IDE. Please install the Gemini CLI Companion extension manually from the marketplace.`
  - **Cause:** You ran `/ide install`, but the CLI does not have an automated
    installer for your specific IDE.
  - **Solution:** Open your IDE's extension marketplace, search for "Gemini CLI
    Companion", and
    [install it manually](#3-manual-installation-from-a-marketplace).



---



# Gemini CLI companion plugin: Interface specification

> Last Updated: September 15, 2025

This document defines the contract for building a companion plugin to enable
Gemini CLI's IDE mode. For VS Code, these features (native diffing, context
awareness) are provided by the official extension
([marketplace](https://marketplace.visualstudio.com/items?itemName=Google.gemini-cli-vscode-ide-companion)).
This specification is for contributors who wish to bring similar functionality
to other editors like JetBrains IDEs, Sublime Text, etc.

## I. The communication interface

Gemini CLI and the IDE plugin communicate through a local communication channel.

### 1. Transport layer: MCP over HTTP

The plugin **MUST** run a local HTTP server that implements the **Model Context
Protocol (MCP)**.

- **Protocol:** The server must be a valid MCP server. We recommend using an
  existing MCP SDK for your language of choice if available.
- **Endpoint:** The server should expose a single endpoint (e.g., `/mcp`) for
  all MCP communication.
- **Port:** The server **MUST** listen on a dynamically assigned port (i.e.,
  listen on port `0`).

### 2. Discovery mechanism: The port file

For Gemini CLI to connect, it needs to discover which IDE instance it's running
in and what port your server is using. The plugin **MUST** facilitate this by
creating a "discovery file."

- **How the CLI finds the file:** The CLI determines the Process ID (PID) of the
  IDE it's running in by traversing the process tree. It then looks for a
  discovery file that contains this PID in its name.
- **File location:** The file must be created in a specific directory:
  `os.tmpdir()/gemini/ide/`. Your plugin must create this directory if it
  doesn't exist.
- **File naming convention:** The filename is critical and **MUST** follow the
  pattern: `gemini-ide-server-${PID}-${PORT}.json`
  - `${PID}`: The process ID of the parent IDE process. Your plugin must
    determine this PID and include it in the filename.
  - `${PORT}`: The port your MCP server is listening on.
- **File content and workspace validation:** The file **MUST** contain a JSON
  object with the following structure:

  ```json
  {
    "port": 12345,
    "workspacePath": "/path/to/project1:/path/to/project2",
    "authToken": "a-very-secret-token",
    "ideInfo": {
      "name": "vscode",
      "displayName": "VS Code"
    }
  }
  ```
  - `port` (number, required): The port of the MCP server.
  - `workspacePath` (string, required): A list of all open workspace root paths,
    delimited by the OS-specific path separator (`:` for Linux/macOS, `;` for
    Windows). The CLI uses this path to ensure it's running in the same project
    folder that's open in the IDE. If the CLI's current working directory is not
    a sub-directory of `workspacePath`, the connection will be rejected. Your
    plugin **MUST** provide the correct, absolute path(s) to the root of the
    open workspace(s).
  - `authToken` (string, required): A secret token for securing the connection.
    The CLI will include this token in an `Authorization: Bearer <token>` header
    on all requests.
  - `ideInfo` (object, required): Information about the IDE.
    - `name` (string, required): A short, lowercase identifier for the IDE
      (e.g., `vscode`, `jetbrains`).
    - `displayName` (string, required): A user-friendly name for the IDE (e.g.,
      `VS Code`, `JetBrains IDE`).

- **Authentication:** To secure the connection, the plugin **MUST** generate a
  unique, secret token and include it in the discovery file. The CLI will then
  include this token in the `Authorization` header for all requests to the MCP
  server (e.g., `Authorization: Bearer a-very-secret-token`). Your server
  **MUST** validate this token on every request and reject any that are
  unauthorized.
- **Tie-breaking with environment variables (recommended):** For the most
  reliable experience, your plugin **SHOULD** both create the discovery file and
  set the `GEMINI_CLI_IDE_SERVER_PORT` environment variable in the integrated
  terminal. The file serves as the primary discovery mechanism, but the
  environment variable is crucial for tie-breaking. If a user has multiple IDE
  windows open for the same workspace, the CLI uses the
  `GEMINI_CLI_IDE_SERVER_PORT` variable to identify and connect to the correct
  window's server.

## II. The context interface

To enable context awareness, the plugin **MAY** provide the CLI with real-time
information about the user's activity in the IDE.

### `ide/contextUpdate` notification

The plugin **MAY** send an `ide/contextUpdate`
[notification](https://modelcontextprotocol.io/specification/2025-06-18/basic/index#notifications)
to the CLI whenever the user's context changes.

- **Triggering events:** This notification should be sent (with a recommended
  debounce of 50ms) when:
  - A file is opened, closed, or focused.
  - The user's cursor position or text selection changes in the active file.
- **Payload (`IdeContext`):** The notification parameters **MUST** be an
  `IdeContext` object:

  ```typescript
  interface IdeContext {
    workspaceState?: {
      openFiles?: File[];
      isTrusted?: boolean;
    };
  }

  interface File {
    // Absolute path to the file
    path: string;
    // Last focused Unix timestamp (for ordering)
    timestamp: number;
    // True if this is the currently focused file
    isActive?: boolean;
    cursor?: {
      // 1-based line number
      line: number;
      // 1-based character number
      character: number;
    };
    // The text currently selected by the user
    selectedText?: string;
  }
  ```

  **Note:** The `openFiles` list should only include files that exist on disk.
  Virtual files (e.g., unsaved files without a path, editor settings pages)
  **MUST** be excluded.

### How the CLI uses this context

After receiving the `IdeContext` object, the CLI performs several normalization
and truncation steps before sending the information to the model.

- **File ordering:** The CLI uses the `timestamp` field to determine the most
  recently used files. It sorts the `openFiles` list based on this value.
  Therefore, your plugin **MUST** provide an accurate Unix timestamp for when a
  file was last focused.
- **Active file:** The CLI considers only the most recent file (after sorting)
  to be the "active" file. It will ignore the `isActive` flag on all other files
  and clear their `cursor` and `selectedText` fields. Your plugin should focus
  on setting `isActive: true` and providing cursor/selection details only for
  the currently focused file.
- **Truncation:** To manage token limits, the CLI truncates both the file list
  (to 10 files) and the `selectedText` (to 16KB).

While the CLI handles the final truncation, it is highly recommended that your
plugin also limits the amount of context it sends.

## III. The diffing interface

To enable interactive code modifications, the plugin **MAY** expose a diffing
interface. This allows the CLI to request that the IDE open a diff view, showing
proposed changes to a file. The user can then review, edit, and ultimately
accept or reject these changes directly within the IDE.

### `openDiff` tool

The plugin **MUST** register an `openDiff` tool on its MCP server.

- **Description:** This tool instructs the IDE to open a modifiable diff view
  for a specific file.
- **Request (`OpenDiffRequest`):** The tool is invoked via a `tools/call`
  request. The `arguments` field within the request's `params` **MUST** be an
  `OpenDiffRequest` object.

  ```typescript
  interface OpenDiffRequest {
    // The absolute path to the file to be diffed.
    filePath: string;
    // The proposed new content for the file.
    newContent: string;
  }
  ```

- **Response (`CallToolResult`):** The tool **MUST** immediately return a
  `CallToolResult` to acknowledge the request and report whether the diff view
  was successfully opened.
  - On Success: If the diff view was opened successfully, the response **MUST**
    contain empty content (i.e., `content: []`).
  - On Failure: If an error prevented the diff view from opening, the response
    **MUST** have `isError: true` and include a `TextContent` block in the
    `content` array describing the error.

  The actual outcome of the diff (acceptance or rejection) is communicated
  asynchronously via notifications.

### `closeDiff` tool

The plugin **MUST** register a `closeDiff` tool on its MCP server.

- **Description:** This tool instructs the IDE to close an open diff view for a
  specific file.
- **Request (`CloseDiffRequest`):** The tool is invoked via a `tools/call`
  request. The `arguments` field within the request's `params` **MUST** be an
  `CloseDiffRequest` object.

  ```typescript
  interface CloseDiffRequest {
    // The absolute path to the file whose diff view should be closed.
    filePath: string;
  }
  ```

- **Response (`CallToolResult`):** The tool **MUST** return a `CallToolResult`.
  - On Success: If the diff view was closed successfully, the response **MUST**
    include a single **TextContent** block in the content array containing the
    file's final content before closing.
  - On Failure: If an error prevented the diff view from closing, the response
    **MUST** have `isError: true` and include a `TextContent` block in the
    `content` array describing the error.

### `ide/diffAccepted` notification

When the user accepts the changes in a diff view (e.g., by clicking an "Apply"
or "Save" button), the plugin **MUST** send an `ide/diffAccepted` notification
to the CLI.

- **Payload:** The notification parameters **MUST** include the file path and
  the final content of the file. The content may differ from the original
  `newContent` if the user made manual edits in the diff view.

  ```typescript
  {
    // The absolute path to the file that was diffed.
    filePath: string;
    // The full content of the file after acceptance.
    content: string;
  }
  ```

### `ide/diffRejected` notification

When the user rejects the changes (e.g., by closing the diff view without
accepting), the plugin **MUST** send an `ide/diffRejected` notification to the
CLI.

- **Payload:** The notification parameters **MUST** include the file path of the
  rejected diff.

  ```typescript
  {
    // The absolute path to the file that was diffed.
    filePath: string;
  }
  ```

## IV. The lifecycle interface

The plugin **MUST** manage its resources and the discovery file correctly based
on the IDE's lifecycle.

- **On activation (IDE startup/plugin enabled):**
  1.  Start the MCP server.
  2.  Create the discovery file.
- **On deactivation (IDE shutdown/plugin disabled):**
  1.  Stop the MCP server.
  2.  Delete the discovery file.



---



# Package overview

This monorepo contains two main packages: `@google/gemini-cli` and
`@google/gemini-cli-core`.

## `@google/gemini-cli`

This is the main package for the Gemini CLI. It is responsible for the user
interface, command parsing, and all other user-facing functionality.

When this package is published, it is bundled into a single executable file.
This bundle includes all of the package's dependencies, including
`@google/gemini-cli-core`. This means that whether a user installs the package
with `npm install -g @google/gemini-cli` or runs it directly with
`npx @google/gemini-cli`, they are using this single, self-contained executable.

## `@google/gemini-cli-core`

This package contains the core logic for interacting with the Gemini API. It is
responsible for making API requests, handling authentication, and managing the
local cache.

This package is not bundled. When it is published, it is published as a standard
Node.js package with its own dependencies. This allows it to be used as a
standalone package in other projects, if needed. All transpiled js code in the
`dist` folder is included in the package.

## NPM workspaces

This project uses
[NPM Workspaces](https://docs.npmjs.com/cli/v10/using-npm/workspaces) to manage
the packages within this monorepo. This simplifies development by allowing us to
manage dependencies and run scripts across multiple packages from the root of
the project.

### How it works

The root `package.json` file defines the workspaces for this project:

```json
{
  "workspaces": ["packages/*"]
}
```

This tells NPM that any folder inside the `packages` directory is a separate
package that should be managed as part of the workspace.

### Benefits of workspaces

- **Simplified dependency management**: Running `npm install` from the root of
  the project will install all dependencies for all packages in the workspace
  and link them together. This means you don't need to run `npm install` in each
  package's directory.
- **Automatic linking**: Packages within the workspace can depend on each other.
  When you run `npm install`, NPM will automatically create symlinks between the
  packages. This means that when you make changes to one package, the changes
  are immediately available to other packages that depend on it.
- **Simplified script execution**: You can run scripts in any package from the
  root of the project using the `--workspace` flag. For example, to run the
  `build` script in the `cli` package, you can run
  `npm run build --workspace @google/gemini-cli`.



---



# Gemini CLI releases

## `dev` vs `prod` environment

Our release flows support both `dev` and `prod` environments.

The `dev` environment pushes to a private Github-hosted NPM repository, with the
package names beginning with `@google-gemini/**` instead of `@google/**`.

The `prod` environment pushes to the public global NPM registry via Wombat
Dressing Room, which is Google's system for managing NPM packages in the
`@google/**` namespace. The packages are all named `@google/**`.

More information can be found about these systems in the
[NPM Package Overview](/docs/npm)

### Package scopes

| Package    | `prod` (Wombat Dressing Room) | `dev` (Github Private NPM Repo)           |
| ---------- | ----------------------------- | ----------------------------------------- |
| CLI        | @google/gemini-cli            | @google-gemini/gemini-cli                 |
| Core       | @google/gemini-cli-core       | @google-gemini/gemini-cli-core A2A Server |
| A2A Server | @google/gemini-cli-a2a-server | @google-gemini/gemini-cli-a2a-server      |

## Release cadence and tags

We will follow https://semver.org/ as closely as possible but will call out when
or if we have to deviate from it. Our weekly releases will be minor version
increments and any bug or hotfixes between releases will go out as patch
versions on the most recent release.

Each Tuesday ~2000 UTC new Stable and Preview releases will be cut. The
promotion flow is:

- Code is committed to main and pushed each night to nightly
- After no more than 1 week on main, code is promoted to the `preview` channel
- After 1 week the most recent `preview` channel is promoted to `stable` channel
- Patch fixes will be produced against both `preview` and `stable` as needed,
  with the final 'patch' version number incrementing each time.

### Preview

These releases will not have been fully vetted and may contain regressions or
other outstanding issues. Please help us test and install with `preview` tag.

```bash
npm install -g @google/gemini-cli@preview
```

### Stable

This will be the full promotion of last week's release + any bug fixes and
validations. Use `latest` tag.

```bash
npm install -g @google/gemini-cli@latest
```

### Nightly

- New releases will be published each day at UTC 0000. This will be all changes
  from the main branch as represented at time of release. It should be assumed
  there are pending validations and issues. Use `nightly` tag.

```bash
npm install -g @google/gemini-cli@nightly
```

## Weekly release promotion

Each Tuesday, the on-call engineer will trigger the "Promote Release" workflow.
This single action automates the entire weekly release process:

1.  **Promotes preview to stable:** The workflow identifies the latest `preview`
    release and promotes it to `stable`. This becomes the new `latest` version
    on npm.
2.  **Promotes nightly to preview:** The latest `nightly` release is then
    promoted to become the new `preview` version.
3.  **Prepares for next nightly:** A pull request is automatically created and
    merged to bump the version in `main` in preparation for the next nightly
    release.

This process ensures a consistent and reliable release cadence with minimal
manual intervention.

### Source of truth for versioning

To ensure the highest reliability, the release promotion process uses the **NPM
registry as the single source of truth** for determining the current version of
each release channel (`stable`, `preview`, and `nightly`).

1.  **Fetch from NPM:** The workflow begins by querying NPM's `dist-tags`
    (`latest`, `preview`, `nightly`) to get the exact version strings for the
    packages currently available to users.
2.  **Cross-check for integrity:** For each version retrieved from NPM, the
    workflow performs a critical integrity check:
    - It verifies that a corresponding **git tag** exists in the repository.
    - It verifies that a corresponding **GitHub release** has been created.
3.  **Halt on discrepancy:** If either the git tag or the GitHub Release is
    missing for a version listed on NPM, the workflow will immediately fail.
    This strict check prevents promotions from a broken or incomplete previous
    release and alerts the on-call engineer to a release state inconsistency
    that must be manually resolved.
4.  **Calculate next version:** Only after these checks pass does the workflow
    proceed to calculate the next semantic version based on the trusted version
    numbers retrieved from NPM.

This NPM-first approach, backed by integrity checks, makes the release process
highly robust and prevents the kinds of versioning discrepancies that can arise
from relying solely on git history or API outputs.

## Manual releases

For situations requiring a release outside of the regular nightly and weekly
promotion schedule, and NOT already covered by patching process, you can use the
`Release: Manual` workflow. This workflow provides a direct way to publish a
specific version from any branch, tag, or commit SHA.

### How to create a manual release

1.  Navigate to the **Actions** tab of the repository.
2.  Select the **Release: Manual** workflow from the list.
3.  Click the **Run workflow** dropdown button.
4.  Fill in the required inputs:
    - **Version**: The exact version to release (e.g., `v0.6.1`). This must be a
      valid semantic version with a `v` prefix.
    - **Ref**: The branch, tag, or full commit SHA to release from.
    - **NPM Channel**: The npm channel to publish to. The options are `preview`,
      `nightly`, `latest` (for stable releases), and `dev`. The default is
      `dev`.
    - **Dry Run**: Leave as `true` to run all steps without publishing, or set
      to `false` to perform a live release.
    - **Force Skip Tests**: Set to `true` to skip the test suite. This is not
      recommended for production releases.
    - **Skip GitHub Release**: Set to `true` to skip creating a GitHub release
      and create an npm release only.
    - **Environment**: Select the appropriate environment. The `dev` environment
      is intended for testing. The `prod` environment is intended for production
      releases. `prod` is the default and will require authorization from a
      release administrator.
5.  Click **Run workflow**.

The workflow will then proceed to test (if not skipped), build, and publish the
release. If the workflow fails during a non-dry run, it will automatically
create a GitHub issue with the failure details.

## Rollback/rollforward

In the event that a release has a critical regression, you can quickly roll back
to a previous stable version or roll forward to a new patch by changing the npm
`dist-tag`. The `Release: Change Tags` workflow provides a safe and controlled
way to do this.

This is the preferred method for both rollbacks and rollforwards, as it does not
require a full release cycle.

### How to change a release tag

1.  Navigate to the **Actions** tab of the repository.
2.  Select the **Release: Change Tags** workflow from the list.
3.  Click the **Run workflow** dropdown button.
4.  Fill in the required inputs:
    - **Version**: The existing package version that you want to point the tag
      to (e.g., `0.5.0-preview-2`). This version **must** already be published
      to the npm registry.
    - **Channel**: The npm `dist-tag` to apply (e.g., `preview`, `stable`).
    - **Dry Run**: Leave as `true` to log the action without making changes, or
      set to `false` to perform the live tag change.
    - **Environment**: Select the appropriate environment. The `dev` environment
      is intended for testing. The `prod` environment is intended for production
      releases. `prod` is the default and will require authorization from a
      release administrator.
5.  Click **Run workflow**.

The workflow will then run `npm dist-tag add` for the appropriate `gemini-cli`,
`gemini-cli-core` and `gemini-cli-a2a-server` packages, pointing the specified
channel to the specified version.

## Patching

If a critical bug that is already fixed on `main` needs to be patched on a
`stable` or `preview` release, the process is now highly automated.

### How to patch

#### 1. Create the patch pull request

There are two ways to create a patch pull request:

**Option A: From a GitHub comment (recommended)**

After a pull request containing the fix has been merged, a maintainer can add a
comment on that same PR with the following format:

`/patch [channel]`

- **channel** (optional):
  - _no channel_ - patches both stable and preview channels (default,
    recommended for most fixes)
  - `both` - patches both stable and preview channels (same as default)
  - `stable` - patches only the stable channel
  - `preview` - patches only the preview channel

Examples:

- `/patch` (patches both stable and preview - default)
- `/patch both` (patches both stable and preview - explicit)
- `/patch stable` (patches only stable)
- `/patch preview` (patches only preview)

The `Release: Patch from Comment` workflow will automatically find the merge
commit SHA and trigger the `Release: Patch (1) Create PR` workflow. If the PR is
not yet merged, it will post a comment indicating the failure.

**Option B: Manually triggering the workflow**

Navigate to the **Actions** tab and run the **Release: Patch (1) Create PR**
workflow.

- **Commit**: The full SHA of the commit on `main` that you want to cherry-pick.
- **Channel**: The channel you want to patch (`stable` or `preview`).

This workflow will automatically:

1.  Find the latest release tag for the channel.
2.  Create a release branch from that tag if one doesn't exist (e.g.,
    `release/v0.5.1-pr-12345`).
3.  Create a new hotfix branch from the release branch.
4.  Cherry-pick your specified commit into the hotfix branch.
5.  Create a pull request from the hotfix branch back to the release branch.

#### 2. Review and merge

Review the automatically created pull request(s) to ensure the cherry-pick was
successful and the changes are correct. Once approved, merge the pull request.

**Security note:** The `release/*` branches are protected by branch protection
rules. A pull request to one of these branches requires at least one review from
a code owner before it can be merged. This ensures that no unauthorized code is
released.

#### 2.5. Adding multiple commits to a hotfix (advanced)

If you need to include multiple fixes in a single patch release, you can add
additional commits to the hotfix branch after the initial patch PR has been
created:

1. **Start with the primary fix**: Use `/patch` (or `/patch both`) on the most
   important PR to create the initial hotfix branch and PR.

2. **Checkout the hotfix branch locally**:

   ```bash
   git fetch origin
   git checkout hotfix/v0.5.1/stable/cherry-pick-abc1234  # Use the actual branch name from the PR
   ```

3. **Cherry-pick additional commits**:

   ```bash
   git cherry-pick <commit-sha-1>
   git cherry-pick <commit-sha-2>
   # Add as many commits as needed
   ```

4. **Push the updated branch**:

   ```bash
   git push origin hotfix/v0.5.1/stable/cherry-pick-abc1234
   ```

5. **Test and review**: The existing patch PR will automatically update with
   your additional commits. Test thoroughly since you're now releasing multiple
   changes together.

6. **Update the PR description**: Consider updating the PR title and description
   to reflect that it includes multiple fixes.

This approach allows you to group related fixes into a single patch release
while maintaining full control over what gets included and how conflicts are
resolved.

#### 3. Automatic release

Upon merging the pull request, the `Release: Patch (2) Trigger` workflow is
automatically triggered. It will then start the `Release: Patch (3) Release`
workflow, which will:

1.  Build and test the patched code.
2.  Publish the new patch version to npm.
3.  Create a new GitHub release with the patch notes.

This fully automated process ensures that patches are created and released
consistently and reliably.

#### Troubleshooting: Older branch workflows

**Issue**: If the patch trigger workflow fails with errors like "Resource not
accessible by integration" or references to non-existent workflow files (e.g.,
`patch-release.yml`), this indicates the hotfix branch contains an outdated
version of the workflow files.

**Root cause**: When a PR is merged, GitHub Actions runs the workflow definition
from the **source branch** (the hotfix branch), not from the target branch (the
release branch). If the hotfix branch was created from an older release branch
that predates workflow improvements, it will use the old workflow logic.

**Solutions**:

**Option 1: Manual trigger (quick fix)** Manually trigger the updated workflow
from the branch with the latest workflow code:

```bash
# For a preview channel patch with tests skipped
gh workflow run release-patch-2-trigger.yml --ref <branch-with-updated-workflow> \
  --field ref="hotfix/v0.6.0-preview.2/preview/cherry-pick-abc1234" \
  --field workflow_ref=<branch-with-updated-workflow> \
  --field dry_run=false \
  --field force_skip_tests=true

# For a stable channel patch
gh workflow run release-patch-2-trigger.yml --ref <branch-with-updated-workflow> \
  --field ref="hotfix/v0.5.1/stable/cherry-pick-abc1234" \
  --field workflow_ref=<branch-with-updated-workflow> \
  --field dry_run=false \
  --field force_skip_tests=false

# Example using main branch (most common case)
gh workflow run release-patch-2-trigger.yml --ref main \
  --field ref="hotfix/v0.6.0-preview.2/preview/cherry-pick-abc1234" \
  --field workflow_ref=main \
  --field dry_run=false \
  --field force_skip_tests=true
```

**Note**: Replace `<branch-with-updated-workflow>` with the branch containing
the latest workflow improvements (usually `main`, but could be a feature branch
if testing updates).

**Option 2: Update the hotfix branch** Merge the latest main branch into your
hotfix branch to get the updated workflows:

```bash
git checkout hotfix/v0.6.0-preview.2/preview/cherry-pick-abc1234
git merge main
git push
```

Then close and reopen the PR to retrigger the workflow with the updated version.

**Option 3: Direct release trigger** Skip the trigger workflow entirely and
directly run the release workflow:

```bash
# Replace channel and release_ref with appropriate values
gh workflow run release-patch-3-release.yml --ref main \
  --field type="preview" \
  --field dry_run=false \
  --field force_skip_tests=true \
  --field release_ref="release/v0.6.0-preview.2"
```

### Docker

We also run a Google cloud build called
[release-docker.yml](https://github.com/google-gemini/gemini-cli/blob/main/.gcp/release-docker.yml). Which publishes the sandbox
docker to match your release. This will also be moved to GH and combined with
the main release file once service account permissions are sorted out.

## Release validation

After pushing a new release smoke testing should be performed to ensure that the
packages are working as expected. This can be done by installing the packages
locally and running a set of tests to ensure that they are functioning
correctly.

- `npx -y @google/gemini-cli@latest --version` to validate the push worked as
  expected if you were not doing a rc or dev tag
- `npx -y @google/gemini-cli@<release tag> --version` to validate the tag pushed
  appropriately
- _This is destructive locally_
  `npm uninstall @google/gemini-cli && npm uninstall -g @google/gemini-cli && npm cache clean --force &&  npm install @google/gemini-cli@<version>`
- Smoke testing a basic run through of exercising a few llm commands and tools
  is recommended to ensure that the packages are working as expected. We'll
  codify this more in the future.

## Local testing and validation: Changes to the packaging and publishing process

If you need to test the release process without actually publishing to NPM or
creating a public GitHub release, you can trigger the workflow manually from the
GitHub UI.

1.  Go to the
    [Actions tab](https://github.com/google-gemini/gemini-cli/actions/workflows/release-manual.yml)
    of the repository.
2.  Click on the "Run workflow" dropdown.
3.  Leave the `dry_run` option checked (`true`).
4.  Click the "Run workflow" button.

This will run the entire release process but will skip the `npm publish` and
`gh release create` steps. You can inspect the workflow logs to ensure
everything is working as expected.

It is crucial to test any changes to the packaging and publishing process
locally before committing them. This ensures that the packages will be published
correctly and that they will work as expected when installed by a user.

To validate your changes, you can perform a dry run of the publishing process.
This will simulate the publishing process without actually publishing the
packages to the npm registry.

```bash
npm_package_version=9.9.9 SANDBOX_IMAGE_REGISTRY="registry" SANDBOX_IMAGE_NAME="thename" npm run publish:npm --dry-run
```

This command will do the following:

1.  Build all the packages.
2.  Run all the prepublish scripts.
3.  Create the package tarballs that would be published to npm.
4.  Print a summary of the packages that would be published.

You can then inspect the generated tarballs to ensure that they contain the
correct files and that the `package.json` files have been updated correctly. The
tarballs will be created in the root of each package's directory (e.g.,
`packages/cli/google-gemini-cli-0.1.6.tgz`).

By performing a dry run, you can be confident that your changes to the packaging
process are correct and that the packages will be published successfully.

## Release deep dive

The release process creates two distinct types of artifacts for different
distribution channels: standard packages for the NPM registry and a single,
self-contained executable for GitHub Releases.

Here are the key stages:

**Stage 1: Pre-release sanity checks and versioning**

- **What happens:** Before any files are moved, the process ensures the project
  is in a good state. This involves running tests, linting, and type-checking
  (`npm run preflight`). The version number in the root `package.json` and
  `packages/cli/package.json` is updated to the new release version.

**Stage 2: Building the source code for NPM**

- **What happens:** The TypeScript source code in `packages/core/src` and
  `packages/cli/src` is compiled into standard JavaScript.
- **File movement:**
  - `packages/core/src/**/*.ts` -> compiled to -> `packages/core/dist/`
  - `packages/cli/src/**/*.ts` -> compiled to -> `packages/cli/dist/`
- **Why:** The TypeScript code written during development needs to be converted
  into plain JavaScript that can be run by Node.js. The `core` package is built
  first as the `cli` package depends on it.

**Stage 3: Publishing standard packages to NPM**

- **What happens:** The `npm publish` command is run for the
  `@google/gemini-cli-core` and `@google/gemini-cli` packages.
- **Why:** This publishes them as standard Node.js packages. Users installing
  via `npm install -g @google/gemini-cli` will download these packages, and
  `npm` will handle installing the `@google/gemini-cli-core` dependency
  automatically. The code in these packages is not bundled into a single file.

**Stage 4: Assembling and creating the GitHub release asset**

This stage happens _after_ the NPM publish and creates the single-file
executable that enables `npx` usage directly from the GitHub repository.

1.  **The JavaScript bundle is created:**
    - **What happens:** The built JavaScript from both `packages/core/dist` and
      `packages/cli/dist`, along with all third-party JavaScript dependencies,
      are bundled by `esbuild` into a single, executable JavaScript file (e.g.,
      `gemini.js`). The `node-pty` library is excluded from this bundle as it
      contains native binaries.
    - **Why:** This creates a single, optimized file that contains all the
      necessary application code. It simplifies execution for users who want to
      run the CLI without a full `npm install`, as all dependencies (including
      the `core` package) are included directly.

2.  **The `bundle` directory is assembled:**
    - **What happens:** A temporary `bundle` folder is created at the project
      root. The single `gemini.js` executable is placed inside it, along with
      other essential files.
    - **File movement:**
      - `gemini.js` (from esbuild) -> `bundle/gemini.js`
      - `README.md` -> `bundle/README.md`
      - `LICENSE` -> `bundle/LICENSE`
      - `packages/cli/src/utils/*.sb` (sandbox profiles) -> `bundle/`
    - **Why:** This creates a clean, self-contained directory with everything
      needed to run the CLI and understand its license and usage.

3.  **The GitHub release is created:**
    - **What happens:** The contents of the `bundle` directory, including the
      `gemini.js` executable, are attached as assets to a new GitHub Release.
    - **Why:** This makes the single-file version of the CLI available for
      direct download and enables the
      `npx https://github.com/google-gemini/gemini-cli` command, which downloads
      and runs this specific bundled asset.

**Summary of artifacts**

- **NPM:** Publishes standard, un-bundled Node.js packages. The primary artifact
  is the code in `packages/cli/dist`, which depends on
  `@google/gemini-cli-core`.
- **GitHub release:** Publishes a single, bundled `gemini.js` file that contains
  all dependencies, for easy execution via `npx`.

This dual-artifact process ensures that both traditional `npm` users and those
who prefer the convenience of `npx` have an optimized experience.

## Notifications

Failing release workflows will automatically create an issue with the label
`release-failure`.

A notification will be posted to the maintainer's chat channel when issues with
this type are created.

### Modifying chat notifications

Notifications use
[GitHub for Google Chat](https://workspace.google.com/marketplace/app/github_for_google_chat/536184076190).
To modify the notifications, use `/github-settings` within the chat space.

> [!WARNING] The following instructions describe a fragile workaround that
> depends on the internal structure of the chat application's UI. It is likely
> to break with future updates.

The list of available labels is not currently populated correctly. If you want
to add a label that does not appear alphabetically in the first 30 labels in the
repo, you must use your browser's developer tools to manually modify the UI:

1. Open your browser's developer tools (e.g., Chrome DevTools).
2. In the `/github-settings` dialog, inspect the list of labels.
3. Locate one of the `<li>` elements representing a label.
4. In the HTML, modify the `data-option-value` attribute of that `<li>` element
   to the desired label name (e.g., `release-failure`).
5. Click on your modified label in the UI to select it, then save your settings.



---



# Integration tests

This document provides information about the integration testing framework used
in this project.

## Overview

The integration tests are designed to validate the end-to-end functionality of
the Gemini CLI. They execute the built binary in a controlled environment and
verify that it behaves as expected when interacting with the file system.

These tests are located in the `integration-tests` directory and are run using a
custom test runner.

## Building the tests

Prior to running any integration tests, you need to create a release bundle that
you want to actually test:

```bash
npm run bundle
```

You must re-run this command after making any changes to the CLI source code,
but not after making changes to tests.

## Running the tests

The integration tests are not run as part of the default `npm run test` command.
They must be run explicitly using the `npm run test:integration:all` script.

The integration tests can also be run using the following shortcut:

```bash
npm run test:e2e
```

## Running a specific set of tests

To run a subset of test files, you can use
`npm run <integration test command> <file_name1> ....` where &lt;integration
test command&gt; is either `test:e2e` or `test:integration*` and `<file_name>`
is any of the `.test.js` files in the `integration-tests/` directory. For
example, the following command runs `list_directory.test.js` and
`write_file.test.js`:

```bash
npm run test:e2e list_directory write_file
```

### Running a single test by name

To run a single test by its name, use the `--test-name-pattern` flag:

```bash
npm run test:e2e -- --test-name-pattern "reads a file"
```

### Regenerating model responses

Some integration tests use faked out model responses, which may need to be
regenerated from time to time as the implementations change.

To regenerate these golden files, set the REGENERATE_MODEL_GOLDENS environment
variable to "true" when running the tests, for example:

**WARNING**: If running locally you should review these updated responses for
any information about yourself or your system that gemini may have included in
these responses.

```bash
REGENERATE_MODEL_GOLDENS="true" npm run test:e2e
```

**WARNING**: Make sure you run **await rig.cleanup()** at the end of your test,
else the golden files will not be updated.

### Deflaking a test

Before adding a **new** integration test, you should test it at least 5 times
with the deflake script or workflow to make sure that it is not flaky.

### Deflake script

```bash
npm run deflake -- --runs=5 --command="npm run test:e2e -- -- --test-name-pattern '<your-new-test-name>'"
```

#### Deflake workflow

```bash
gh workflow run deflake.yml --ref <your-branch> -f test_name_pattern="<your-test-name-pattern>"
```

### Running all tests

To run the entire suite of integration tests, use the following command:

```bash
npm run test:integration:all
```

### Sandbox matrix

The `all` command will run tests for `no sandboxing`, `docker` and `podman`.
Each individual type can be run using the following commands:

```bash
npm run test:integration:sandbox:none
```

```bash
npm run test:integration:sandbox:docker
```

```bash
npm run test:integration:sandbox:podman
```

## Diagnostics

The integration test runner provides several options for diagnostics to help
track down test failures.

### Keeping test output

You can preserve the temporary files created during a test run for inspection.
This is useful for debugging issues with file system operations.

To keep the test output set the `KEEP_OUTPUT` environment variable to `true`.

```bash
KEEP_OUTPUT=true npm run test:integration:sandbox:none
```

When output is kept, the test runner will print the path to the unique directory
for the test run.

### Verbose output

For more detailed debugging, set the `VERBOSE` environment variable to `true`.

```bash
VERBOSE=true npm run test:integration:sandbox:none
```

When using `VERBOSE=true` and `KEEP_OUTPUT=true` in the same command, the output
is streamed to the console and also saved to a log file within the test's
temporary directory.

The verbose output is formatted to clearly identify the source of the logs:

```
--- TEST: <log dir>:<test-name> ---
... output from the gemini command ...
--- END TEST: <log dir>:<test-name> ---
```

## Linting and formatting

To ensure code quality and consistency, the integration test files are linted as
part of the main build process. You can also manually run the linter and
auto-fixer.

### Running the linter

To check for linting errors, run the following command:

```bash
npm run lint
```

You can include the `:fix` flag in the command to automatically fix any fixable
linting errors:

```bash
npm run lint:fix
```

## Directory structure

The integration tests create a unique directory for each test run inside the
`.integration-tests` directory. Within this directory, a subdirectory is created
for each test file, and within that, a subdirectory is created for each
individual test case.

This structure makes it easy to locate the artifacts for a specific test run,
file, or case.

```
.integration-tests/
└── <run-id>/
    └── <test-file-name>.test.js/
        └── <test-case-name>/
            ├── output.log
            └── ...other test artifacts...
```

## Continuous integration

To ensure the integration tests are always run, a GitHub Actions workflow is
defined in `.github/workflows/chained_e2e.yml`. This workflow automatically runs
the integrations tests for pull requests against the `main` branch, or when a
pull request is added to a merge queue.

The workflow runs the tests in different sandboxing environments to ensure
Gemini CLI is tested across each:

- `sandbox:none`: Runs the tests without any sandboxing.
- `sandbox:docker`: Runs the tests in a Docker container.
- `sandbox:podman`: Runs the tests in a Podman container.



---



# Automation and triage processes

This document provides a detailed overview of the automated processes we use to
manage and triage issues and pull requests. Our goal is to provide prompt
feedback and ensure that contributions are reviewed and integrated efficiently.
Understanding this automation will help you as a contributor know what to expect
and how to best interact with our repository bots.

## Guiding principle: Issues and pull requests

First and foremost, almost every Pull Request (PR) should be linked to a
corresponding Issue. The issue describes the "what" and the "why" (the bug or
feature), while the PR is the "how" (the implementation). This separation helps
us track work, prioritize features, and maintain clear historical context. Our
automation is built around this principle.

> **Note:** Issues tagged as "🔒Maintainers only" are reserved for project
> maintainers. We will not accept pull requests related to these issues.

---

## Detailed automation workflows

Here is a breakdown of the specific automation workflows that run in our
repository.

### 1. When you open an issue: `Automated Issue Triage`

This is the first bot you will interact with when you create an issue. Its job
is to perform an initial analysis and apply the correct labels.

- **Workflow File**: `.github/workflows/gemini-automated-issue-triage.yml`
- **When it runs**: Immediately after an issue is created or reopened.
- **What it does**:
  - It uses a Gemini model to analyze the issue's title and body against a
    detailed set of guidelines.
  - **Applies one `area/*` label**: Categorizes the issue into a functional area
    of the project (e.g., `area/ux`, `area/models`, `area/platform`).
  - **Applies one `kind/*` label**: Identifies the type of issue (e.g.,
    `kind/bug`, `kind/enhancement`, `kind/question`).
  - **Applies one `priority/*` label**: Assigns a priority from P0 (critical) to
    P3 (low) based on the described impact.
  - **May apply `status/need-information`**: If the issue lacks critical details
    (like logs or reproduction steps), it will be flagged for more information.
  - **May apply `status/need-retesting`**: If the issue references a CLI version
    that is more than six versions old, it will be flagged for retesting on a
    current version.
- **What you should do**:
  - Fill out the issue template as completely as possible. The more detail you
    provide, the more accurate the triage will be.
  - If the `status/need-information` label is added, please provide the
    requested details in a comment.

### 2. When you open a pull request: `Continuous Integration (CI)`

This workflow ensures that all changes meet our quality standards before they
can be merged.

- **Workflow File**: `.github/workflows/ci.yml`
- **When it runs**: On every push to a pull request.
- **What it does**:
  - **Lint**: Checks that your code adheres to our project's formatting and
    style rules.
  - **Test**: Runs our full suite of automated tests across macOS, Windows, and
    Linux, and on multiple Node.js versions. This is the most time-consuming
    part of the CI process.
  - **Post Coverage Comment**: After all tests have successfully passed, a bot
    will post a comment on your PR. This comment provides a summary of how well
    your changes are covered by tests.
- **What you should do**:
  - Ensure all CI checks pass. A green checkmark ✅ will appear next to your
    commit when everything is successful.
  - If a check fails (a red "X" ❌), click the "Details" link next to the failed
    check to view the logs, identify the problem, and push a fix.

### 3. Ongoing triage for pull requests: `PR Auditing and Label Sync`

This workflow runs periodically to ensure all open PRs are correctly linked to
issues and have consistent labels.

- **Workflow File**: `.github/workflows/gemini-scheduled-pr-triage.yml`
- **When it runs**: Every 15 minutes on all open pull requests.
- **What it does**:
  - **Checks for a linked issue**: The bot scans your PR description for a
    keyword that links it to an issue (e.g., `Fixes #123`, `Closes #456`).
  - **Adds `status/need-issue`**: If no linked issue is found, the bot will add
    the `status/need-issue` label to your PR. This is a clear signal that an
    issue needs to be created and linked.
  - **Synchronizes labels**: If an issue _is_ linked, the bot ensures the PR's
    labels perfectly match the issue's labels. It will add any missing labels
    and remove any that don't belong, and it will remove the `status/need-issue`
    label if it was present.
- **What you should do**:
  - **Always link your PR to an issue.** This is the most important step. Add a
    line like `Resolves #<issue-number>` to your PR description.
  - This will ensure your PR is correctly categorized and moves through the
    review process smoothly.

### 4. Ongoing triage for issues: `Scheduled Issue Triage`

This is a fallback workflow to ensure that no issue gets missed by the triage
process.

- **Workflow File**: `.github/workflows/gemini-scheduled-issue-triage.yml`
- **When it runs**: Every hour on all open issues.
- **What it does**:
  - It actively seeks out issues that either have no labels at all or still have
    the `status/need-triage` label.
  - It then triggers the same powerful Gemini-based analysis as the initial
    triage bot to apply the correct labels.
- **What you should do**:
  - You typically don't need to do anything. This workflow is a safety net to
    ensure every issue is eventually categorized, even if the initial triage
    fails.

### 5. Release automation

This workflow handles the process of packaging and publishing new versions of
the Gemini CLI.

- **Workflow File**: `.github/workflows/release-manual.yml`
- **When it runs**: On a daily schedule for "nightly" releases, and manually for
  official patch/minor releases.
- **What it does**:
  - Automatically builds the project, bumps the version numbers, and publishes
    the packages to npm.
  - Creates a corresponding release on GitHub with generated release notes.
- **What you should do**:
  - As a contributor, you don't need to do anything for this process. You can be
    confident that once your PR is merged into the `main` branch, your changes
    will be included in the very next nightly release.

We hope this detailed overview is helpful. If you have any questions about our
automation or processes, please don't hesitate to ask!



---
