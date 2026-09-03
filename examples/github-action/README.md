# Composite GitHub Action

The action runs `remount run` against a hosted control plane and returns the
durable Agent and workspace IDs. The Remount token stays in the subprocess
environment, while the repository and binding ID are ordinary arguments.

```yaml
steps:
  - uses: actions/checkout@v4
  - uses: your-org/remount/examples/github-action@v1
    id: remount
    with:
      server: https://remount.example
      token: ${{ secrets.REMOUNT_TOKEN }}
      repo-binding: b_github_this_repo
      task: Fix the failing test and open a patch.
  - run: echo "Agent ${{ steps.remount.outputs.agent-id }}"
```

`repo-binding` must already exist on the Remount server and be restricted to
the current GitHub repository. Passing a broad GitHub token does not make it
repo-scoped; scope is a server-side binding policy. The action refuses plain
HTTP unless `allow-insecure-http: true`, which exists only for local tests.

The runner must install a `remount` binary whose version you have pinned and
verified before this action. Release installation and signature verification
are deliberately separate from executing an agent.

The smoke test substitutes a fake binary, asserts the exact `run --repo
... --binding ... --detach` invocation and proves the bearer token never enters
the process argument list. It does not claim a hosted control plane or GitHub
App was exercised.

Reference: [GitHub composite actions](https://docs.github.com/en/actions/tutorials/create-actions/create-a-composite-action).
