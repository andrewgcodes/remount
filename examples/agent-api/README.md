# Agent API from Python and TypeScript

Both programs use the generated Remount SDK and demonstrate the same durable
sequence: create an Agent, wait until it needs input, send a message, wait for
a pending approval, and choose one of the server-offered options.

Install the repository SDKs and run either client:

```sh
python -m pip install -e ../../sdk/python
export REMOUNT_SERVER=https://remount.example REMOUNT_TOKEN=...
python python_example.py --binding b_openai \
  --operation-id ticket-123 \
  --task 'Ask which test to run, then request permission before changing a file.' \
  --reply 'Run the reconnect test and fix it.'

npm install
npx tsx typescript_example.ts \
  'Ask which test to run, then request permission before changing a file.' \
  'Run the reconnect test and fix it.'
```

The workspace binding is an ID only. Its secret remains on the node. Pass the
same operation ID when restarting an ambiguous orchestration attempt; each
mutation derives a stable, stage-specific idempotency key from it. Status and
approval polling are read-only and never wake a sleeping Agent. The example
intentionally fails if the requested approval option was not offered.

The smoke tests use deterministic fake clients to prove call ordering and
request shape. They do not claim a model provider or real harness was run.
