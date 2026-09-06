# remount.dev vanity import site

The Go module path is `remount.dev/remount`. `go get` and `go install`
resolve it by fetching `https://remount.dev/remount?go-get=1` and reading the
`go-import` meta tag in this directory's `index.html`, which points at the
GitHub repository. `install.sh` is served from the same host so the documented
one-liner works.

This is a static site with no build step and no runtime. It is the only
piece of Remount that is hosted, and it hosts nothing but two meta tags and a
redirect.

## Deploy on Vercel

The domain is registered through Vercel Domains, so DNS is already managed
there.

```sh
npm i -g vercel            # once
cd deploy/vanity
vercel link                # create a new project named "remount-vanity" when asked
vercel domains add remount.dev
vercel domains add get.remount.dev   # optional alias for install.sh
vercel --prod
```

Then verify from any machine:

```sh
curl -sS 'https://remount.dev/remount?go-get=1' | grep go-import
go install remount.dev/remount/cmd/remount@main    # works once the repository is public
```

`go install` also needs the repository to be reachable over `https` without
credentials, which means public, or a tagged release once one exists.

## What each rewrite is for

- `/remount` and `/remount/*`: Go requests the full import path of whatever
  package it is fetching (`/remount/cmd/remount?go-get=1`), so every subpath
  must answer with the same meta tag.
- `/install.sh`: proxies the installer from the repository's `main` branch.
  Point `get.remount.dev` at the same project so
  `curl -fsSL https://get.remount.dev/install.sh | sh` matches
  `docs/releases.md`. Until a release is tagged the installer's download step
  will report that no release exists, which is the truthful outcome.
