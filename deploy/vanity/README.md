# remount.dev vanity import site

The Go module path is `remount.dev/remount`. `go get` and `go install`
resolve it by fetching `https://remount.dev/remount?go-get=1` and reading the
`go-import` meta tag in this directory's `index.html`, which points at the
GitHub repository. `install.sh` is served from the same host so the documented
one-liner works.

This is a static site with no build step and no runtime: one HTML page
that carries the meta tags and a short developer-facing landing page, plus
the installer rewrite. It is the only piece of Remount that is hosted.

## Deploy on Vercel

The Vercel project `remount-vanity` (scope `andrew-gaos-projects-7ae0e473`)
is connected to this GitHub repository with **Root Directory** set to
`deploy/vanity` and Framework "Other". Every push to `main` that touches this
directory redeploys it; nothing else in the repository is built. The domains
`remount.dev` and `get.remount.dev` are attached to that project, and the
domain is registered through Vercel Domains, so DNS lives there too.

The root directory matters: without it Vercel treats the repository's `api/`
package as Go serverless functions and the build fails with "Could not find
an exported function in api/api.go". If a build ever fails that way, the
setting has been lost.

To deploy by hand instead of by push, link the checkout to the project once
and deploy from the repository root (the root-directory setting is applied
server-side):

```sh
npm i -g vercel            # once
vercel login
vercel link --yes --scope andrew-gaos-projects-7ae0e473 --project remount-vanity
vercel deploy --prod --yes --scope andrew-gaos-projects-7ae0e473
```

The `*.vercel.app` deployment URLs sit behind Vercel's deployment protection
and redirect to a login; only the custom domains are public, which is what
Go and `curl` use.

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
  `docs/releases.md`. While the repository is private the raw URL answers
  404, so `get.remount.dev/install.sh` does too; once it is public the
  installer will run and, until a release is tagged, truthfully report that
  no release exists.
