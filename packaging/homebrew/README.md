# Homebrew tap contract

The tap is an external repository named `andrewgcodes/homebrew-tap`; Homebrew
maps that name to `brew install andrewgcodes/tap/remount`. This repository does
not carry a hand-edited formula because its URLs and SHA-256 values must come
from one published release manifest.

After E18 verifies a tag, it attaches `remount.rb` to that release. A maintainer
copies the exact file to `Formula/remount.rb` in the tap and records the tap
commit in the release ledger. To reproduce it locally:

```sh
gh release download v0.1.0 --repo andrewgcodes/remount \
  --pattern checksums.txt --dir release
python3 scripts/release/generate_homebrew.py \
  --tag v0.1.0 \
  --repository andrewgcodes/remount \
  --checksums release/checksums.txt \
  --output release/remount.rb
ruby -c release/remount.rb
brew style release/remount.rb
```

Before updating the tap, install the formula from its local path on every
supported Homebrew OS/architecture lane and require `remount version` to report
the same tag. Publishing to the tap is intentionally not automated from this
repository: it needs separate repository authority and a separately reviewed
commit.
