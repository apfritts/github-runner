#!/bin/bash
# Updates this fork's runner on Linux arm64. Stop the runner first.
cd ~/actions-runner
T=$(curl -s https://api.github.com/repos/apfritts/github-runner/releases/latest | grep -m1 tag_name | cut -d'"' -f4)
V=${T#v}; V=${V%-multi*}
curl -LO https://github.com/apfritts/github-runner/releases/download/$T/actions-runner-linux-arm64-$V.tar.gz
rm -rf bin.old externals.old; mv bin bin.old; mv externals externals.old
tar xzf actions-runner-linux-arm64-$V.tar.gz
./bin/Runner.Listener --version
