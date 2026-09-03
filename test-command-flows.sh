#!/bin/zsh
set -eu

repo=${0:A:h}

dotnet test "$repo/Slon.Tests/Slon.Tests.csproj" -c Release \
    -p:CommandFlowImplementation=Legacy "$@"
dotnet test "$repo/Slon.Tests/Slon.Tests.csproj" -c Release \
    -p:CommandFlowImplementation=Next "$@"
