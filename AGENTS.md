docs/DESIGN.md contains the purpose of this project and the design decisions.

docs/PLAN.md contains the implementation plan for the project.

`default.nix` builds the package (`nix-build default.nix`). `shell.sh` enters the
development shell, or runs one command in it: `./shell.sh cargo test`.

`docs/hashsync.1` is the man page, in mdoc. Change it along with the commands,
options, and configuration keys that it describes; tests check that each of
them has an entry there, and that no other entry does.
