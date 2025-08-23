_default:
  just --list 

alias f:= format
alias l:= lint
alias lf:= lint-fix

# format
format:
    cargo fmt --all

# format in CI
format-ci:
    RUSTFLAGS="--deny warnings" cargo fmt --all --check

# Show lint error
lint:
    cargo clippy --workspace --all-targets --all-features 

# Fix clippy error
lint-fix:
    cargo clippy --fix --workspace --all-targets --all-features --allow-dirty --allow-staged

# lint in CI
lint-ci:
    RUSTFLAGS="--deny warnings" cargo clippy --workspace --all-targets --all-features

# Run tests
test:
    cargo test --workspace

# Generate by sqlc
generate:
    sqlc generate -f crates/tasuki-sqlx/sqlc.json
    just f

# Reset database
reset-db:
    sqlx database reset --source crates/tasuki-sqlx/migrations