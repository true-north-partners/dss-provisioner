default:
    @just --list

[doc('Install all dependencies')]
install:
    uv sync

# --- helpers ---

[private]
[script('bash')]
_on_changed +cmd:
    changed=()
    while IFS= read -r file; do
        changed+=("$file")
    done < <(git diff "$(git merge-base --fork-point origin/main)" --diff-filter=d --name-only "*.py")
    [[ ${#changed[@]} -eq 0 ]] && echo "No files changed!" && exit 0
    {{ cmd }} "${changed[@]}"

# --- ruff ---

[doc('Run ruff linter')]
[group('ruff')]
lint *files:
    uv run ruff check {{ files }}

[doc('Run ruff formatter')]
[group('ruff')]
format *files:
    uv run ruff format {{ files }}

[doc('Check formatting (no changes)')]
[group('ruff')]
format_check *files:
    uv run ruff format --check {{ files }}

[doc('Lint changed files vs main')]
[group('ruff')]
lint_changed: (_on_changed "uv run ruff check")

[doc('Format changed files vs main')]
[group('ruff')]
format_changed: (_on_changed "uv run ruff format")

[doc('Lint + format + type-check')]
[group('ruff')]
check: lint format_check type_check

# --- ty ---

[doc('Run ty type checker')]
[group('ty')]
type_check *files:
    uv run ty check src tests {{ files }}

[doc('Type-check changed files vs main')]
[group('ty')]
type_check_changed: (_on_changed "uv run ty check")

# --- project ---

[doc('Wipe venv, dist, egg-info and uv cache')]
clean:
    rm -rf .venv dist *egg-info && uv cache clean

[doc('Build wheel and sdist')]
build:
    uv build --no-sources --no-cache

[doc('Prepare a release')]
prepare_release *version:
    uv run release/prepare.py {{ version }}

[doc('Run test suite with coverage')]
test:
    uv run pytest --cov=dss_provisioner --cov-report=term-missing

[doc('Build documentation')]
build_docs:
    uv run mkdocs build --clean

[doc('Serve documentation locally')]
serve_docs:
    uv run mkdocs serve

[doc('Clean, install, check, test, build, docs')]
all: clean install check test build build_docs
