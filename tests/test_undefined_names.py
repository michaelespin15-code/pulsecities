"""
No module may reference a name it never binds.

On 2026-08-28 scheduler/alerts.py started calling mailer.send() and
mailer.EmailRefused without importing mailer. Every ops email raised NameError
for a day, including the one escalating a frozen upstream feed. It took a live
log line to find, because the failure only happens on the branch that needs a
real API key and the test suite blanks that key by design.

Nothing about that bug needed a database, an environment or a mock to see. It
is visible in the file. This is the check that sees it, over every module in
the repo, in under a second.

Deliberately narrow. pyflakes also reports unused imports, f-strings with no
placeholders and dead locals; there are around a hundred of those here and
none of them is a bug. Undefined names are different in kind: every one is a
crash waiting for the right branch to execute.
"""
import ast
from pathlib import Path

from pyflakes import messages
from pyflakes.checker import Checker

REPO = Path(__file__).resolve().parent.parent

# Everything the repo actually ships or runs. Not venv, node_modules or caches.
PACKAGES = (
    "api", "config", "migrations", "models",
    "scheduler", "scoring", "scrapers", "scripts", "tests",
)

# A name that is read but never bound is a NameError on that line. A local read
# before assignment is the same thing inside a function. An __all__ entry that
# names nothing is a broken import for whoever does `from x import *`.
FATAL = (messages.UndefinedName, messages.UndefinedLocal, messages.UndefinedExport)


def _python_files():
    for pkg in PACKAGES:
        for path in sorted((REPO / pkg).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            yield path


class TestEveryNameResolves:
    def test_no_module_reads_a_name_it_never_binds(self):
        offenders = []
        for path in _python_files():
            rel = path.relative_to(REPO).as_posix()
            source = path.read_text()
            try:
                tree = ast.parse(source, filename=rel)
            except SyntaxError as exc:  # a file that will not parse cannot run
                offenders.append(f"{rel}:{exc.lineno}: {exc.msg}")
                continue
            for msg in Checker(tree, filename=rel).messages:
                if isinstance(msg, FATAL):
                    offenders.append(
                        f"{rel}:{msg.lineno}: {msg.message % msg.message_args}"
                    )

        assert not offenders, (
            "These names are read and never bound. Each is a NameError the "
            "moment its branch runs:\n  " + "\n  ".join(offenders)
        )
