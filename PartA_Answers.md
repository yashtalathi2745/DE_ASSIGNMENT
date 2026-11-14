# Part A — Git and CI/CD
### Assignment: From Git to Gold | Roll No: 23BCS246

---

## A1. Repository Structure

GitHub repository: **https://github.com/yashtalathi2745/DE_ASSIGNMENT**

```text
freshcart-pipelines/
├── README.md
├── .gitignore              <- ignores .env, *.pkl, *.csv, mage.log, __pycache__/
├── requirements.txt
├── elt/                    <- FreshCart ELT notebook (Part B)
├── iris_project/           <- Mage ML project (Part C)
├── stream/                 <- dispatcher.py and driver.py (Part D)
├── tests/
│   └── test_prepare_data.py
└── .github/workflows/ci.yml
```

### Git Commit History (`git log --oneline`):

```text
b35610a fix(ci): restore test_size to 0.2 for passing 80/20 train-test split
f76ba6a test(ci): intentional test failure with test_size=0.3
654f8df fix(ci): resolve requests dependency conflict and add PYTHONPATH to CI workflow
f7d5f01 feat(ml): add Mage iris classification pipeline with eval branch and export block (Part C)
3cbffa4 docs(elt): complete Part B5 data quality check and ETL vs ELT analysis
eee4982 docs(elt): add Part B trace analysis, currency validation rule, and SCD2 idempotency evaluation
642f5e9 feat: add FreshCart ELT notebook with B1 baseline run outputs (Part B)
ec8e8c9 docs: add Part A answers - git command table and CI workflow explanation
7289710 feat: initialise repo structure with folders, .gitignore, requirements and CI workflow
```

---

## A2. Where Does Each Command Act?

| Command | Where the change happens | What exactly happens? |
|---|---|---|
| `git init` | Local machine — **Working folder** | Creates a hidden `.git/` directory inside the current folder; this directory becomes the local repository that Git uses to track all future changes and history. |
| `git add train.py` | Local machine — **Staging area (index)** | Copies the current version of `train.py` from the working folder into the staging area, marking it as ready to be included in the next commit snapshot. |
| `git commit -m "msg"` | Local machine — **Local repository** | Permanently saves a snapshot of everything currently in the staging area into the local `.git/` repository as a new commit, labelled with the given message. |
| `git remote add origin URL` | Local machine — **Local repository config** | Stores the remote GitHub URL under the alias `origin` inside the local Git config file; no files are transferred yet — it only registers where to push or pull from. |
| `git push -u origin main` | **Remote — GitHub repository** | Uploads all local commits on the `main` branch to GitHub and links the local `main` to `origin/main` so future `git push` and `git pull` commands need no extra arguments. |
| `git pull origin main` | Local machine — **Working folder + Local repository** | Fetches the latest commits from GitHub's `main` branch and merges them into the local repository and working folder, keeping the local copy in sync with the remote. |

### Question: After `git commit`, can your teammate see your code on GitHub? Why or why not?

**No.** `git commit` only saves the snapshot to the **local repository** on your own machine — GitHub is not involved at this step at all. The teammate can only see the code after `git push` is run, which is the command that actually uploads the local commits to the remote GitHub repository.

---

## A3. CI Workflow

### Test file — `tests/test_prepare_data.py`

```python
from sklearn.datasets import load_iris
from iris_project.transformers.prepare_data import transform


def test_split_is_80_20():
    """Verifies that prepare_data splits the Iris dataset 80/20 (train/test)."""
    df = load_iris(as_frame=True).frame
    out = transform(df)
    assert (out["split"] == "train").sum() == 120
    assert (out["split"] == "test").sum() == 30
```

### Workflow file — `.github/workflows/ci.yml`

```yaml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    env:
      PYTHONPATH: .

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run tests
        run: python -m pytest tests/ -v
```

### Submitted Evidence:
- **Green (passing) run**: Run #7 (`654f8df`) passed in 27s with exit code 0 (`ci_green.png` — Appendix A3-green).
- **Red (failing) run**: Run #8 (`f76ba6a`) failed on purpose when `test_size` was set to `0.3` (`ci_red.png` — Appendix A3-red).
- **Fix commit**: Commit `b35610a` restored `test_size=0.2` and returned CI to green.
