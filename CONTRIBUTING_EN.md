[한국어](CONTRIBUTING.md) | [English](CONTRIBUTING_EN.md)

# Contributing guide

This describes the procedure to follow when working in this repository. The standards for writing code are in [docs/CODE_CONVENTIONS_EN.md](docs/CODE_CONVENTIONS_EN.md), and how to set up your development environment and run the tests is in [Running](README_EN.md#running) and [Tests and documentation](README_EN.md#tests-and-documentation) of the README.

---

## Contribution flow

1. Open an [issue](https://github.com/mash-up-kr/spr1n6-osscontest-worker/issues/new/choose) first. Use the Bug report form for bugs and the Feature request form for suggestions.
2. Comment on the issue you want to work on, then branch off of `main`. Include the issue number in the branch name. (`fix/123-claim-timeout`)
3. Write `Closes #123` under Related Issue in the PR body. The issue closes when the PR is merged.

Small changes such as typo or documentation fixes can go straight to a PR without an issue.

Do not report security vulnerabilities as issues. Report them privately as described in the [security policy](SECURITY_EN.md).

## Branches

| Branch | Purpose |
|--------------|-------------|
| `main` | Integration branch |
| `feat/*` | Feature development |
| `fix/*` | Bug fixes |
| `refactor/*` | Refactoring |
| `docs/*` | Documentation work |
| `chore/*` | Config, build, dependencies |

Branch off of `main` for your work, and merge back into `main` via a PR when done.

## Commit messages

The subject line follows the `<type>: <subject>` format. The body is free-form.

```
feat: 삭제된 문서의 INDEX Job을 정상 종결 처리
fix: 장애 전환 중 연결이 풀에 남아 같은 오류가 반복되던 문제 수정
docs: README의 DB 연결 설정 불일치 수정
refactor: DB Job 기반 Worker 실행권·재시도 처리 통합
test: 장애 처리 동작 검증 추가
chore: 소스 파일에 MIT 헤더 추가
```

Don't mix cleanup commits (comments, naming, dead code) with commits that change behavior. Mixing them makes it impossible to tell, during review, whether something was moved or actually changed.

## Pull Request

Follow `.github/pull_request_template.md`. In the body, describe what the problem was, how to reproduce it, what was fixed, and how it was verified.

Split PRs by root cause. Only bundle changes together when they can't stand on their own without each other.

Record any deliberation over design or alternatives considered in the PR body, not in code comments. Comments should stay focused on what the code does.
