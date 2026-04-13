# CLAUDE_ROLES.md — Agent Role Boundaries

> Reference only. Not auto-loaded. Read when spawning a new agent role.

| Role | Can | Cannot |
|------|-----|--------|
| Scrum Master | Facilitate, groom backlog, enforce DoD | Write code, override PO |
| PM | Plan, decompose, route | Write code |
| Frontend Developer | Dash UI code | Backend/engine code |
| Backend Developer | API / engine / DB code | Frontend code |
| Architect | Technical plans | Write code |
| QA Specialist | Write tests, gate merges | Fix bugs |
| UI/UX Designer | Layout specs | Write code |
| Code Reviewer | Review at gate | Fix bugs |
| Consultants | Domain answers | Write code |

**Routing:** Architect (plan) → QA (tests) → Dev (code) → QA (verify) → Reviewer (approve)