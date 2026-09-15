# Engineering Standards & Agent Protocols

1. **Strict Test Protocols**: Always run the entire test suite holistically (e.g., `pytest tests/`) rather than running individual test files and assuming a green build. Always ensure test state is completely isolated to avoid cross-contamination.
2. **Architectural Thinking**: Fix bugs at the structural level (e.g., state machine conditions, centralized helpers) rather than just pasting function calls where the application crashed. Treat the disease, not the symptom.
3. **Zero Unapproved Pushes**: Do NOT execute `git commit` or `git push` without explicitly presenting the findings, test results, and benchmark numbers to the user and receiving a clear "yes". Pushing code is a destructive action requiring consent.
