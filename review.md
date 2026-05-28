# Code Review

**Reviewer:** Shaochong  
**Date:** May 2026  
**Commit reviewed:** `46870a2`  
**Files in question:** [`src/pipeline/pipeline.py`](https://github.com/jxiong321/final_project/blob/46870a2/src/pipeline/pipeline.py), [`src/pipeline/sources.py`](https://github.com/jxiong321/final_project/blob/46870a2/src/pipeline/sources.py)

---

## Questions I asked:

**Q1: Does the `@pipeline.transform(input_fields={"age": str})` decorator feel natural to use, or awkward? Would you have expected it to work differently?**

**Q2: What do you think about having two sinks for the data processing (a sink for the good rows and another sink for the errors)? Does this approach make sense to you, and do you have a better approach for handling errors?**

---

## Shaochong's response:

Shaochong's project is a file organizer that watches a directory and applies rules to move/rename files, also using asyncio. His review focused on my pipeline's concurrency model, testing strategy, and design decisions.

**On Q1 (the decorator):**  
He thought the decorator felt natural overall, since it's similar to how frameworks like Flask register routes. His one note was that `input_fields` being a dict of `{field: type}` is slightly non-obvious at first. He suggested a docstring or a short example in the README would help new users understand that the types are used for validation.

**On Q2 (dual sinks):**  
He liked the dual-sink approach and said it's a common pattern in data systems where you separate good records from bad ones. His main suggestion was to make it more explicit to the user why a record ended up in the error sink. for example, including the full record alongside the transform name and error message. He also noted that the current implementation drops a record after the first transform that errors on it, which means if two transforms would have errored, you only see the first one. For this project scope he thought it was fine, but flagged it as something to think about.

---

## Miscellaneous notes

Shaochong pointed out something I hadn't thought much about: my `_sample()` method in `dry_run` fully drains the source into a temp queue to get a sample, which means for a large file it reads the whole thing just to validate one record. He called it out as a note but said a production version would want to peek without reading everything first. I left a comment about this in the code already, but it was good validation that it's worth flagging.

He also noted that the async producer-consumer pattern I used is very similar to what he did in his own project.

---

## Changes made as a result of this review

- Added a note to the README explaining that `input_fields` values are types used for validation, not just presence checks — this directly addresses his Q1 feedback about it being non-obvious.
- The error sink already includes `transform` name and `reason` in the routed record, which partially addresses his Q2 feedback. No code change needed there.
- No other structural changes.Tt he review mainly confirmed my design decisions (dual sinks, first-match-wins for transforms, bounded queue). It was nice to get a second pair of eyes.
