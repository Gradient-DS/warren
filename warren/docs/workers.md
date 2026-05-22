# Worker Boundary Philosophy

## Core Principle

**Group by default. Split only when forced by resource constraints.**

Over-decomposition adds operational overhead that compounds daily. Deployment cost is occasional and usually cheap.

---

## When to Split

**Hard constraints (must split):**
- Can't fit in memory together (multi-GB models)
- Different compute types (CPU vs GPU)
- Scale differently -- slow tasks can clog fast tasks (e.g. PyMuPDF4LLM (fast) vs CPU-bound OCR (slow))
- Conflicting dependencies
- Minutes-long startup (model loading)

---

## When to Group

- Lightweight dependencies (< 1GB) (e.g. loading different doctypes)
- Same compute type
- Change together

---

## Key Principles

### Reserve GPU workers for GPU work

If GPU utilization matters, separate CPU preprocessing so inference never waits:

```
Intake (CPU: load, parse, convert) → [queue] → GPU Worker (just inference)
```

Images arrive ready. GPU stays saturated.

### API calls are just I/O

External APIs (Mistral OCR, etc.) are HTTP calls — same profile as file loading. Inline them into lightweight workers or routers. Don't waste a queue hop.

### Same interface = same worker, different strategy

Dispatch internally rather than splitting workers:

```python
class Chunker:
    strategies = {
        "text": TextChunker(),
        "xml": XMLStructureChunker(),
    }
```

Same resource profile, same scaling, same deployment. Just different code paths.

---

## Summary

| Keep Together | Split Apart |
|---------------|-------------|
| Lightweight parsers (docx, xlsx, pdf, xml) | GPU models that can't share memory |
| Load + parse + convert (all CPU/IO) | CPU preprocessing vs GPU inference (when utilization matters) |
| API calls + routing logic | Different model variants (olmOCR vs Docling) |
| Multiple chunking strategies | Conflicting dependencies |
| Things that change together | Things with different scaling ratios |