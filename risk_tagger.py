"""
OrbisScope Risk Tagger (Tier B).

Embeds each named risk's prototype sentences into a semantic centroid, then tags
untagged processed_events with the risk they most resemble (cosine similarity) —
BGRI's "classify relevance via a language model" instead of keyword matching.

Writes risk_id (slug or 'OTHER') and risk_relevance (0..1) per event.

Uses all-MiniLM-L6-v2 (tiny, CPU-fine). On the GPU tier the notebook does the
same with device='cuda'. Requires migration 002.
"""

import numpy as np

from common import get_supabase, fetch_all, RISKS

MODEL = "all-MiniLM-L6-v2"
THRESHOLD = 0.22      # below this similarity -> 'OTHER' (no clear risk match)
BATCH = 400           # events to tag per run


def risk_centroids(embedder):
    """One normalized centroid embedding per risk from its prototype sentences."""
    centroids = {}
    for r in RISKS:
        emb = embedder.encode(r["prototypes"], normalize_embeddings=True)
        c = np.mean(emb, axis=0)
        c = c / (np.linalg.norm(c) or 1.0)
        centroids[r["slug"]] = c
    return centroids


def run():
    supabase = get_supabase()
    print("🏷️  Risk tagger starting...")

    # Untagged events with usable text.
    rows = fetch_all(supabase, "processed_events", "id,event_description,location_name,risk_id")
    todo = [r for r in rows
            if not r.get("risk_id") and (r.get("event_description") or r.get("location_name"))][:BATCH]
    if not todo:
        print("✅ Nothing to tag.")
        return

    from sentence_transformers import SentenceTransformer
    embedder = SentenceTransformer(MODEL)
    centroids = risk_centroids(embedder)
    slugs = list(centroids)
    C = np.vstack([centroids[s] for s in slugs])  # (n_risks, dim)

    texts = [(r.get("event_description") or r.get("location_name")) for r in todo]
    emb = embedder.encode(texts, normalize_embeddings=True, show_progress_bar=False)

    sims = emb @ C.T  # cosine (all normalized) -> (n_events, n_risks)
    tagged = 0
    for r, row_sims in zip(todo, sims):
        best = int(np.argmax(row_sims))
        score = float(row_sims[best])
        slug = slugs[best] if score >= THRESHOLD else "OTHER"
        supabase.table("processed_events").update({
            "risk_id": slug,
            "risk_relevance": round(score, 4),
        }).eq("id", r["id"]).execute()
        tagged += 1

    print(f"✅ Tagged {tagged} events across {len(slugs)} risks.")


if __name__ == "__main__":
    run()
