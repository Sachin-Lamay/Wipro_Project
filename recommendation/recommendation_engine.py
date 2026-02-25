"""
Recommendation System
E-Commerce Customer Behavior & Recommendation System

Algorithms implemented:
  1. Collaborative Filtering (ALS - Alternating Least Squares)
  2. Content-Based Filtering (TF-IDF on product attributes)
  3. Market Basket Analysis (Apriori-style association rules)
  4. Hybrid Recommender (weighted blend)
  5. Popular / Trending fallback
"""

import json
import math
import random
import logging
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")

PROC_DIR = Path(__file__).parent.parent / "data" / "processed"
REC_DIR  = Path(__file__).parent.parent / "data" / "recommendations"
REC_DIR.mkdir(parents=True, exist_ok=True)

# ─── Data Structures ──────────────────────────────────────────────────────────
@dataclass
class Product:
    product_id:   str
    product_name: str
    category:     str
    brand:        str
    unit_price:   float
    avg_rating:   float = 0.0
    review_count: int   = 0

@dataclass
class Recommendation:
    product_id:   str
    product_name: str
    category:     str
    score:        float
    reason:       str
    algorithm:    str

# ─── 1. COLLABORATIVE FILTERING (Matrix Factorization / ALS) ─────────────────
class CollaborativeFilter:
    """
    Simplified ALS-style matrix factorization using NumPy.
    In production: use PySpark MLlib ALS or implicit library.
    """

    def __init__(self, n_factors: int = 20, n_iterations: int = 15,
                 regularization: float = 0.1, learning_rate: float = 0.01):
        self.n_factors    = n_factors
        self.n_iters      = n_iterations
        self.reg          = regularization
        self.lr           = learning_rate
        self.user_factors: Optional[np.ndarray] = None
        self.item_factors: Optional[np.ndarray] = None
        self.user_index:   dict[str, int] = {}
        self.item_index:   dict[str, int] = {}
        self.user_ids:     list[str] = []
        self.item_ids:     list[str] = []

    def fit(self, interactions: pd.DataFrame):
        """
        interactions: DataFrame with columns [customer_id, product_id, implicit_score]
        implicit_score = quantity * (1 + log(review_rating)) if reviewed
        """
        log.info("CF: Building user-item matrix …")
        self.user_ids = interactions["customer_id"].unique().tolist()
        self.item_ids = interactions["product_id"].unique().tolist()
        self.user_index = {u: i for i, u in enumerate(self.user_ids)}
        self.item_index = {p: i for i, p in enumerate(self.item_ids)}

        n_users = len(self.user_ids)
        n_items = len(self.item_ids)

        # Initialize factors
        np.random.seed(42)
        self.user_factors = np.random.normal(0, 0.01, (n_users, self.n_factors))
        self.item_factors = np.random.normal(0, 0.01, (n_items, self.n_factors))

        # Build sparse matrix
        R = np.zeros((n_users, n_items))
        for _, row in interactions.iterrows():
            u = self.user_index.get(row["customer_id"])
            i = self.item_index.get(row["product_id"])
            if u is not None and i is not None:
                R[u, i] = min(row["implicit_score"], 5.0)   # cap at 5

        # SGD training
        log.info("CF: Training ALS (%d factors, %d iters) …", self.n_factors, self.n_iters)
        nonzero = list(zip(*R.nonzero()))
        for iteration in range(self.n_iters):
            random.shuffle(nonzero)
            total_loss = 0.0
            for u, i in nonzero:
                prediction = self.user_factors[u] @ self.item_factors[i]
                error      = R[u, i] - prediction
                total_loss += error ** 2

                # Update
                uf_old = self.user_factors[u].copy()
                self.user_factors[u] += self.lr * (error * self.item_factors[i] - self.reg * self.user_factors[u])
                self.item_factors[i] += self.lr * (error * uf_old              - self.reg * self.item_factors[i])

            if (iteration + 1) % 5 == 0:
                rmse = math.sqrt(total_loss / max(len(nonzero), 1))
                log.info("  iter %d/%d  RMSE=%.4f", iteration+1, self.n_iters, rmse)

        log.info("CF: Training complete.")
        return self

    def recommend(self, customer_id: str, top_n: int = 10,
                  exclude_purchased: set[str] | None = None) -> list[tuple[str, float]]:
        if customer_id not in self.user_index:
            return []
        u_idx   = self.user_index[customer_id]
        scores  = self.user_factors[u_idx] @ self.item_factors.T
        ranked  = np.argsort(scores)[::-1]
        results = []
        for idx in ranked:
            pid = self.item_ids[idx]
            if exclude_purchased and pid in exclude_purchased:
                continue
            results.append((pid, float(scores[idx])))
            if len(results) >= top_n:
                break
        return results

# ─── 2. CONTENT-BASED FILTERING ───────────────────────────────────────────────
class ContentBasedFilter:
    """TF-IDF cosine similarity on product attributes"""

    def __init__(self):
        self.product_vectors: dict[str, np.ndarray] = {}
        self.vocab: dict[str, int] = {}

    def _tokenize(self, product: dict) -> list[str]:
        text = " ".join([
            str(product.get("product_name",  "")),
            str(product.get("category_name", "")),
            str(product.get("brand",         "")),
            str(product.get("subcategory",   "")),
        ]).lower()
        return [t for t in text.split() if len(t) > 2]

    def fit(self, products: list[dict]):
        log.info("CBF: Building TF-IDF product vectors …")
        corpus: dict[str, list[str]] = {}
        for p in products:
            corpus[p["product_id"]] = self._tokenize(p)

        # Build vocab
        all_tokens = [t for tokens in corpus.values() for t in tokens]
        vocab_counts = Counter(all_tokens)
        # Filter rare tokens
        self.vocab = {t: i for i, (t, c) in enumerate(vocab_counts.items()) if c >= 2}

        # Compute TF-IDF
        n_docs = len(corpus)
        df_counts = Counter(t for tokens in corpus.values() for t in set(tokens))
        idf = {t: math.log((n_docs + 1) / (df_counts.get(t, 0) + 1)) + 1
               for t in self.vocab}

        for pid, tokens in corpus.items():
            tf = Counter(tokens)
            vec = np.zeros(len(self.vocab))
            for token, count in tf.items():
                if token in self.vocab:
                    vec[self.vocab[token]] = (count / len(tokens)) * idf[token]
            norm = np.linalg.norm(vec)
            self.product_vectors[pid] = vec / norm if norm > 0 else vec

        log.info("CBF: %d products indexed, vocab size=%d", len(self.product_vectors), len(self.vocab))
        return self

    def similar_products(self, product_id: str, top_n: int = 10) -> list[tuple[str, float]]:
        if product_id not in self.product_vectors:
            return []
        query  = self.product_vectors[product_id]
        scores = {pid: float(query @ vec)
                  for pid, vec in self.product_vectors.items() if pid != product_id}
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]

    def recommend_from_history(self, purchased_ids: list[str], top_n: int = 10,
                                exclude: set[str] | None = None) -> list[tuple[str, float]]:
        """Average of profile vectors then find nearest neighbors"""
        vecs = [self.product_vectors[pid]
                for pid in purchased_ids if pid in self.product_vectors]
        if not vecs:
            return []
        profile = np.mean(vecs, axis=0)
        scores  = {pid: float(profile @ vec)
                   for pid, vec in self.product_vectors.items()
                   if pid not in (exclude or set())}
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]

# ─── 3. ASSOCIATION RULES (Market Basket) ─────────────────────────────────────
class AssociationRules:
    """Frequent itemset mining via simplified Apriori"""

    def __init__(self, min_support: float = 0.001, min_confidence: float = 0.1,
                 min_lift: float = 1.0):
        self.min_support    = min_support
        self.min_confidence = min_confidence
        self.min_lift       = min_lift
        self.rules: list[dict] = []

    def fit(self, baskets: list[list[str]]):
        log.info("MBA: Mining association rules (%d baskets) …", len(baskets))
        n = len(baskets)
        item_counts   = Counter(item for basket in baskets for item in basket)
        pair_counts   = Counter()

        for basket in baskets:
            items = list(set(basket))
            for i in range(len(items)):
                for j in range(i+1, len(items)):
                    pair_counts[frozenset([items[i], items[j]])] += 1

        for (a, b), count in pair_counts.items():
            support    = count / n
            if support < self.min_support:
                continue
            conf_ab    = count / item_counts[a]
            conf_ba    = count / item_counts[b]
            lift       = support / (item_counts[a]/n * item_counts[b]/n)

            if lift < self.min_lift:
                continue
            if conf_ab >= self.min_confidence:
                self.rules.append({"antecedent": a, "consequent": b,
                                   "support": support, "confidence": conf_ab, "lift": lift})
            if conf_ba >= self.min_confidence:
                self.rules.append({"antecedent": b, "consequent": a,
                                   "support": support, "confidence": conf_ba, "lift": lift})

        self.rules.sort(key=lambda r: r["lift"], reverse=True)
        log.info("MBA: %d rules generated", len(self.rules))
        return self

    def recommend(self, cart_items: list[str], top_n: int = 5) -> list[tuple[str, float]]:
        candidates: dict[str, float] = {}
        for rule in self.rules:
            if rule["antecedent"] in cart_items and rule["consequent"] not in cart_items:
                pid   = rule["consequent"]
                score = rule["lift"] * rule["confidence"]
                candidates[pid] = max(candidates.get(pid, 0), score)
        return sorted(candidates.items(), key=lambda x: x[1], reverse=True)[:top_n]

# ─── 4. HYBRID RECOMMENDER ────────────────────────────────────────────────────
class HybridRecommender:
    """Weighted blend of CF + CBF + MBA + Popularity"""

    def __init__(self, cf_weight: float = 0.5, cbf_weight: float = 0.3,
                 mba_weight: float = 0.15, pop_weight: float = 0.05):
        self.cf_weight  = cf_weight
        self.cbf_weight = cbf_weight
        self.mba_weight = mba_weight
        self.pop_weight = pop_weight
        self.cf:         Optional[CollaborativeFilter]  = None
        self.cbf:        Optional[ContentBasedFilter]   = None
        self.mba:        Optional[AssociationRules]     = None
        self.popularity: dict[str, float]               = {}
        self.product_meta: dict[str, dict]              = {}

    def fit(self, interactions: pd.DataFrame, products: list[dict],
            baskets: list[list[str]]):
        log.info("Hybrid: Fitting all models …")

        self.product_meta = {p["product_id"]: p for p in products}

        # Popularity score (normalized)
        pop = interactions.groupby("product_id")["implicit_score"].sum()
        max_pop = pop.max() or 1
        self.popularity = (pop / max_pop).to_dict()

        # Fit CF
        self.cf = CollaborativeFilter(n_factors=15, n_iterations=10).fit(interactions)

        # Fit CBF
        self.cbf = ContentBasedFilter().fit(products)

        # Fit MBA
        self.mba = AssociationRules(min_support=0.0005).fit(baskets)

        log.info("Hybrid: All models fitted.")
        return self

    def _normalize(self, scores: list[tuple[str, float]]) -> dict[str, float]:
        if not scores:
            return {}
        vals    = [s for _, s in scores]
        max_val = max(vals) or 1
        return {pid: score/max_val for pid, score in scores}

    def recommend(self, customer_id: str, purchased_ids: list[str],
                  cart_items: list[str], top_n: int = 10) -> list[Recommendation]:
        exclude = set(purchased_ids)
        all_pids = set(self.product_meta.keys()) - exclude

        # CF scores
        cf_raw = self.cf.recommend(customer_id, top_n=50, exclude_purchased=exclude) if self.cf else []
        cf_scores = self._normalize(cf_raw)

        # CBF scores
        cbf_raw = self.cbf.recommend_from_history(purchased_ids[-10:], top_n=50, exclude=exclude) if self.cbf else []
        cbf_scores = self._normalize(cbf_raw)

        # MBA scores
        mba_raw   = self.mba.recommend(cart_items or purchased_ids[-3:], top_n=30) if self.mba else []
        mba_scores = self._normalize(mba_raw)

        # Popularity
        pop_scores = {pid: self.popularity.get(pid, 0) for pid in all_pids}

        # Blend
        combined: dict[str, float] = {}
        for pid in all_pids:
            score = (
                self.cf_weight  * cf_scores.get(pid, 0) +
                self.cbf_weight * cbf_scores.get(pid, 0) +
                self.mba_weight * mba_scores.get(pid, 0) +
                self.pop_weight * pop_scores.get(pid, 0)
            )
            combined[pid] = round(score, 6)

        # Top-N
        top = sorted(combined.items(), key=lambda x: x[1], reverse=True)[:top_n]

        results = []
        for rank, (pid, score) in enumerate(top, 1):
            meta = self.product_meta.get(pid, {})

            # Determine reason
            if cf_scores.get(pid, 0) > 0.3:
                reason = "Based on customers like you"
            elif cbf_scores.get(pid, 0) > 0.3:
                reason = "Similar to items you bought"
            elif mba_scores.get(pid, 0) > 0.3:
                reason = "Frequently bought together"
            else:
                reason = "Trending in your category"

            results.append(Recommendation(
                product_id=pid,
                product_name=meta.get("product_name","Unknown"),
                category=meta.get("category_name",""),
                score=score,
                reason=reason,
                algorithm="hybrid_v1",
            ))
        return results

# ─── Build & Persist Recommendations ──────────────────────────────────────────
def build_recommendations():
    log.info("=" * 60)
    log.info("  Recommendation System  START")
    log.info("=" * 60)

    # Load data
    def load(name: str) -> pd.DataFrame:
        p = PROC_DIR / f"{name}_clean.csv"
        if not p.exists():
            p = PROC_DIR / f"{name}.csv"
        if not p.exists():
            log.warning("File not found: %s — using empty df", p)
            return pd.DataFrame()
        return pd.read_csv(p)

    df_orders  = load("orders")
    df_items   = load("order_items")
    df_prods   = load("products")
    df_reviews = load("reviews")

    if df_orders.empty or df_prods.empty:
        log.error("Missing required data files. Run ETL pipeline first.")
        return

    # Build implicit interactions
    item_agg = (
        df_items.groupby(["order_id","product_id"])["quantity"]
        .sum().reset_index()
        .merge(df_orders[["order_id","customer_id"]], on="order_id")
    )
    if not df_reviews.empty:
        review_agg = df_reviews.groupby(["customer_id","product_id"])["rating"].mean().reset_index()
        item_agg   = item_agg.merge(review_agg, on=["customer_id","product_id"], how="left")
        item_agg["rating"] = item_agg["rating"].fillna(3.0)
    else:
        item_agg["rating"] = 3.0

    interactions = (
        item_agg.groupby(["customer_id","product_id"])
        .agg(quantity=("quantity","sum"), rating=("rating","mean"))
        .reset_index()
    )
    interactions["implicit_score"] = (
        interactions["quantity"] * (1 + np.log1p(interactions["rating"]))
    ).round(4)

    # Product metadata
    products_list = df_prods.rename(columns={
        "category_name": "category_name",
        "subcategory_name": "subcategory",
    }).to_dict("records")

    # Baskets for MBA
    baskets = df_items.groupby("order_id")["product_id"].apply(list).tolist()

    # Fit hybrid model
    recommender = HybridRecommender()
    recommender.fit(interactions, products_list, baskets)

    # Generate top-10 picks for all customers
    log.info("Generating recommendations for all customers …")
    customers = interactions["customer_id"].unique()
    all_recs  = []

    customer_history = (
        interactions.groupby("customer_id")["product_id"]
        .apply(list).to_dict()
    )

    for i, cust_id in enumerate(customers):
        purchased = customer_history.get(cust_id, [])
        recs      = recommender.recommend(cust_id, purchased, purchased[-3:], top_n=10)
        for rank, rec in enumerate(recs, 1):
            all_recs.append({
                "customer_id":  cust_id,
                "rank":         rank,
                "product_id":   rec.product_id,
                "product_name": rec.product_name,
                "category":     rec.category,
                "score":        rec.score,
                "reason":       rec.reason,
                "algorithm":    rec.algorithm,
            })
        if (i + 1) % 500 == 0:
            log.info("  Processed %d / %d customers …", i+1, len(customers))

    # Save
    df_recs = pd.DataFrame(all_recs)
    df_recs.to_csv(REC_DIR / "top_picks.csv", index=False)
    log.info("Saved recommendations → %s/top_picks.csv  (%d rows)", REC_DIR, len(df_recs))

    # Save affinity rules
    rules_df = pd.DataFrame(recommender.mba.rules)
    rules_df.to_csv(REC_DIR / "association_rules.csv", index=False)
    log.info("Saved association rules → %s/association_rules.csv  (%d rules)",
             REC_DIR, len(rules_df))

    log.info("=" * 60)
    log.info("  Recommendation System  COMPLETE")
    log.info("=" * 60)

if __name__ == "__main__":
    build_recommendations()
