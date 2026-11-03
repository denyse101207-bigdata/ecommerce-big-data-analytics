"""
visualize.py -- Part 4: static charts built from the result files in results/.

Run:  python visualize.py
Out:  figures/fig1..fig7 (PNG)

Only matplotlib + numpy are needed (no pandas).
"""
import csv, os
import matplotlib
matplotlib.use("Agg")                       # draw to files, no window needed
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np

RES, FIG = "results", "figures"
os.makedirs(FIG, exist_ok=True)
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.titlesize": 12, "axes.titleweight": "bold"})
BLUE, DARK, ORANGE, GREEN, GREY = "#6baed6", "#08519c", "#fd8d3c", "#31a354", "#9e9e9e"
money_k = FuncFormatter(lambda v, _: f"${v/1000:,.0f}K")

def read(name):
    with open(os.path.join(RES, name), newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def col(rows, key, cast=float):
    return [cast(r[key]) for r in rows]

def short(s, n=30):
    return s if len(s) <= n else s[: n - 1] + "…"

def save(fig, name):
    path = os.path.join(FIG, name)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("saved", path)

# ---------- Fig 1: revenue over time ----------
rows = read("sql_q5_daily_revenue_7d_avg.csv")
days, rev, ma = [r["day"] for r in rows], col(rows, "revenue"), col(rows, "revenue_7d_avg")
fig, ax = plt.subplots(figsize=(10, 4.5))
x = np.arange(len(days))
ax.bar(x, rev, color=BLUE, label="Daily revenue")
ax.plot(x, ma, color=DARK, lw=2.2, label="7-day average")
ax.set_xticks(x[::10]); ax.set_xticklabels(days[::10], rotation=30, ha="right")
ax.yaxis.set_major_formatter(money_k); ax.set_ylabel("Revenue")
ax.set_title("Daily revenue (MongoDB transactions), Aug 4 – Nov 2, 2026")
ax.annotate("first and last days\nare partial", xy=(0, rev[0]), xytext=(6, max(rev) * 0.55),
            arrowprops=dict(arrowstyle="->", color=GREY), color=GREY, fontsize=9)
ax.legend(frameon=False, loc="lower right")
save(fig, "fig1_daily_revenue.png")

# ---------- Fig 2: revenue vs estimated profit by category ----------
rows = read("revenue_by_category.csv")[:10]
names = [short(r["category"], 28) for r in rows]
revc, prof = col(rows, "revenue"), col(rows, "est_profit")
margin = col(rows, "margin_pct")
y = np.arange(len(rows))
fig, ax = plt.subplots(figsize=(10, 5.5))
ax.barh(y + 0.2, revc, height=0.38, color=BLUE, label="Revenue")
ax.barh(y - 0.2, prof, height=0.38, color=GREEN, label="Estimated profit")
for i, m in enumerate(margin):
    ax.text(max(revc[i], prof[i]) + 15000, y[i], f"{m:.0f}% margin", va="center", fontsize=8, color=GREY)
ax.set_yticks(y); ax.set_yticklabels(names); ax.invert_yaxis()
ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"${v/1e6:.1f}M"))
ax.set_xlim(0, max(revc) * 1.2)
ax.set_title("Top 10 categories: revenue vs estimated profit")
ax.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.06), ncol=2)
save(fig, "fig2_category_revenue_profit.png")

# ---------- Fig 3: customer segments ----------
rows = read("user_segments.csv")
labels = [r["segment"].replace(" (", "\n(") for r in rows]
users, avg_spent = col(rows, "users", int), col(rows, "avg_spent")
fig, axs = plt.subplots(1, 2, figsize=(11, 4.5))
axs[0].bar(labels, users, color=BLUE)
for i, v in enumerate(users): axs[0].text(i, v, f"{v:,}", ha="center", va="bottom", fontsize=9)
axs[0].set_title("Users per purchase-frequency segment"); axs[0].set_ylabel("Users")
axs[1].bar(labels, avg_spent, color=ORANGE)
for i, v in enumerate(avg_spent): axs[1].text(i, v, f"${v:,.0f}", ha="center", va="bottom", fontsize=9)
axs[1].set_title("Average spend per user"); axs[1].set_ylabel("Average spend ($)")
for a in axs: a.tick_params(axis="x", labelsize=8)
save(fig, "fig3_customer_segments.png")

# ---------- Fig 4: top-selling vs most-viewed products ----------
sell = read("top_products_revenue.csv")[:10]
view = read("top_products_views.csv")[:10]
fig, axs = plt.subplots(1, 2, figsize=(12, 5))
axs[0].barh([short(r["name"], 32) for r in sell], col(sell, "revenue"), color=GREEN)
axs[0].invert_yaxis(); axs[0].xaxis.set_major_formatter(money_k)
axs[0].set_title("Top 10 products by revenue")
axs[1].barh([short(r["name"], 32) for r in view], col(view, "views"), color=BLUE)
axs[1].invert_yaxis(); axs[1].set_title("Top 10 most-viewed products (sessions)")
overlap = {r["product_id"] for r in sell} & {r["product_id"] for r in view}
fig.suptitle(f"Best sellers vs most viewed: {len(overlap)} products appear in both lists", y=1.0, fontsize=11)
for a in axs: a.tick_params(axis="y", labelsize=8)
save(fig, "fig4_sellers_vs_viewed.png")

# ---------- Fig 5: conversion funnel (corrected) ----------
dev = read("sql_q3_funnel_by_device.csv")
sessions = sum(col(dev, "sessions", int)); with_cart = sum(col(dev, "with_cart", int))
labelled = sum(col(dev, "converted", int))
no_txn = next(int(r["value"]) for r in read("spark_data_quality.csv")
              if "marked 'converted' with no transaction" in r["check"])
real = labelled - no_txn
stages = [("All sessions", sessions), ("Added to cart", with_cart),
          ("Labelled 'converted'", labelled), ("Real purchase\n(matching transaction)", real)]
fig, ax = plt.subplots(figsize=(10, 4.8))
for i, (name, v) in enumerate(stages):
    c = GREEN if i == 3 else BLUE
    ax.barh(i, v, color=c)
    ax.text(v + sessions * 0.01, i, f"{v:,}  ({v/sessions:.1%})", va="center")
ax.set_yticks(range(len(stages))); ax.set_yticklabels([s[0] for s in stages]); ax.invert_yaxis()
ax.set_xlim(0, sessions * 1.25); ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1000:.0f}K"))
ax.set_title("Conversion funnel: the 'converted' label overstates real purchases")
save(fig, "fig5_conversion_funnel.png")

# ---------- Fig 6: CLV tiers ----------
rows = read("clv_by_tier.csv")
tiers = [r["clv_tier"] for r in rows]
avgp = col(rows, "avg_pred_12m_profit"); totp = col(rows, "total_pred_12m_profit")
share = [100 * v / sum(totp) for v in totp]
colors = ["#6a51a3", "#fdb827", "#9e9e9e", "#cd7f32"]
fig, axs = plt.subplots(1, 2, figsize=(11, 4.5))
axs[0].bar(tiers, avgp, color=colors)
for i, v in enumerate(avgp): axs[0].text(i, v, f"${v:,.0f}", ha="center", va="bottom")
axs[0].set_title("Average predicted 12-month profit per customer"); axs[0].set_ylabel("Profit ($)")
axs[1].bar(tiers, share, color=colors)
for i, v in enumerate(share): axs[1].text(i, v, f"{v:.0f}%", ha="center", va="bottom")
axs[1].set_title("Share of total predicted profit"); axs[1].set_ylabel("% of total")
save(fig, "fig6_clv_tiers.png")

# ---------- Fig 7: engagement (HBase) vs value (MongoDB) ----------
rows = [r for r in read("clv_users.csv") if int(r["orders"]) > 0 and int(r["sessions_observed"]) > 0]
sx = np.array(col(rows, "sessions_observed")); vy = np.array(col(rows, "pred_12m_revenue"))
rng = np.random.default_rng(0)
fig, ax = plt.subplots(figsize=(9, 5))
ax.scatter(sx + rng.uniform(-0.25, 0.25, len(sx)), vy, s=8, alpha=0.25, color=DARK)
m, b = np.polyfit(sx, vy, 1)
xs = np.array([sx.min(), sx.max()])
ax.plot(xs, m * xs + b, color=ORANGE, lw=2.5, label=f"trend (correlation r = {np.corrcoef(sx, vy)[0,1]:.2f})")
ax.yaxis.set_major_formatter(money_k)
ax.set_xlabel("Sessions observed per user (HBase sample)"); ax.set_ylabel("Predicted 12-month revenue (MongoDB)")
ax.set_title("Does more browsing mean a more valuable customer? Only weakly")
ax.legend(frameon=False)
save(fig, "fig7_engagement_vs_clv.png")

print("\nAll charts are in the 'figures' folder.")
