import pandas as pd
from sentence_transformers import SentenceTransformer, util

# Load SentenceTransformer model
model = SentenceTransformer('BAAI/bge-base-en-v1.5')

# Load product group datasets
dvflora_df = pd.read_csv('utils/Mapping_products/dvflora_productgroups.csv', dtype={"competitor_product_group_id": str})
ibf_df = pd.read_csv('utils/Mapping_products/ibf_productgroups.csv')

dvflora_names = dvflora_df["competitor_product_group_name"].astype(str).tolist()
ibf_names = ibf_df["ibf_product_group"].astype(str).tolist()

# Encode product group names
dvflora_embeddings = model.encode(dvflora_names, convert_to_tensor=True, show_progress_bar=True)
ibf_embeddings = model.encode(ibf_names, convert_to_tensor=True, show_progress_bar=True)

# Similarity threshold
threshold = 0.5
matches = []

# Match each DVFlora product group to the best IBF group
for idx, dvflora_name in enumerate(dvflora_names):
    dvflora_id = dvflora_df.iloc[idx]["competitor_product_group_id"]
    similarity_scores = util.pytorch_cos_sim(dvflora_embeddings[idx], ibf_embeddings)[0]
    best_idx = similarity_scores.argmax().item()
    best_score = similarity_scores[best_idx].item()
    
    if best_score >= threshold:
        ibf_id = ibf_df.iloc[best_idx]["_KEY"]
        ibf_name = ibf_df.iloc[best_idx]["ibf_product_group"]
        matches.append({
            "ibf_product_group": ibf_id,
            "ibf_product_group_name": ibf_name,
            "competitor_product_group_id": dvflora_id,
            "competitor_product_group_name": dvflora_name,
            "similarity_score": round(best_score, 3)
        })

# Save output
output_df = pd.DataFrame(matches)
output_df.to_csv("dvflora_product_group_matches.csv", index=False)
print("✅ Matches saved to product_group_matches.csv")