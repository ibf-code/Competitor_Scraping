import pandas as pd
from sentence_transformers import SentenceTransformer, util

model = SentenceTransformer('BAAI/bge-base-en-v1.5')

# Load both datasets
mayesh_df = pd.read_csv('mayesh_product_names_and_ids.csv')
ibf_df = pd.read_csv('ibf_product_names_and_ids.csv')

# Create a reference for product names to match
mayesh_names = mayesh_df["mayesh_product_name"].astype(str).tolist()
ibf_names = ibf_df["ibf_product_name"].astype(str).tolist()

# Create embeddings for both datasets
mayesh_embeddings = model.encode(mayesh_names, convert_to_tensor=True)
ibf_embeddings = model.encode(ibf_names, convert_to_tensor=True)

# dict to store best matches for each mayesh product
best_matches = {}

# dict to check which ibf varieties have been matched
ibf_matched = {}
threshold = 0.8

# First filter to find all possible matches that are above the threshoold
candidate_matches = []
for idx, mayesh_name in enumerate(mayesh_names):
    mayesh_id = mayesh_df.iloc[idx]["competitor_product_id"]
    similarities = util.pytorch_cos_sim(mayesh_embeddings[idx], ibf_embeddings)[0]
    best_idx = similarities.argmax().item()
    best_score = similarities[best_idx].item()

    # penalty for matching names in mayesh containing "assorted" to a single color ibf name
    if "assorted" in mayesh_name.lower():
        ibf_name_lower = ibf_names[best_idx].lower()
        specific_colors = ["red", "blue", "yellow", 
                           "green", "white", "pink", "purple", "orange",
                           "peach", "lavender", "coral", "burgundy", "cream",
                           "gold", "silver", "cream", 
                           ]
        # check if the ibf name contains any of the specific colors
        has_color = any(color in ibf_name_lower for color in specific_colors)
        is_not_assorted = "assorted" not in ibf_name_lower
        
        # if the ibf name contains a specific color and is not assorted, penalize the score
        if has_color and is_not_assorted:
            best_score -= 0.05

    # penalty for matching names in mayesh containing "assorted" to a single color ibf name
    # onlt use matches thay are above the threshold formualted in the dict
    if best_score >= threshold:
        ibf_id = ibf_df.iloc[best_idx]["variety_key"]
        candidate_matches.append({
            "mayesh_product_id": mayesh_id,
            "mayesh_product_name": mayesh_name,
            "ibf_product_id": ibf_id,
            "ibf_product_name": ibf_names[best_idx],
            "similarity_score": round(best_score, 3)
        })
# sort candidate matches on score (descending)
candidate_matches.sort(key=lambda x: x["similarity_score"], reverse=True)

# dict to track ibf varieties that have been matched to mayesh varieties
ibf_to_mayesh_mapping = {}

# second filter to only take the highest scroing match for each mayesh product
final_matches = []

for match in candidate_matches:
    mayesh_id = match["mayesh_product_id"]
    ibf_id = match["ibf_product_id"]
    mayesh_name = match["mayesh_product_name"]
    score = match["similarity_score"]

    if ibf_id in ibf_to_mayesh_mapping: # is the ibf product already matched?
        if ibf_to_mayesh_mapping[ibf_id]["name"] == mayesh_name: # handles matching of same name (product_id is different) (size)
            final_matches.append(match)
        elif score > ibf_to_mayesh_mapping[ibf_id]["score"] + 0.05: # only if the new score is significantly higher replace all matches
            final_matches = [m for m in final_matches if m["ibf_product_id"] != ibf_id] # remove all previous matches for ibf name
            final_matches.append(match) # add new match
            ibf_to_mayesh_mapping[ibf_id] = {"name": mayesh_name, "score": score} # update the mapping
    else:
        ibf_to_mayesh_mapping[ibf_id] = {"name": mayesh_name, "score": score} # first time this name is seen
        final_matches.append(match)

result_df = pd.DataFrame(final_matches)
result_df.to_csv("mapped_products_and_ids.csv", index=False)


