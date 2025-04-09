import pandas as pd
from sentence_transformers import SentenceTransformer, util

model = SentenceTransformer('BAAI/bge-base-en-v1.5')

# Load both datasets
fmp_df = pd.read_csv('utils/flowermarketplace_product_names_and_ids.csv')
ibf_df = pd.read_csv('utils/ibf_product_names_and_ids.csv')

#reference df in names
fmp_names = fmp_df['fmp_product_name'].astype(str).tolist()
ibf_names = ibf_df['ibf_product_name'].astype(str).tolist()

# emnbeddings for the names 
fmp_embeddings = model.encode(fmp_names, convert_to_tensor=True)
ibf_embeddings = model.encode(ibf_names, convert_to_tensor=True)

best_mastches = {}

ibf_matched = {}
threshold = 0.8 # can be tweaked to lower or higher threshold. The higeher the threshold the stricker the match


candidate_matches= []
for idx, fmp_name in enumerate(fmp_names): # loop through all the names in the fmp df
    fmp_id = fmp_df.iloc[idx]["fmp_product_id"] # get the fmp id
    similarities = util.pytorch_cos_sim(fmp_embeddings[idx], ibf_embeddings)[0] # cosine similarity
    best_idx = similarities.argmax().item() # index of the best match
    best_score = similarities[best_idx].item() # score of the best match

    if "assorted" in fmp_name.lower(): # logic to attempt to match single color flowers on asssorted flowers 
        ibf_name_lower = ibf_names[best_idx].lower()
        specific_colors = ["red", "blue", "yellow", 
                           "green", "white", "pink", "purple", "orange",
                           "peach", "lavender", "coral", "burgundy", "cream",
                           "gold", "silver", "cream", 
                           ]
        has_color = any(color in ibf_name_lower for color in specific_colors)
        is_not_assorted = "assorted" not in ibf_name_lower

        if has_color and is_not_assorted:
            best_score-= 0.05 # reduce the score for matches that match single color flowers on assorted flowers


    if best_score >= threshold: # only use matches that are above the threshold
        ibf_id = ibf_df.iloc[best_idx]["variety_key"] # get the ibf variety key
        candidate_matches.append({ 
            "fmp_product_id": fmp_id,
            "fmp_product_name": fmp_name,
            "ibf_product_id": ibf_id,
            "ibf_product_name": ibf_names[best_idx],
            "similarity_score": round(best_score, 3)
        })

# sort candidate matches on score from high to low
candidate_matches.sort(key=lambda x: x["similarity_score"], reverse=True) # search diego

# create a dictionairy of the best matches for fmp and ibf
ibf_to_fmp_mapping = {} # ibf to fmp mapping

final_matches =[]
for match in candidate_matches:
    fmp_id = match["fmp_product_id"]
    ibf_id = match["ibf_product_id"]
    fmp_name = match["fmp_product_name"]
    score = match["similarity_score"]

    if ibf_id in ibf_to_fmp_mapping:
        if ibf_to_fmp_mapping[ibf_id]["name"] == fmp_name:
            final_matches.append(match)
        elif score > ibf_to_fmp_mapping[ibf_id]["score"] + 0.05:
            final_matches = [m for m in final_matches if m["ibf_product_id"] != ibf_id]
            final_matches.append(match)
            ibf_to_fmp_mapping[ibf_id] = {"name": fmp_name, "score": score}
    else:
        ibf_to_fmp_mapping[ibf_id] = {"name": fmp_name, "score": score}
        final_matches.append(match)

result_df = pd.DataFrame(final_matches)
result_df.to_csv('utils/mapped_products_and_ids_fmp.csv', index=False) 


        