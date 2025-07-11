import pandas as pd

# Parameters
PMB_THRESHOLD = 5.0
INGREDIENT_LIST = ["simvastatin", "atorvastatin", "rosuvastatin", "pravastatin","fluvastatin","lovastatin","pitavastatin"]

# Load CSV files
df_ingredients = pd.read_csv(r"c:\Users\User\Desktop\onsides-v3.1.0\csv\vocab_rxnorm_ingredient.csv")
df_products = pd.read_csv(r"c:\Users\User\Desktop\onsides-v3.1.0\csv\vocab_rxnorm_ingredient_to_product.csv")
df_product_labels = pd.read_csv(r"c:\Users\User\Desktop\onsides-v3.1.0\csv\product_to_rxnorm.csv")
df_label_info = pd.read_csv(r"c:\Users\User\Desktop\onsides-v3.1.0\csv\product_label.csv")
df_adverse_effects = pd.read_csv(r"c:\Users\User\Desktop\onsides-v3.1.0\csv\product_adverse_effect.csv")
df_meddra_terms = pd.read_csv(r"c:\Users\User\Desktop\onsides-v3.1.0\csv\vocab_meddra_adverse_effect.csv")

# Filter adverse effects based on threshold (SM AND PMD > 5)
original_count = len(df_adverse_effects)
filter_mask = (
    df_adverse_effects['pred1'].isna() |
    ((df_adverse_effects['pred1'] > PMB_THRESHOLD) & (df_adverse_effects['pred0'] == 0.0))
)
filtered_effects = df_adverse_effects[filter_mask]

filtered_file = r"c:\Users\User\Desktop\filtered_adverse_effects.csv" #save data
filtered_effects.to_csv(filtered_file, index=False)
print(f"Filtered data saved to: {filtered_file}")

# Remove duplicate product IDs from products data
df_products = df_products.drop_duplicates(subset=['product_id'], keep=False)

# Find RxNorm IDs for the ingredients we need
ingredient_match = df_ingredients[
    df_ingredients['rxnorm_name'].str.lower().isin([ing.lower() for ing in INGREDIENT_LIST])
]

if ingredient_match.empty:
    print("No ingredients found.")
    exit()

print(f"\nIngredients found:")
print(ingredient_match[['rxnorm_id', 'rxnorm_name']])

# Get products that contain these ingredients
rxcui_list = ingredient_match['rxnorm_id'].tolist()
matched_products = df_products[df_products['ingredient_id'].isin(rxcui_list)]

if matched_products.empty:
    print("No products found for these ingredients.")
    exit()

# Add ingredient names to matched products
matched_products = matched_products.merge(
    df_ingredients[['rxnorm_id', 'rxnorm_name']],
    left_on='ingredient_id',
    right_on='rxnorm_id',
    how='inner'
)

print(f"\nProducts with ingredient names:")
print(matched_products[['product_id', 'rxnorm_name']].drop_duplicates())

product_ids = matched_products['product_id'].tolist()

# Get product labels for these products
matched_labels = df_product_labels[
    df_product_labels['product_rxnorm_id'].astype(str).isin(map(str, product_ids))
]

if matched_labels.empty:
    print("No labels found for these products.")
    exit()

# Merge labels with label info
label_data = pd.merge(matched_labels, df_label_info, on='label_id', how='inner')

# Merge labels with filtered adverse effects
effects_data = pd.merge(
    label_data,
    filtered_effects,
    left_on='label_id',
    right_on='product_label_id',
    how='inner'
)

# Add ingredient name to effects data
effects_data = effects_data.merge(
    matched_products[['product_id', 'rxnorm_name']],
    left_on='product_rxnorm_id',
    right_on='product_id',
    how='inner',
    suffixes=('', '_ingredient')
)

# Merge with MedDRA terms for adverse effect names
final_output = pd.merge(
    effects_data,
    df_meddra_terms,
    left_on='effect_meddra_id',
    right_on='meddra_id',
    how='inner'
)

# Select useful columns and drop rows without MedDRA name
final_output = final_output[[
    'rxnorm_name',         # ingredient name
    'label_id',            # label id
    'product_rxnorm_id',   # product id
    'effect_meddra_id',    # MedDRA id
    'meddra_name',         # MedDRA name
    'meddra_term_type',
    'source'
]].dropna(subset=['meddra_name'])

print("\nFinal Adverse Effects (PMB-significant):")
print(final_output.head(20))

final_output_file = r"c:\Users\User\Desktop\pmb_significant_adverse_effects.csv"
final_output.to_csv(final_output_file, index=False)
print(f"Saved final results to: {final_output_file}")

# Remove duplicates
deduplicated = final_output.drop_duplicates(
    subset=['rxnorm_name', 'label_id', 'effect_meddra_id', 'meddra_name', 'meddra_term_type', 'source']
)

deduplicated_file = r"c:\Users\User\Desktop\pmb_significant_adverse_effects_DEDUPLICATED.csv"
deduplicated.to_csv(deduplicated_file, index=False)

print(f"\nRemoved {len(final_output) - len(deduplicated)} duplicates")
print(f"Unique records count: {len(deduplicated)}")
print(f"Saved deduplicated results to: {deduplicated_file}")
